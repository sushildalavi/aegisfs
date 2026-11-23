#include "aegisfs/node.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <random>
#include <set>
#include <thread>

#include "aegisfs/metrics.hpp"
#include "aegisfs/replication.hpp"

namespace fs = std::filesystem;

namespace aegisfs {

static std::string randomHex(int bytes) {
  static thread_local std::mt19937_64 rng(
      std::random_device{}() ^
      static_cast<uint64_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count()));
  static const char* kHex = "0123456789abcdef";
  std::string out;
  for (int i = 0; i < bytes; ++i) {
    uint8_t b = static_cast<uint8_t>(rng() & 0xFF);
    out.push_back(kHex[(b >> 4) & 0xF]);
    out.push_back(kHex[b & 0xF]);
  }
  return out;
}

static std::string sanitize(const std::string& s) {
  std::string out;
  for (char c : s) {
    out.push_back((std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_') ? c : '_');
  }
  if (out.size() > 80) out.resize(80);
  return out;
}

static void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

Node::Node(NodeConfig cfg) : config(std::move(cfg)) {}
Node::~Node() { stop(); }

std::shared_ptr<std::mutex> Node::fileLock(const std::string& fileId) {
  std::lock_guard<std::mutex> lk(fileLocksMu);
  auto it = fileLocks.find(fileId);
  if (it == fileLocks.end()) {
    auto m = std::make_shared<std::mutex>();
    fileLocks[fileId] = m;
    return m;
  }
  return it->second;
}

std::string Node::loadOrCreateIdentity() {
  std::string path = config.dataRoot + "/node.id";
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd >= 0) {
    char buf[128];
    ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
    ::close(fd);
    if (n > 0) {
      std::string id(buf, static_cast<size_t>(n));
      while (!id.empty() && (id.back() == '\n' || id.back() == ' ')) id.pop_back();
      if (!id.empty()) return id;
    }
  }
  std::string id = config.nodeId.empty() ? ("node-" + randomHex(8)) : config.nodeId;
  fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd >= 0) {
    (void)::write(fd, id.data(), id.size());
    ::fsync(fd);
    ::close(fd);
  }
  return id;
}

uint64_t Node::nextLamport() { return lamportClock.fetch_add(1) + 1; }
void Node::observeLamport(uint64_t ts) {
  uint64_t cur = lamportClock.load();
  while (ts > cur && !lamportClock.compare_exchange_weak(cur, ts)) {}
}

Status Node::start() {
  storageEngine = std::make_unique<StorageEngine>(config.dataRoot);
  if (!isOk(storageEngine->init())) return Status::IoError;
  std::string id = loadOrCreateIdentity();
  listener = Listener::bind(config.host, config.port);
  if (!listener.valid()) return Status::NetworkError;
  portNumber = listener.port();
  std::string addr = config.host + ":" + std::to_string(portNumber);
  membershipTable = std::make_unique<Membership>(id, addr);
  // Seeds are contacted directly in gossipOnce(); we do not insert placeholder members with
  // synthetic ids (that would pollute the ring). Real members are learned via gossip replies.
  threadPool = std::make_unique<ThreadPool>(static_cast<size_t>(config.ioThreads));
  running = true;
  acceptThread = std::thread([this] { acceptLoop(); });
  gossipThread = std::thread([this] { gossipLoop(); });
  fdThread = std::thread([this] { failureDetectorLoop(); });
  maintThread = std::thread([this] { maintenanceLoop(); });
  return Status::Ok;
}

void Node::stop() {
  bool was = running.exchange(false);
  if (!was) return;
  listener.close();
  if (acceptThread.joinable()) acceptThread.join();
  if (gossipThread.joinable()) gossipThread.join();
  if (fdThread.joinable()) fdThread.join();
  if (maintThread.joinable()) maintThread.join();
  if (threadPool) threadPool->shutdown();
}

Ring Node::currentRing() const {
  Ring r;
  r.setMembers(membershipTable->ringNodes());
  return r;
}

std::vector<RingNode> Node::replicasFor(const std::string& filename) const {
  return currentRing().successors(filename, config.rf);
}

void Node::acceptLoop() {
  while (running) {
    Conn c = listener.accept();
    if (!c.valid()) {
      if (!running) break;
      continue;
    }
    auto sp = std::make_shared<Conn>(std::move(c));
    threadPool->submit([this, sp] { handleConn(sp); });
  }
}

void Node::handleConn(std::shared_ptr<Conn> c) {
  c->setRecvTimeout(30000);
  c->setSendTimeout(30000);
  MsgType t;
  Fields h;
  uint64_t plen = 0;
  if (!isOk(readMessageHead(*c, t, h, plen))) return;
  Metrics::instance().incr("rpc.accepted");
  switch (t) {
    case MsgType::Create: hCreate(*c, h, plen); return;
    case MsgType::Append: hAppend(*c, h, plen); return;
    case MsgType::ReplicatePut: hReplicate(*c, h, plen, true); return;
    case MsgType::ReplicateAppend: hReplicate(*c, h, plen, false); return;
    default: break;
  }
  // small-payload requests: pull payload into memory
  Frame fr;
  fr.type = t;
  fr.header = h;
  fr.payload.resize(plen);
  if (plen && !c->readAll(fr.payload.data(), plen)) return;
  switch (t) {
    case MsgType::Ping: hPing(*c); break;
    case MsgType::Membership: hMembership(*c, fr); break;
    case MsgType::ListMemIds: hListMemIds(*c); break;
    case MsgType::FetchManifest: hFetchManifest(*c, fr); break;
    case MsgType::FetchSegment: hFetchSegment(*c, fr); break;
    case MsgType::GetFromReplica: hGetFromReplica(*c, fr); break;
    case MsgType::ListStore: hListStore(*c); break;
    case MsgType::Ls: hLs(*c, fr); break;
    case MsgType::Get: hGet(*c, fr); break;
    case MsgType::Merge: hMerge(*c, fr); break;
    case MsgType::StatusQuery: hStatus(*c); break;
    case MsgType::Health: hHealth(*c); break;
    case MsgType::Leave: hLeave(*c); break;
    case MsgType::Metrics: hMetrics(*c); break;
    default: repl::sendReply(*c, Status::InvalidArgument); break;
  }
}

// ---------------- replica-role handlers ----------------

void Node::hPing(Conn& c) { repl::sendReply(c, Status::Ok); }

void Node::hMembership(Conn& c, Frame& fr) {
  std::vector<Member> incoming;
  if (Membership::parse(fr.payload, incoming)) membershipTable->merge(incoming);
  // reply with our table so gossip is bidirectional
  Fields h;
  std::string body = membershipTable->serialize();
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  writeFrame(c, MsgType::Reply, h, body);
}

void Node::hListMemIds(Conn& c) {
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  writeFrame(c, MsgType::Reply, h, membershipTable->serialize());
}

void Node::hReplicate(Conn& c, Fields& h, uint64_t plen, bool isBase) {
  std::string fileId = h.getOr("fileId");
  std::string filename = h.getOr("filename");
  std::string createOp = h.getOr("createOpId");
  SegmentRef seg;
  if (fileId.empty() || !repl::segFromFields(h, seg)) {
    discardPayload(c, plen);
    repl::sendReply(c, Status::InvalidArgument);
    return;
  }
  observeLamport(seg.lamportTs);
  // Spool the payload to a temp file (verifying its SHA), then apply under the per-file lock.
  Sha256Digest got;
  std::string tmp;
  Status ss = spoolPayload(c, plen, "repl", tmp, got);
  if (!isOk(ss)) { repl::sendReply(c, ss); return; }
  if (got != seg.sha) {
    fs::remove(tmp);
    repl::sendReply(c, Status::ChecksumMismatch);
    return;
  }
  Status cs;
  {
    auto lk = fileLock(fileId);
    std::lock_guard<std::mutex> g(*lk);
    cs = applySegmentLocal(fileId, filename, createOp, seg, tmp);
  }
  fs::remove(tmp);
  Metrics::instance().incr(isBase ? "replicatePut" : "replicateAppend");
  repl::sendReply(c, cs);
}

void Node::hFetchManifest(Conn& c, Frame& fr) {
  std::string fileId = fr.header.getOr("fileId");
  Manifest m;
  if (!storageEngine->loadManifest(fileId, m)) {
    repl::sendReply(c, Status::NotFound);
    return;
  }
  std::string body;
  m.serialize(body);
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  writeFrame(c, MsgType::Reply, h, body);
}

void Node::hFetchSegment(Conn& c, Frame& fr) {
  std::string fileId = fr.header.getOr("fileId");
  std::string disk = fr.header.getOr("disk");
  // Determine size via manifest
  Manifest m;
  uint64_t size = 0;
  if (storageEngine->loadManifest(fileId, m)) {
    for (const auto& s : m.segments)
      if (s.diskName == disk) size = s.size;
  }
  int fd = ::open((storageEngine->dataDir() + "/files/" + fileId + "/" + disk).c_str(), O_RDONLY);
  if (fd < 0) {
    Fields h;
    h.setI64("status", static_cast<int64_t>(Status::NotFound));
    writeFrame(c, MsgType::Reply, h, "");
    return;
  }
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  writeFrameStreaming(c, MsgType::Reply, h, size, [&](char* b, size_t cap) -> size_t {
    ssize_t n = ::read(fd, b, cap);
    return n > 0 ? static_cast<size_t>(n) : 0;
  });
  ::close(fd);
}

void Node::hGetFromReplica(Conn& c, Frame& fr) {
  std::string filename = fr.header.getOr("file");
  std::string fileId = makeFileId(filename);
  Manifest m;
  if (!storageEngine->loadManifest(fileId, m)) {
    Fields h;
    h.setI64("status", static_cast<int64_t>(Status::NotFound));
    writeFrame(c, MsgType::Reply, h, "");
    return;
  }
  uint64_t total = m.totalSize();
  // First pass: compute the logical file SHA-256 (also verifies each segment checksum) so the
  // reply header can carry the real file digest for merge/compare. Second pass streams bytes.
  Sha256Digest fsha{};
  Status fs = storageEngine->fileSha(m, fsha);
  if (!isOk(fs)) {
    Fields h;
    h.setI64("status", static_cast<int64_t>(fs));  // e.g. ChecksumMismatch on local corruption
    writeFrame(c, MsgType::Reply, h, "");
    return;
  }
  auto ordered = m.ordered();
  size_t idx = 0;
  std::string cur;
  size_t curOff = 0;
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  h.set("fileSha", toHex(fsha));
  h.setU64("size", total);
  writeFrameStreaming(c, MsgType::Reply, h, total, [&](char* b, size_t cap) -> size_t {
    while (curOff >= cur.size()) {
      if (idx >= ordered.size()) return 0;
      cur.clear();
      curOff = 0;
      if (!isOk(storageEngine->readSegment(fileId, ordered[idx]->diskName, cur))) return 0;
      ++idx;
    }
    size_t n = std::min(cap, cur.size() - curOff);
    std::memcpy(b, cur.data() + curOff, n);
    curOff += n;
    return n;
  });
}

void Node::hListStore(Conn& c) {
  auto ms = storageEngine->listManifests();
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  h.setI64("count", static_cast<int64_t>(ms.size()));
  int i = 0;
  for (auto& m : ms) {
    std::string pfx = "f" + std::to_string(i++) + "_";
    h.set(pfx + "name", m.filename);
    h.set(pfx + "id", m.fileId);
    h.setU64(pfx + "size", m.totalSize());
    h.setU64(pfx + "version", m.version);
    h.setI64(pfx + "segs", static_cast<int64_t>(m.segments.size()));
  }
  writeFrame(c, MsgType::Reply, h, "");
}

void Node::hLs(Conn& c, Frame& fr) {
  std::string filename = fr.header.getOr("file");
  std::string fileId = makeFileId(filename);
  auto reps = replicasFor(filename);
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  h.set("file", filename);
  h.set("fileId", fileId);
  h.setI64("replicas", static_cast<int64_t>(reps.size()));
  int present = 0;
  for (size_t i = 0; i < reps.size(); ++i) {
    std::string pfx = "r" + std::to_string(i) + "_";
    h.set(pfx + "node", reps[i].nodeId);
    h.set(pfx + "addr", reps[i].address);
    // verified presence: does this intended replica actually hold the manifest right now?
    Manifest m;
    bool has = isOk(repl::fetchManifest(reps[i].address, fileId, m));
    h.set(pfx + "present", has ? "yes" : "no");
    if (has) ++present;
  }
  h.setI64("presentCount", present);
  writeFrame(c, MsgType::Reply, h, "");
}

void Node::hStatus(Conn& c) {
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  h.set("nodeId", membershipTable->selfId());
  h.set("address", address());
  h.setI64("alive", static_cast<int64_t>(membershipTable->aliveCount()));
  h.setI64("files", static_cast<int64_t>(storageEngine->listManifests().size()));
  h.setU64("lamport", lamportClock.load());
  writeFrame(c, MsgType::Reply, h, "");
}

void Node::hHealth(Conn& c) { repl::sendReply(c, Status::Ok); }

void Node::hMetrics(Conn& c) {
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  for (const auto& [k, v] : Metrics::instance().snapshotCounters()) h.setU64("counter." + k, v);
  for (const char* op : {"create", "append", "get", "merge"}) {
    auto s = Metrics::instance().latency(op);
    if (s.count == 0) continue;
    std::string pfx = std::string("lat.") + op + ".";
    h.setU64(pfx + "count", s.count);
    h.setU64(pfx + "p50Us", s.p50);
    h.setU64(pfx + "p95Us", s.p95);
    h.setU64(pfx + "p99Us", s.p99);
  }
  writeFrame(c, MsgType::Reply, h, "");
}

void Node::hLeave(Conn& c) {
  // Graceful leave: exclude self from the ring, then hand off our replicas to the new successors
  // (reconcilePlacement pushes missing data and safely deletes our now-redundant copies), and
  // disseminate the LEFT state so peers stop routing to us.
  membershipTable->markLeft(membershipTable->selfId());
  reconcilePlacement();
  gossipOnce();
  Metrics::instance().incr("gracefulLeave");
  repl::sendReply(c, Status::Ok);
}

// ---------------- coordinator-role handlers ----------------

Status Node::spoolPayload(Conn& c, uint64_t plen, const std::string& tag, std::string& tmpPath,
                           Sha256Digest& sha) {
  tmpPath = storageEngine->dataDir() + "/tmp/" + tag + "." + randomHex(8);
  int fd = ::open(tmpPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::IoError;
  Sha256Stream hasher;
  Status s = streamPayloadIn(c, plen, [&](const char* b, size_t n) {
    hasher.update(b, n);
    const char* p = b;
    size_t left = n;
    while (left) {
      ssize_t w = ::write(fd, p, left);
      if (w < 0) return false;
      p += w;
      left -= static_cast<size_t>(w);
    }
    return true;
  });
  ::fsync(fd);
  ::close(fd);
  if (!isOk(s)) { fs::remove(tmpPath); return s; }
  sha = hasher.finalize();
  return Status::Ok;
}

Status Node::applySegmentLocal(const std::string& fileId, const std::string& filename,
                                 const std::string& createOp, const SegmentRef& seg,
                                 const std::string& srcPath) {
  // Lock-free: the caller holds the per-file lock. Enforces idempotency and first-create-only.
  Manifest m;
  bool have = storageEngine->loadManifest(fileId, m);
  if (have && m.hasOp(seg.opId)) return Status::Ok;  // idempotent replay
  if (seg.kind == SegKind::Base && have) {
    for (const auto& s : m.segments) {
      if (s.kind == SegKind::Base && s.opId != seg.opId) return Status::AlreadyExists;
    }
  }
  int fd = ::open(srcPath.c_str(), O_RDONLY);
  if (fd < 0) return Status::IoError;
  Sha256Digest stored;
  Status ps = storageEngine->putSegmentStream(
      fileId, seg.diskName, seg.size,
      [&](char* b, size_t cap) -> size_t {
        ssize_t n = ::read(fd, b, cap);
        return n > 0 ? static_cast<size_t>(n) : 0;
      },
      stored);
  ::close(fd);
  if (!isOk(ps)) return ps;
  if (stored != seg.sha) return Status::ChecksumMismatch;
  if (!have) {
    m.filename = filename;
    m.fileId = fileId;
    m.createOpId = createOp;
    m.version = 0;
  }
  m.addSegment(seg);
  return storageEngine->commitManifest(m);
}

Status Node::applyLocalLocked(const std::string& fileId, const std::string& filename,
                                const std::string& createOp, const SegmentRef& seg,
                                const std::string& srcPath) {
  auto lk = fileLock(fileId);
  std::lock_guard<std::mutex> g(*lk);
  return applySegmentLocal(fileId, filename, createOp, seg, srcPath);
}

int Node::fanout(const std::vector<RingNode>& targets, const std::string& fileId,
                 const std::string& filename, const std::string& createOp, const SegmentRef& seg,
                 const std::string& srcPath, uint64_t plen, bool isBase) {
  auto one = [&](const RingNode& r) -> Status {
    if (r.nodeId == membershipTable->selfId())
      return applyLocalLocked(fileId, filename, createOp, seg, srcPath);
    int fd = ::open(srcPath.c_str(), O_RDONLY);
    if (fd < 0) return Status::IoError;
    std::function<size_t(char*, size_t)> src = [fd](char* b, size_t cap) -> size_t {
      ssize_t n = ::read(fd, b, cap);
      return n > 0 ? static_cast<size_t>(n) : 0;
    };
    Status rs = repl::replicateSegment(r.address, fileId, filename, createOp, seg, plen, src,
                                        isBase);
    ::close(fd);
    return rs;
  };
  std::atomic<int> committed{0};
  if (!config.parallelReplication || targets.size() <= 1) {
    for (const auto& r : targets)
      if (isOk(one(r))) committed.fetch_add(1);
  } else {
    std::vector<std::thread> ts;
    for (const auto& r : targets)
      ts.emplace_back([&, r] { if (isOk(one(r))) committed.fetch_add(1); });
    for (auto& t : ts) t.join();
  }
  return committed.load();
}

static size_t makeFileSource(int& fdHolder, const std::string& path,
                               std::function<size_t(char*, size_t)>& src) {
  int fd = ::open(path.c_str(), O_RDONLY);
  fdHolder = fd;
  src = [fd](char* b, size_t cap) -> size_t {
    ssize_t n = ::read(fd, b, cap);
    return n > 0 ? static_cast<size_t>(n) : 0;
  };
  return fd >= 0 ? 1 : 0;
}

void Node::hCreate(Conn& c, Fields& h, uint64_t plen) {
  ScopedTimer timer("create");
  Metrics::instance().incr("op.create");
  std::string filename = h.getOr("file");
  std::string clientId = h.getOr("clientId", "anon");
  std::string opId = h.getOr("opId");
  if (opId.empty()) opId = clientId + "#create#" + randomHex(6);
  if (!StorageEngine::validFilename(filename)) {
    discardPayload(c, plen);
    repl::sendReply(c, Status::InvalidArgument);
    return;
  }
  std::string fileId = makeFileId(filename);
  std::string tmp;
  Sha256Digest sha;
  Status ss = spoolPayload(c, plen, "create", tmp, sha);
  if (!isOk(ss)) { repl::sendReply(c, ss); return; }

  auto reps = replicasFor(filename);
  if (reps.empty()) { fs::remove(tmp); repl::sendReply(c, Status::Unavailable); return; }

  SegmentRef seg;
  seg.kind = SegKind::Base;
  seg.opId = opId;
  seg.clientId = clientId;
  seg.clientSeq = 0;
  seg.lamportTs = nextLamport();
  seg.size = plen;
  seg.sha = sha;
  seg.diskName = "base.seg";

  // Primary-arbitrated first-create-only: write the base to the primary (reps[0]) first; it
  // atomically rejects a duplicate base (different opId). Concurrent creates serialize there.
  // The coordinator holds NO cross-node lock; each node locks only its own local apply.
  auto writeTo = [&](const RingNode& r) -> Status {
    if (r.nodeId == membershipTable->selfId()) return applyLocalLocked(fileId, filename, opId, seg, tmp);
    int fd = -1;
    std::function<size_t(char*, size_t)> src;
    makeFileSource(fd, tmp, src);
    if (fd < 0) return Status::IoError;
    Status rs = repl::replicateSegment(r.address, fileId, filename, opId, seg, plen, src, true);
    ::close(fd);
    return rs;
  };

  Status primary = writeTo(reps[0]);
  if (primary == Status::AlreadyExists) {
    fs::remove(tmp);
    repl::sendReply(c, Status::AlreadyExists);
    return;
  }
  int committed = isOk(primary) ? 1 : 0;
  // fan out to the remaining replicas (sequential or parallel per config)
  std::vector<RingNode> rest(reps.begin() + 1, reps.end());
  committed += fanout(rest, fileId, filename, opId, seg, tmp, plen, true);
  fs::remove(tmp);
  Fields reply;
  reply.set("fileId", fileId);
  reply.setI64("committed", committed);
  reply.setI64("rf", config.rf);
  for (size_t i = 0; i < reps.size(); ++i)
    reply.set("r" + std::to_string(i), reps[i].address);
  Status final = (committed == static_cast<int>(reps.size())) ? Status::Ok : Status::Degraded;
  if (committed == 0) final = Status::Unavailable;
  repl::sendReply(c, final, reply);
}

Status Node::authoritativeManifest(const std::string& filename, Manifest& out,
                                    std::vector<RingNode>& liveReplicas) {
  std::string fileId = makeFileId(filename);
  auto reps = replicasFor(filename);
  bool found = false;
  Manifest merged;
  merged.filename = filename;
  merged.fileId = fileId;
  for (const auto& r : reps) {
    Manifest m;
    if (isOk(repl::fetchManifest(r.address, fileId, m))) {
      liveReplicas.push_back(r);
      if (!found) {
        merged = m;
        found = true;
      } else {
        for (const auto& s : m.segments) merged.addSegment(s);
        if (merged.createOpId.empty()) merged.createOpId = m.createOpId;
      }
    }
  }
  if (!found) return Status::NotFound;
  out = merged;
  return Status::Ok;
}

void Node::hAppend(Conn& c, Fields& h, uint64_t plen) {
  ScopedTimer timer("append");
  Metrics::instance().incr("op.append");
  std::string filename = h.getOr("file");
  std::string clientId = h.getOr("clientId", "anon");
  std::string opId = h.getOr("opId");
  if (opId.empty()) opId = clientId + "#" + randomHex(8);
  if (!StorageEngine::validFilename(filename)) {
    discardPayload(c, plen);
    repl::sendReply(c, Status::InvalidArgument);
    return;
  }
  std::string fileId = makeFileId(filename);
  std::string tmp;
  Sha256Digest sha;
  Status ss = spoolPayload(c, plen, "append", tmp, sha);
  if (!isOk(ss)) { repl::sendReply(c, ss); return; }

  Manifest auth;
  std::vector<RingNode> live;
  Status as = authoritativeManifest(filename, auth, live);
  if (!isOk(as)) { fs::remove(tmp); repl::sendReply(c, as); return; }

  // idempotent retry
  if (auth.hasOp(opId)) {
    fs::remove(tmp);
    Fields reply;
    reply.set("note", "idempotent");
    for (const auto& existing : auth.segments) {
      if (existing.opId == opId) {
        reply.setU64("clientSeq", existing.clientSeq);
        reply.set("opId", existing.opId);
        break;
      }
    }
    repl::sendReply(c, Status::Ok, reply);
    return;
  }
  // per-client sequence = max existing for this client + 1
  uint64_t seq = 0;
  for (const auto& s : auth.segments)
    if (s.clientId == clientId) seq = std::max(seq, s.clientSeq);
  seq += 1;

  SegmentRef seg;
  seg.kind = SegKind::Append;
  seg.opId = opId;
  seg.clientId = clientId;
  seg.clientSeq = seq;
  seg.lamportTs = nextLamport();
  seg.size = plen;
  seg.sha = sha;
  seg.diskName = "append-" + sanitize(opId) + ".seg";

  auto reps = replicasFor(filename);
  int committed = fanout(reps, fileId, filename, auth.createOpId, seg, tmp, plen, false);
  fs::remove(tmp);
  Fields reply;
  reply.setI64("committed", committed);
  reply.setU64("clientSeq", seq);
  reply.set("opId", opId);
  Status final = (committed == static_cast<int>(reps.size())) ? Status::Ok : Status::Degraded;
  if (committed == 0) final = Status::Unavailable;
  repl::sendReply(c, final, reply);
}

void Node::hGet(Conn& c, Frame& fr) {
  ScopedTimer timer("get");
  Metrics::instance().incr("op.get");
  std::string filename = fr.header.getOr("file");
  std::string clientId = fr.header.getOr("clientId", "anon");
  uint64_t requiredSeq = fr.header.getU64("requiredSeq");
  bool quorumRead = fr.header.getOr("readMode") == "quorum";
  std::string fileId = makeFileId(filename);
  auto reps = replicasFor(filename);
  int quorumCandidates = 0;

  // choose a replica whose manifest satisfies the read-your-writes watermark
  std::string chosenAddr;
  Manifest chosen;
  bool haveFile = false;
  for (const auto& r : reps) {
    Manifest m;
    if (!isOk(repl::fetchManifest(r.address, fileId, m))) continue;
    haveFile = true;
    uint64_t maxseq = 0;
    for (const auto& s : m.segments)
      if (s.clientId == clientId) maxseq = std::max(maxseq, s.clientSeq);
    if (maxseq >= requiredSeq) ++quorumCandidates;
    if (maxseq >= requiredSeq && (!quorumRead || chosenAddr.empty() || m.version > chosen.version)) {
      chosenAddr = r.address;
      chosen = m;
    }
  }
  if (quorumRead) {
    // A quorum read requires a majority of replica manifests before serving bytes. The
    // highest-version manifest among that quorum is selected; repair remains an explicit merge.
    int required = static_cast<int>(reps.size() / 2) + 1;
    if (quorumCandidates < required) chosenAddr.clear();
  }
  if (chosenAddr.empty()) {
    Fields h;
    Status st = haveFile ? Status::Stale : (reps.empty() ? Status::Unavailable : Status::NotFound);
    h.setI64("status", static_cast<int64_t>(st));
    writeFrame(c, MsgType::Reply, h, "");
    return;
  }
  // stream the file from the chosen replica to the client.
  uint64_t total = chosen.totalSize();
  Fields h;
  h.setI64("status", static_cast<int64_t>(Status::Ok));
  h.setU64("size", total);
  if (quorumRead) h.set("readMode", "quorum");
  // Pull from replica, forward to client. Use a pipe via buffered relay.
  std::string host; int port;
  parseEndpoint(chosenAddr, host, port);
  Conn rc = Conn::dial(host, port, 30000);
  if (!rc.valid()) {
    h.setI64("status", static_cast<int64_t>(Status::Unavailable));
    writeFrame(c, MsgType::Reply, h, "");
    return;
  }
  Fields req;
  req.set("file", filename);
  writeFrame(rc, MsgType::GetFromReplica, req, "");
  MsgType rt; Fields rh; uint64_t rplen = 0;
  if (!isOk(readMessageHead(rc, rt, rh, rplen))) {
    h.setI64("status", static_cast<int64_t>(Status::NetworkError));
    writeFrame(c, MsgType::Reply, h, "");
    return;
  }
  // relay rplen bytes from rc to client c
  writeFrameStreaming(c, MsgType::Reply, h, rplen, [&](char* b, size_t cap) -> size_t {
    // read up to cap from rc
    ssize_t n = ::recv(rc.fd(), b, cap, 0);
    return n > 0 ? static_cast<size_t>(n) : 0;
  });
}

void Node::hMerge(Conn& c, Frame& fr) {
  ScopedTimer timer("merge");
  Metrics::instance().incr("op.merge");
  std::string filename = fr.header.getOr("file");
  std::string fileId = makeFileId(filename);
  // No cross-node lock held during fan-out (would deadlock with replica-side locks). Self-repair
  // uses applyLocalLocked for its short local critical section.
  Manifest auth;
  std::vector<RingNode> live;
  Status as = authoritativeManifest(filename, auth, live);
  if (!isOk(as)) { repl::sendReply(c, as); return; }

  // Ensure every live replica has every op in the authoritative (union) manifest.
  for (const auto& r : live) {
    Manifest m;
    if (!isOk(repl::fetchManifest(r.address, fileId, m))) continue;
    for (const auto& seg : auth.segments) {
      if (m.hasOp(seg.opId)) continue;
      // find a source replica that has this op's segment
      std::string srcAddr;
      for (const auto& s : live) {
        Manifest sm;
        if (isOk(repl::fetchManifest(s.address, fileId, sm)) && sm.hasOp(seg.opId)) {
          srcAddr = s.address;
          break;
        }
      }
      if (srcAddr.empty()) continue;
      std::string tmp = storageEngine->dataDir() + "/tmp/merge." + randomHex(8);
      int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd < 0) continue;
      repl::fetchSegment(srcAddr, fileId, seg.diskName, [&](const char* b, size_t n) {
        ssize_t w = ::write(fd, b, n);
        return w == static_cast<ssize_t>(n);
      });
      ::fsync(fd);
      ::close(fd);
      if (r.nodeId == membershipTable->selfId()) {
        applyLocalLocked(fileId, filename, auth.createOpId, seg, tmp);
      } else {
        int rfd = -1;
        std::function<size_t(char*, size_t)> src;
        makeFileSource(rfd, tmp, src);
        if (rfd >= 0) {
          repl::replicateSegment(r.address, fileId, filename, auth.createOpId, seg, seg.size,
                                  src, seg.kind == SegKind::Base);
          ::close(rfd);
        }
      }
      fs::remove(tmp);
    }
  }

  // verify all live replicas are byte-identical
  Sha256Digest ref{};
  bool first = true;
  bool identical = true;
  for (const auto& r : live) {
    Sha256Digest fsha{};
    Status gs = repl::getFromReplica(r.address, filename,
                                       [](const char*, size_t) { return true; }, fsha);
    if (!isOk(gs)) { identical = false; break; }
    if (first) { ref = fsha; first = false; }
    else if (fsha != ref) { identical = false; break; }
  }
  Fields reply;
  reply.setI64("replicas", static_cast<int64_t>(live.size()));
  reply.set("fileSha", toHex(ref));
  repl::sendReply(c, identical ? Status::Ok : Status::Conflict, reply);
}

// ---------------- background loops ----------------

void Node::gossipOnce() {
  // Targets: a random sample of known alive peers, plus all configured seeds (by address).
  std::vector<std::string> targets;
  auto peers = membershipTable->alive();
  for (auto& m : peers)
    if (m.nodeId != membershipTable->selfId()) targets.push_back(m.address);
  static thread_local std::mt19937 rng(std::random_device{}());
  std::shuffle(targets.begin(), targets.end(), rng);
  if (targets.size() > 3) targets.resize(3);
  for (const auto& s : config.seeds) {
    if (s.empty() || s == address()) continue;
    if (std::find(targets.begin(), targets.end(), s) == targets.end()) targets.push_back(s);
  }
  std::string body = membershipTable->serialize();
  for (const auto& addr : targets) {
    std::string host; int port;
    if (!parseEndpoint(addr, host, port)) continue;
    Conn c = Conn::dial(host, port, 1000);
    if (!c.valid()) continue;
    Fields h;
    if (isOk(writeFrame(c, MsgType::Membership, h, body))) {
      Frame reply;
      if (isOk(readFrame(c, reply))) {
        std::vector<Member> incoming;
        if (Membership::parse(reply.payload, incoming)) membershipTable->merge(incoming);
      }
    }
  }
}

void Node::gossipLoop() {
  while (running) {
    gossipOnce();
    sleepMs(config.gossipIntervalMs);
  }
}

void Node::failureDetectorLoop() {
  while (running) {
    auto peers = membershipTable->alive();
    for (auto& m : peers) {
      if (!running) break;
      if (m.nodeId == membershipTable->selfId()) continue;
      std::string host; int port;
      if (!parseEndpoint(m.address, host, port)) continue;
      Conn c = Conn::dial(host, port, config.probeIntervalMs);
      bool ok = false;
      if (c.valid()) {
        Fields h;
        if (isOk(writeFrame(c, MsgType::Ping, h, ""))) {
          Frame reply;
          ok = isOk(readFrame(c, reply));
        }
      }
      if (!ok) membershipTable->markSuspect(m.nodeId);
      else membershipTable->markAlive(m.nodeId, m.address, m.incarnation);
    }
    sleepMs(config.probeIntervalMs);
  }
}

void Node::maintenanceLoop() {
  while (running) {
    auto failed = membershipTable->tick(static_cast<uint64_t>(config.suspectMs),
                                    static_cast<uint64_t>(config.reapMs));
    reconcilePlacement();
    antiEntropy();
    // GC stale staging files left by a coordinator that crashed mid-spool (>60s old).
    {
      std::error_code ec;
      auto now = fs::file_time_type::clock::now();
      for (auto& e : fs::directory_iterator(storageEngine->dataDir() + "/tmp", ec)) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(
                       now - fs::last_write_time(e.path(), ec)).count();
        if (age > 60) fs::remove(e.path(), ec);
      }
    }
    (void)failed;
    sleepMs(config.reconcileMs);
  }
}

void Node::antiEntropy() {
  if (!storageEngine) return;
  auto manifests = storageEngine->listManifests();
  for (auto& m : manifests) {
    auto reps = replicasFor(m.filename);
    bool selfReplica = false;
    for (const auto& r : reps)
      if (r.nodeId == membershipTable->selfId()) selfReplica = true;
    if (!selfReplica) continue;  // push/handoff path owns non-replica files

    // 1) detect locally corrupt/missing segments (checksum mismatch) that need re-fetch.
    std::vector<SegmentRef> need = {};
    for (const auto& seg : m.segments) {
      Sha256Stream hsh;
      bool ok = isOk(storageEngine->readSegmentStream(
          m.fileId, seg.diskName, [&](const char* b, size_t n) { hsh.update(b, n); return true; }));
      if (!ok || hsh.finalize() != seg.sha) need.push_back(seg);
    }

    // 2) learn ops present on peer replicas but missing locally.
    for (const auto& r : reps) {
      if (r.nodeId == membershipTable->selfId()) continue;
      Manifest pm;
      if (!isOk(repl::fetchManifest(r.address, m.fileId, pm))) continue;
      for (const auto& seg : pm.segments)
        if (!m.hasOp(seg.opId)) need.push_back(seg);
    }
    if (need.empty()) continue;

    // 3) pull each needed segment from a peer replica that has a good copy, apply locally.
    for (const auto& seg : need) {
      std::string src;
      for (const auto& r : reps) {
        if (r.nodeId == membershipTable->selfId()) continue;
        Manifest pm;
        if (isOk(repl::fetchManifest(r.address, m.fileId, pm)) && pm.hasOp(seg.opId)) {
          src = r.address;
          break;
        }
      }
      if (src.empty()) continue;
      std::string tmp = storageEngine->dataDir() + "/tmp/ae." + randomHex(8);
      int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd < 0) continue;
      Status fs = repl::fetchSegment(src, m.fileId, seg.diskName, [&](const char* b, size_t n) {
        ssize_t w = ::write(fd, b, n);
        return w == static_cast<ssize_t>(n);
      });
      ::fsync(fd);
      ::close(fd);
      if (isOk(fs)) {
        if (m.hasOp(seg.opId)) {
          // corrupt-segment repair: overwrite the bytes in place (manifest already has the op)
          std::string bytes;
          int rfd = ::open(tmp.c_str(), O_RDONLY);
          if (rfd >= 0) {
            char buf[65536]; ssize_t n;
            while ((n = ::read(rfd, buf, sizeof(buf))) > 0) bytes.append(buf, static_cast<size_t>(n));
            ::close(rfd);
            auto lk = fileLock(m.fileId);
            std::lock_guard<std::mutex> g(*lk);
            if (isOk(storageEngine->putSegment(m.fileId, seg.diskName, bytes, seg.sha)))
              Metrics::instance().incr("antiEntropyRepair");
          }
        } else {
          // missing op: apply as a new segment
          if (isOk(applyLocalLocked(m.fileId, m.filename, m.createOpId, seg, tmp)))
            Metrics::instance().incr("antiEntropyRepair");
        }
      }
      fs::remove(tmp);
    }
  }
}

void Node::reconcilePlacement() {
  if (!storageEngine) return;
  auto manifests = storageEngine->listManifests();
  for (auto& m : manifests) {
    auto reps = replicasFor(m.filename);
    bool selfIsReplica = false;
    for (const auto& r : reps)
      if (r.nodeId == membershipTable->selfId()) selfIsReplica = true;

    // push any ops the designated replicas are missing (re-replication / rebalance)
    int confirmed = 0;
    for (const auto& r : reps) {
      if (r.nodeId == membershipTable->selfId()) { ++confirmed; continue; }
      Manifest rm;
      bool have = isOk(repl::fetchManifest(r.address, m.fileId, rm));
      bool complete = have;
      for (const auto& seg : m.segments) {
        if (have && rm.hasOp(seg.opId)) continue;
        complete = false;
        int fd = -1;
        std::function<size_t(char*, size_t)> src;
        std::string path = storageEngine->dataDir() + "/files/" + m.fileId + "/" + seg.diskName;
        makeFileSource(fd, path, src);
        if (fd < 0) continue;
        Status rs = repl::replicateSegment(r.address, m.fileId, m.filename, m.createOpId, seg,
                                            seg.size, src, seg.kind == SegKind::Base);
        ::close(fd);
        if (isOk(rs)) complete = true;
      }
      if (complete) ++confirmed;
    }
    // safe handoff: if we are no longer a designated replica and all designated replicas have
    // the full file, drop our local copy.
    if (!selfIsReplica && !reps.empty() &&
        confirmed == static_cast<int>(reps.size())) {
      storageEngine->removeFile(m.fileId);
      Metrics::instance().incr("handoffDelete");
    }
  }
}

}  // namespace aegisfs
