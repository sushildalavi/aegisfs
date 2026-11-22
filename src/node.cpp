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

static std::string random_hex(int bytes) {
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

static void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

Node::Node(NodeConfig cfg) : cfg_(std::move(cfg)) {}
Node::~Node() { stop(); }

std::shared_ptr<std::mutex> Node::file_lock(const std::string& file_id) {
  std::lock_guard<std::mutex> lk(file_locks_mu_);
  auto it = file_locks_.find(file_id);
  if (it == file_locks_.end()) {
    auto m = std::make_shared<std::mutex>();
    file_locks_[file_id] = m;
    return m;
  }
  return it->second;
}

std::string Node::load_or_create_identity() {
  std::string path = cfg_.data_dir + "/node.id";
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
  std::string id = cfg_.node_id.empty() ? ("node-" + random_hex(8)) : cfg_.node_id;
  fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd >= 0) {
    (void)::write(fd, id.data(), id.size());
    ::fsync(fd);
    ::close(fd);
  }
  return id;
}

uint64_t Node::next_lamport() { return lamport_.fetch_add(1) + 1; }
void Node::observe_lamport(uint64_t ts) {
  uint64_t cur = lamport_.load();
  while (ts > cur && !lamport_.compare_exchange_weak(cur, ts)) {}
}

Status Node::start() {
  storage_ = std::make_unique<StorageEngine>(cfg_.data_dir);
  if (!is_ok(storage_->init())) return Status::IoError;
  std::string id = load_or_create_identity();
  listener_ = Listener::bind(cfg_.host, cfg_.port);
  if (!listener_.valid()) return Status::NetworkError;
  port_ = listener_.port();
  std::string addr = cfg_.host + ":" + std::to_string(port_);
  membership_ = std::make_unique<Membership>(id, addr);
  // Seeds are contacted directly in gossip_once(); we do not insert placeholder members with
  // synthetic ids (that would pollute the ring). Real members are learned via gossip replies.
  pool_ = std::make_unique<ThreadPool>(static_cast<size_t>(cfg_.io_threads));
  running_ = true;
  accept_thread_ = std::thread([this] { accept_loop(); });
  gossip_thread_ = std::thread([this] { gossip_loop(); });
  fd_thread_ = std::thread([this] { failure_detector_loop(); });
  maint_thread_ = std::thread([this] { maintenance_loop(); });
  return Status::Ok;
}

void Node::stop() {
  bool was = running_.exchange(false);
  if (!was) return;
  listener_.close();
  if (accept_thread_.joinable()) accept_thread_.join();
  if (gossip_thread_.joinable()) gossip_thread_.join();
  if (fd_thread_.joinable()) fd_thread_.join();
  if (maint_thread_.joinable()) maint_thread_.join();
  if (pool_) pool_->shutdown();
}

Ring Node::current_ring() const {
  Ring r;
  r.set_members(membership_->ring_nodes());
  return r;
}

std::vector<RingNode> Node::replicas_for(const std::string& filename) const {
  return current_ring().successors(filename, cfg_.rf);
}

void Node::accept_loop() {
  while (running_) {
    Conn c = listener_.accept();
    if (!c.valid()) {
      if (!running_) break;
      continue;
    }
    auto sp = std::make_shared<Conn>(std::move(c));
    pool_->submit([this, sp] { handle_conn(sp); });
  }
}

void Node::handle_conn(std::shared_ptr<Conn> c) {
  c->set_recv_timeout(30000);
  c->set_send_timeout(30000);
  MsgType t;
  Fields h;
  uint64_t plen = 0;
  if (!is_ok(read_message_head(*c, t, h, plen))) return;
  switch (t) {
    case MsgType::Create: h_create(*c, h, plen); return;
    case MsgType::Append: h_append(*c, h, plen); return;
    case MsgType::ReplicatePut: h_replicate(*c, h, plen, true); return;
    case MsgType::ReplicateAppend: h_replicate(*c, h, plen, false); return;
    default: break;
  }
  // small-payload requests: pull payload into memory
  Frame fr;
  fr.type = t;
  fr.header = h;
  fr.payload.resize(plen);
  if (plen && !c->read_all(fr.payload.data(), plen)) return;
  switch (t) {
    case MsgType::Ping: h_ping(*c); break;
    case MsgType::Membership: h_membership(*c, fr); break;
    case MsgType::ListMemIds: h_list_mem_ids(*c); break;
    case MsgType::FetchManifest: h_fetch_manifest(*c, fr); break;
    case MsgType::FetchSegment: h_fetch_segment(*c, fr); break;
    case MsgType::GetFromReplica: h_get_from_replica(*c, fr); break;
    case MsgType::ListStore: h_liststore(*c); break;
    case MsgType::Ls: h_ls(*c, fr); break;
    case MsgType::Get: h_get(*c, fr); break;
    case MsgType::Merge: h_merge(*c, fr); break;
    case MsgType::Status_: h_status(*c); break;
    case MsgType::Health: h_health(*c); break;
    case MsgType::Leave: h_leave(*c); break;
    case MsgType::Metrics: h_metrics(*c); break;
    default: repl::send_reply(*c, Status::InvalidArgument); break;
  }
}

// ---------------- replica-role handlers ----------------

void Node::h_ping(Conn& c) { repl::send_reply(c, Status::Ok); }

void Node::h_membership(Conn& c, Frame& fr) {
  std::vector<Member> incoming;
  if (Membership::parse(fr.payload, incoming)) membership_->merge(incoming);
  // reply with our table so gossip is bidirectional
  Fields h;
  std::string body = membership_->serialize();
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  write_frame(c, MsgType::Reply, h, body);
}

void Node::h_list_mem_ids(Conn& c) {
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  write_frame(c, MsgType::Reply, h, membership_->serialize());
}

void Node::h_replicate(Conn& c, Fields& h, uint64_t plen, bool is_base) {
  std::string file_id = h.get_or("file_id");
  std::string filename = h.get_or("filename");
  std::string create_op = h.get_or("create_op_id");
  SegmentRef seg;
  if (file_id.empty() || !repl::seg_from_fields(h, seg)) {
    discard_payload(c, plen);
    repl::send_reply(c, Status::InvalidArgument);
    return;
  }
  observe_lamport(seg.lamport_ts);
  // Spool the payload to a temp file (verifying its SHA), then apply under the per-file lock.
  Sha256Digest got;
  std::string tmp;
  Status ss = spool_payload(c, plen, "repl", tmp, got);
  if (!is_ok(ss)) { repl::send_reply(c, ss); return; }
  if (got != seg.sha) {
    fs::remove(tmp);
    repl::send_reply(c, Status::ChecksumMismatch);
    return;
  }
  Status cs;
  {
    auto lk = file_lock(file_id);
    std::lock_guard<std::mutex> g(*lk);
    cs = apply_segment_local(file_id, filename, create_op, seg, tmp);
  }
  fs::remove(tmp);
  Metrics::instance().incr(is_base ? "replicate_put" : "replicate_append");
  repl::send_reply(c, cs);
}

void Node::h_fetch_manifest(Conn& c, Frame& fr) {
  std::string file_id = fr.header.get_or("file_id");
  Manifest m;
  if (!storage_->load_manifest(file_id, m)) {
    repl::send_reply(c, Status::NotFound);
    return;
  }
  std::string body;
  m.serialize(body);
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  write_frame(c, MsgType::Reply, h, body);
}

void Node::h_fetch_segment(Conn& c, Frame& fr) {
  std::string file_id = fr.header.get_or("file_id");
  std::string disk = fr.header.get_or("disk");
  // Determine size via manifest
  Manifest m;
  uint64_t size = 0;
  if (storage_->load_manifest(file_id, m)) {
    for (const auto& s : m.segments)
      if (s.disk_name == disk) size = s.size;
  }
  int fd = ::open((storage_->data_dir() + "/files/" + file_id + "/" + disk).c_str(), O_RDONLY);
  if (fd < 0) {
    Fields h;
    h.set_i64("status", static_cast<int64_t>(Status::NotFound));
    write_frame(c, MsgType::Reply, h, "");
    return;
  }
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  write_frame_streaming(c, MsgType::Reply, h, size, [&](char* b, size_t cap) -> size_t {
    ssize_t n = ::read(fd, b, cap);
    return n > 0 ? static_cast<size_t>(n) : 0;
  });
  ::close(fd);
}

void Node::h_get_from_replica(Conn& c, Frame& fr) {
  std::string filename = fr.header.get_or("file");
  std::string file_id = make_file_id(filename);
  Manifest m;
  if (!storage_->load_manifest(file_id, m)) {
    Fields h;
    h.set_i64("status", static_cast<int64_t>(Status::NotFound));
    write_frame(c, MsgType::Reply, h, "");
    return;
  }
  uint64_t total = m.total_size();
  // First pass: compute the logical file SHA-256 (also verifies each segment checksum) so the
  // reply header can carry the real file digest for merge/compare. Second pass streams bytes.
  Sha256Digest fsha{};
  Status fs = storage_->file_sha(m, fsha);
  if (!is_ok(fs)) {
    Fields h;
    h.set_i64("status", static_cast<int64_t>(fs));  // e.g. ChecksumMismatch on local corruption
    write_frame(c, MsgType::Reply, h, "");
    return;
  }
  auto ordered = m.ordered();
  size_t idx = 0;
  std::string cur;
  size_t cur_off = 0;
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  h.set("file_sha", to_hex(fsha));
  h.set_u64("size", total);
  write_frame_streaming(c, MsgType::Reply, h, total, [&](char* b, size_t cap) -> size_t {
    while (cur_off >= cur.size()) {
      if (idx >= ordered.size()) return 0;
      cur.clear();
      cur_off = 0;
      if (!is_ok(storage_->read_segment(file_id, ordered[idx]->disk_name, cur))) return 0;
      ++idx;
    }
    size_t n = std::min(cap, cur.size() - cur_off);
    std::memcpy(b, cur.data() + cur_off, n);
    cur_off += n;
    return n;
  });
}

void Node::h_liststore(Conn& c) {
  auto ms = storage_->list_manifests();
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  h.set_i64("count", static_cast<int64_t>(ms.size()));
  int i = 0;
  for (auto& m : ms) {
    std::string pfx = "f" + std::to_string(i++) + "_";
    h.set(pfx + "name", m.filename);
    h.set(pfx + "id", m.file_id);
    h.set_u64(pfx + "size", m.total_size());
    h.set_u64(pfx + "version", m.version);
    h.set_i64(pfx + "segs", static_cast<int64_t>(m.segments.size()));
  }
  write_frame(c, MsgType::Reply, h, "");
}

void Node::h_ls(Conn& c, Frame& fr) {
  std::string filename = fr.header.get_or("file");
  std::string file_id = make_file_id(filename);
  auto reps = replicas_for(filename);
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  h.set("file", filename);
  h.set("file_id", file_id);
  h.set_i64("replicas", static_cast<int64_t>(reps.size()));
  int present = 0;
  for (size_t i = 0; i < reps.size(); ++i) {
    std::string pfx = "r" + std::to_string(i) + "_";
    h.set(pfx + "node", reps[i].node_id);
    h.set(pfx + "addr", reps[i].address);
    // verified presence: does this intended replica actually hold the manifest right now?
    Manifest m;
    bool has = is_ok(repl::fetch_manifest(reps[i].address, file_id, m));
    h.set(pfx + "present", has ? "yes" : "no");
    if (has) ++present;
  }
  h.set_i64("present_count", present);
  write_frame(c, MsgType::Reply, h, "");
}

void Node::h_status(Conn& c) {
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  h.set("node_id", membership_->self_id());
  h.set("address", address());
  h.set_i64("alive", static_cast<int64_t>(membership_->alive_count()));
  h.set_i64("files", static_cast<int64_t>(storage_->list_manifests().size()));
  h.set_u64("lamport", lamport_.load());
  write_frame(c, MsgType::Reply, h, "");
}

void Node::h_health(Conn& c) { repl::send_reply(c, Status::Ok); }

void Node::h_metrics(Conn& c) {
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  for (const auto& [k, v] : Metrics::instance().snapshot_counters()) h.set_u64("counter." + k, v);
  for (const char* op : {"create", "append", "get", "merge"}) {
    auto s = Metrics::instance().latency(op);
    if (s.count == 0) continue;
    std::string pfx = std::string("lat.") + op + ".";
    h.set_u64(pfx + "count", s.count);
    h.set_u64(pfx + "p50_us", s.p50);
    h.set_u64(pfx + "p95_us", s.p95);
    h.set_u64(pfx + "p99_us", s.p99);
  }
  write_frame(c, MsgType::Reply, h, "");
}

void Node::h_leave(Conn& c) {
  // Graceful leave: exclude self from the ring, then hand off our replicas to the new successors
  // (reconcile_placement pushes missing data and safely deletes our now-redundant copies), and
  // disseminate the LEFT state so peers stop routing to us.
  membership_->mark_left(membership_->self_id());
  reconcile_placement();
  gossip_once();
  Metrics::instance().incr("graceful_leave");
  repl::send_reply(c, Status::Ok);
}

// ---------------- coordinator-role handlers ----------------

Status Node::spool_payload(Conn& c, uint64_t plen, const std::string& tag, std::string& tmp_path,
                           Sha256Digest& sha) {
  tmp_path = storage_->data_dir() + "/tmp/" + tag + "." + random_hex(8);
  int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::IoError;
  Sha256Stream hasher;
  Status s = stream_payload_in(c, plen, [&](const char* b, size_t n) {
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
  if (!is_ok(s)) { fs::remove(tmp_path); return s; }
  sha = hasher.finalize();
  return Status::Ok;
}

Status Node::apply_segment_local(const std::string& file_id, const std::string& filename,
                                 const std::string& create_op, const SegmentRef& seg,
                                 const std::string& src_path) {
  // Lock-free: the caller holds the per-file lock. Enforces idempotency and first-create-only.
  Manifest m;
  bool have = storage_->load_manifest(file_id, m);
  if (have && m.has_op(seg.op_id)) return Status::Ok;  // idempotent replay
  if (seg.kind == SegKind::Base && have) {
    for (const auto& s : m.segments) {
      if (s.kind == SegKind::Base && s.op_id != seg.op_id) return Status::AlreadyExists;
    }
  }
  int fd = ::open(src_path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IoError;
  Sha256Digest stored;
  Status ps = storage_->put_segment_stream(
      file_id, seg.disk_name, seg.size,
      [&](char* b, size_t cap) -> size_t {
        ssize_t n = ::read(fd, b, cap);
        return n > 0 ? static_cast<size_t>(n) : 0;
      },
      stored);
  ::close(fd);
  if (!is_ok(ps)) return ps;
  if (stored != seg.sha) return Status::ChecksumMismatch;
  if (!have) {
    m.filename = filename;
    m.file_id = file_id;
    m.create_op_id = create_op;
    m.version = 0;
  }
  m.add_segment(seg);
  return storage_->commit_manifest(m);
}

Status Node::apply_local_locked(const std::string& file_id, const std::string& filename,
                                const std::string& create_op, const SegmentRef& seg,
                                const std::string& src_path) {
  auto lk = file_lock(file_id);
  std::lock_guard<std::mutex> g(*lk);
  return apply_segment_local(file_id, filename, create_op, seg, src_path);
}

int Node::fanout(const std::vector<RingNode>& targets, const std::string& file_id,
                 const std::string& filename, const std::string& create_op, const SegmentRef& seg,
                 const std::string& src_path, uint64_t plen, bool is_base) {
  auto one = [&](const RingNode& r) -> Status {
    if (r.node_id == membership_->self_id())
      return apply_local_locked(file_id, filename, create_op, seg, src_path);
    int fd = ::open(src_path.c_str(), O_RDONLY);
    if (fd < 0) return Status::IoError;
    std::function<size_t(char*, size_t)> src = [fd](char* b, size_t cap) -> size_t {
      ssize_t n = ::read(fd, b, cap);
      return n > 0 ? static_cast<size_t>(n) : 0;
    };
    Status rs = repl::replicate_segment(r.address, file_id, filename, create_op, seg, plen, src,
                                        is_base);
    ::close(fd);
    return rs;
  };
  std::atomic<int> committed{0};
  if (!cfg_.parallel_replication || targets.size() <= 1) {
    for (const auto& r : targets)
      if (is_ok(one(r))) committed.fetch_add(1);
  } else {
    std::vector<std::thread> ts;
    for (const auto& r : targets)
      ts.emplace_back([&, r] { if (is_ok(one(r))) committed.fetch_add(1); });
    for (auto& t : ts) t.join();
  }
  return committed.load();
}

static size_t make_file_source(int& fd_holder, const std::string& path,
                               std::function<size_t(char*, size_t)>& src) {
  int fd = ::open(path.c_str(), O_RDONLY);
  fd_holder = fd;
  src = [fd](char* b, size_t cap) -> size_t {
    ssize_t n = ::read(fd, b, cap);
    return n > 0 ? static_cast<size_t>(n) : 0;
  };
  return fd >= 0 ? 1 : 0;
}

void Node::h_create(Conn& c, Fields& h, uint64_t plen) {
  ScopedTimer timer("create");
  std::string filename = h.get_or("file");
  std::string client_id = h.get_or("client_id", "anon");
  std::string op_id = h.get_or("op_id");
  if (op_id.empty()) op_id = client_id + "#create#" + random_hex(6);
  if (!StorageEngine::valid_filename(filename)) {
    discard_payload(c, plen);
    repl::send_reply(c, Status::InvalidArgument);
    return;
  }
  std::string file_id = make_file_id(filename);
  std::string tmp;
  Sha256Digest sha;
  Status ss = spool_payload(c, plen, "create", tmp, sha);
  if (!is_ok(ss)) { repl::send_reply(c, ss); return; }

  auto reps = replicas_for(filename);
  if (reps.empty()) { fs::remove(tmp); repl::send_reply(c, Status::Unavailable); return; }

  SegmentRef seg;
  seg.kind = SegKind::Base;
  seg.op_id = op_id;
  seg.client_id = client_id;
  seg.client_seq = 0;
  seg.lamport_ts = next_lamport();
  seg.size = plen;
  seg.sha = sha;
  seg.disk_name = "base.seg";

  // Primary-arbitrated first-create-only: write the base to the primary (reps[0]) first; it
  // atomically rejects a duplicate base (different op_id). Concurrent creates serialize there.
  // The coordinator holds NO cross-node lock; each node locks only its own local apply.
  auto write_to = [&](const RingNode& r) -> Status {
    if (r.node_id == membership_->self_id()) return apply_local_locked(file_id, filename, op_id, seg, tmp);
    int fd = -1;
    std::function<size_t(char*, size_t)> src;
    make_file_source(fd, tmp, src);
    if (fd < 0) return Status::IoError;
    Status rs = repl::replicate_segment(r.address, file_id, filename, op_id, seg, plen, src, true);
    ::close(fd);
    return rs;
  };

  Status primary = write_to(reps[0]);
  if (primary == Status::AlreadyExists) {
    fs::remove(tmp);
    repl::send_reply(c, Status::AlreadyExists);
    return;
  }
  int committed = is_ok(primary) ? 1 : 0;
  // fan out to the remaining replicas (sequential or parallel per config)
  std::vector<RingNode> rest(reps.begin() + 1, reps.end());
  committed += fanout(rest, file_id, filename, op_id, seg, tmp, plen, true);
  fs::remove(tmp);
  Fields reply;
  reply.set("file_id", file_id);
  reply.set_i64("committed", committed);
  reply.set_i64("rf", cfg_.rf);
  for (size_t i = 0; i < reps.size(); ++i)
    reply.set("r" + std::to_string(i), reps[i].address);
  Status final = (committed == static_cast<int>(reps.size())) ? Status::Ok : Status::Degraded;
  if (committed == 0) final = Status::Unavailable;
  repl::send_reply(c, final, reply);
}

Status Node::authoritative_manifest(const std::string& filename, Manifest& out,
                                    std::vector<RingNode>& live_replicas) {
  std::string file_id = make_file_id(filename);
  auto reps = replicas_for(filename);
  bool found = false;
  Manifest merged;
  merged.filename = filename;
  merged.file_id = file_id;
  for (const auto& r : reps) {
    Manifest m;
    if (is_ok(repl::fetch_manifest(r.address, file_id, m))) {
      live_replicas.push_back(r);
      if (!found) {
        merged = m;
        found = true;
      } else {
        for (const auto& s : m.segments) merged.add_segment(s);
        if (merged.create_op_id.empty()) merged.create_op_id = m.create_op_id;
      }
    }
  }
  if (!found) return Status::NotFound;
  out = merged;
  return Status::Ok;
}

void Node::h_append(Conn& c, Fields& h, uint64_t plen) {
  ScopedTimer timer("append");
  std::string filename = h.get_or("file");
  std::string client_id = h.get_or("client_id", "anon");
  std::string op_id = h.get_or("op_id");
  if (op_id.empty()) op_id = client_id + "#" + random_hex(8);
  if (!StorageEngine::valid_filename(filename)) {
    discard_payload(c, plen);
    repl::send_reply(c, Status::InvalidArgument);
    return;
  }
  std::string file_id = make_file_id(filename);
  std::string tmp;
  Sha256Digest sha;
  Status ss = spool_payload(c, plen, "append", tmp, sha);
  if (!is_ok(ss)) { repl::send_reply(c, ss); return; }

  Manifest auth;
  std::vector<RingNode> live;
  Status as = authoritative_manifest(filename, auth, live);
  if (!is_ok(as)) { fs::remove(tmp); repl::send_reply(c, as); return; }

  // idempotent retry
  if (auth.has_op(op_id)) {
    fs::remove(tmp);
    Fields reply;
    reply.set("note", "idempotent");
    repl::send_reply(c, Status::Ok, reply);
    return;
  }
  // per-client sequence = max existing for this client + 1
  uint64_t seq = 0;
  for (const auto& s : auth.segments)
    if (s.client_id == client_id) seq = std::max(seq, s.client_seq);
  seq += 1;

  SegmentRef seg;
  seg.kind = SegKind::Append;
  seg.op_id = op_id;
  seg.client_id = client_id;
  seg.client_seq = seq;
  seg.lamport_ts = next_lamport();
  seg.size = plen;
  seg.sha = sha;
  seg.disk_name = "append-" + sanitize(op_id) + ".seg";

  auto reps = replicas_for(filename);
  int committed = fanout(reps, file_id, filename, auth.create_op_id, seg, tmp, plen, false);
  fs::remove(tmp);
  Fields reply;
  reply.set_i64("committed", committed);
  reply.set_u64("client_seq", seq);
  reply.set("op_id", op_id);
  Status final = (committed == static_cast<int>(reps.size())) ? Status::Ok : Status::Degraded;
  if (committed == 0) final = Status::Unavailable;
  repl::send_reply(c, final, reply);
}

void Node::h_get(Conn& c, Frame& fr) {
  ScopedTimer timer("get");
  std::string filename = fr.header.get_or("file");
  std::string client_id = fr.header.get_or("client_id", "anon");
  uint64_t required_seq = fr.header.get_u64("required_seq");
  std::string file_id = make_file_id(filename);
  auto reps = replicas_for(filename);

  // choose a replica whose manifest satisfies the read-your-writes watermark
  std::string chosen_addr;
  Manifest chosen;
  bool have_file = false;
  for (const auto& r : reps) {
    Manifest m;
    if (!is_ok(repl::fetch_manifest(r.address, file_id, m))) continue;
    have_file = true;
    uint64_t maxseq = 0;
    for (const auto& s : m.segments)
      if (s.client_id == client_id) maxseq = std::max(maxseq, s.client_seq);
    if (maxseq >= required_seq) {
      chosen_addr = r.address;
      chosen = m;
      break;
    }
  }
  if (chosen_addr.empty()) {
    Fields h;
    Status st = have_file ? Status::Stale : (reps.empty() ? Status::Unavailable : Status::NotFound);
    h.set_i64("status", static_cast<int64_t>(st));
    write_frame(c, MsgType::Reply, h, "");
    return;
  }
  // stream the file from the chosen replica to the client.
  uint64_t total = chosen.total_size();
  Fields h;
  h.set_i64("status", static_cast<int64_t>(Status::Ok));
  h.set_u64("size", total);
  // Pull from replica, forward to client. Use a pipe via buffered relay.
  std::string host; int port;
  parse_endpoint(chosen_addr, host, port);
  Conn rc = Conn::dial(host, port, 30000);
  if (!rc.valid()) {
    h.set_i64("status", static_cast<int64_t>(Status::Unavailable));
    write_frame(c, MsgType::Reply, h, "");
    return;
  }
  Fields req;
  req.set("file", filename);
  write_frame(rc, MsgType::GetFromReplica, req, "");
  MsgType rt; Fields rh; uint64_t rplen = 0;
  if (!is_ok(read_message_head(rc, rt, rh, rplen))) {
    h.set_i64("status", static_cast<int64_t>(Status::NetworkError));
    write_frame(c, MsgType::Reply, h, "");
    return;
  }
  // relay rplen bytes from rc to client c
  write_frame_streaming(c, MsgType::Reply, h, rplen, [&](char* b, size_t cap) -> size_t {
    // read up to cap from rc
    ssize_t n = ::recv(rc.fd(), b, cap, 0);
    return n > 0 ? static_cast<size_t>(n) : 0;
  });
}

void Node::h_merge(Conn& c, Frame& fr) {
  ScopedTimer timer("merge");
  std::string filename = fr.header.get_or("file");
  std::string file_id = make_file_id(filename);
  // No cross-node lock held during fan-out (would deadlock with replica-side locks). Self-repair
  // uses apply_local_locked for its short local critical section.
  Manifest auth;
  std::vector<RingNode> live;
  Status as = authoritative_manifest(filename, auth, live);
  if (!is_ok(as)) { repl::send_reply(c, as); return; }

  // Ensure every live replica has every op in the authoritative (union) manifest.
  for (const auto& r : live) {
    Manifest m;
    if (!is_ok(repl::fetch_manifest(r.address, file_id, m))) continue;
    for (const auto& seg : auth.segments) {
      if (m.has_op(seg.op_id)) continue;
      // find a source replica that has this op's segment
      std::string src_addr;
      for (const auto& s : live) {
        Manifest sm;
        if (is_ok(repl::fetch_manifest(s.address, file_id, sm)) && sm.has_op(seg.op_id)) {
          src_addr = s.address;
          break;
        }
      }
      if (src_addr.empty()) continue;
      std::string tmp = storage_->data_dir() + "/tmp/merge." + random_hex(8);
      int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd < 0) continue;
      repl::fetch_segment(src_addr, file_id, seg.disk_name, [&](const char* b, size_t n) {
        ssize_t w = ::write(fd, b, n);
        return w == static_cast<ssize_t>(n);
      });
      ::fsync(fd);
      ::close(fd);
      if (r.node_id == membership_->self_id()) {
        apply_local_locked(file_id, filename, auth.create_op_id, seg, tmp);
      } else {
        int rfd = -1;
        std::function<size_t(char*, size_t)> src;
        make_file_source(rfd, tmp, src);
        if (rfd >= 0) {
          repl::replicate_segment(r.address, file_id, filename, auth.create_op_id, seg, seg.size,
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
    Status gs = repl::get_from_replica(r.address, filename,
                                       [](const char*, size_t) { return true; }, fsha);
    if (!is_ok(gs)) { identical = false; break; }
    if (first) { ref = fsha; first = false; }
    else if (fsha != ref) { identical = false; break; }
  }
  Fields reply;
  reply.set_i64("replicas", static_cast<int64_t>(live.size()));
  reply.set("file_sha", to_hex(ref));
  repl::send_reply(c, identical ? Status::Ok : Status::Conflict, reply);
}

// ---------------- background loops ----------------

void Node::gossip_once() {
  // Targets: a random sample of known alive peers, plus all configured seeds (by address).
  std::vector<std::string> targets;
  auto peers = membership_->alive();
  for (auto& m : peers)
    if (m.node_id != membership_->self_id()) targets.push_back(m.address);
  static thread_local std::mt19937 rng(std::random_device{}());
  std::shuffle(targets.begin(), targets.end(), rng);
  if (targets.size() > 3) targets.resize(3);
  for (const auto& s : cfg_.seeds) {
    if (s.empty() || s == address()) continue;
    if (std::find(targets.begin(), targets.end(), s) == targets.end()) targets.push_back(s);
  }
  std::string body = membership_->serialize();
  for (const auto& addr : targets) {
    std::string host; int port;
    if (!parse_endpoint(addr, host, port)) continue;
    Conn c = Conn::dial(host, port, 1000);
    if (!c.valid()) continue;
    Fields h;
    if (is_ok(write_frame(c, MsgType::Membership, h, body))) {
      Frame reply;
      if (is_ok(read_frame(c, reply))) {
        std::vector<Member> incoming;
        if (Membership::parse(reply.payload, incoming)) membership_->merge(incoming);
      }
    }
  }
}

void Node::gossip_loop() {
  while (running_) {
    gossip_once();
    sleep_ms(cfg_.gossip_interval_ms);
  }
}

void Node::failure_detector_loop() {
  while (running_) {
    auto peers = membership_->alive();
    for (auto& m : peers) {
      if (!running_) break;
      if (m.node_id == membership_->self_id()) continue;
      std::string host; int port;
      if (!parse_endpoint(m.address, host, port)) continue;
      Conn c = Conn::dial(host, port, cfg_.probe_interval_ms);
      bool ok = false;
      if (c.valid()) {
        Fields h;
        if (is_ok(write_frame(c, MsgType::Ping, h, ""))) {
          Frame reply;
          ok = is_ok(read_frame(c, reply));
        }
      }
      if (!ok) membership_->mark_suspect(m.node_id);
      else membership_->mark_alive(m.node_id, m.address, m.incarnation);
    }
    sleep_ms(cfg_.probe_interval_ms);
  }
}

void Node::maintenance_loop() {
  while (running_) {
    auto failed = membership_->tick(static_cast<uint64_t>(cfg_.suspect_ms),
                                    static_cast<uint64_t>(cfg_.reap_ms));
    reconcile_placement();
    anti_entropy();
    (void)failed;
    sleep_ms(cfg_.reconcile_ms);
  }
}

void Node::anti_entropy() {
  if (!storage_) return;
  auto manifests = storage_->list_manifests();
  for (auto& m : manifests) {
    auto reps = replicas_for(m.filename);
    bool self_replica = false;
    for (const auto& r : reps)
      if (r.node_id == membership_->self_id()) self_replica = true;
    if (!self_replica) continue;  // push/handoff path owns non-replica files

    // 1) detect locally corrupt/missing segments (checksum mismatch) that need re-fetch.
    std::vector<SegmentRef> need = {};
    for (const auto& seg : m.segments) {
      Sha256Stream hsh;
      bool ok = is_ok(storage_->read_segment_stream(
          m.file_id, seg.disk_name, [&](const char* b, size_t n) { hsh.update(b, n); return true; }));
      if (!ok || hsh.finalize() != seg.sha) need.push_back(seg);
    }

    // 2) learn ops present on peer replicas but missing locally.
    for (const auto& r : reps) {
      if (r.node_id == membership_->self_id()) continue;
      Manifest pm;
      if (!is_ok(repl::fetch_manifest(r.address, m.file_id, pm))) continue;
      for (const auto& seg : pm.segments)
        if (!m.has_op(seg.op_id)) need.push_back(seg);
    }
    if (need.empty()) continue;

    // 3) pull each needed segment from a peer replica that has a good copy, apply locally.
    for (const auto& seg : need) {
      std::string src;
      for (const auto& r : reps) {
        if (r.node_id == membership_->self_id()) continue;
        Manifest pm;
        if (is_ok(repl::fetch_manifest(r.address, m.file_id, pm)) && pm.has_op(seg.op_id)) {
          src = r.address;
          break;
        }
      }
      if (src.empty()) continue;
      std::string tmp = storage_->data_dir() + "/tmp/ae." + random_hex(8);
      int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd < 0) continue;
      Status fs = repl::fetch_segment(src, m.file_id, seg.disk_name, [&](const char* b, size_t n) {
        ssize_t w = ::write(fd, b, n);
        return w == static_cast<ssize_t>(n);
      });
      ::fsync(fd);
      ::close(fd);
      if (is_ok(fs)) {
        if (m.has_op(seg.op_id)) {
          // corrupt-segment repair: overwrite the bytes in place (manifest already has the op)
          std::string bytes;
          int rfd = ::open(tmp.c_str(), O_RDONLY);
          if (rfd >= 0) {
            char buf[65536]; ssize_t n;
            while ((n = ::read(rfd, buf, sizeof(buf))) > 0) bytes.append(buf, static_cast<size_t>(n));
            ::close(rfd);
            auto lk = file_lock(m.file_id);
            std::lock_guard<std::mutex> g(*lk);
            if (is_ok(storage_->put_segment(m.file_id, seg.disk_name, bytes, seg.sha)))
              Metrics::instance().incr("anti_entropy_repair");
          }
        } else {
          // missing op: apply as a new segment
          if (is_ok(apply_local_locked(m.file_id, m.filename, m.create_op_id, seg, tmp)))
            Metrics::instance().incr("anti_entropy_repair");
        }
      }
      fs::remove(tmp);
    }
  }
}

void Node::reconcile_placement() {
  if (!storage_) return;
  auto manifests = storage_->list_manifests();
  for (auto& m : manifests) {
    auto reps = replicas_for(m.filename);
    bool self_is_replica = false;
    for (const auto& r : reps)
      if (r.node_id == membership_->self_id()) self_is_replica = true;

    // push any ops the designated replicas are missing (re-replication / rebalance)
    int confirmed = 0;
    for (const auto& r : reps) {
      if (r.node_id == membership_->self_id()) { ++confirmed; continue; }
      Manifest rm;
      bool have = is_ok(repl::fetch_manifest(r.address, m.file_id, rm));
      bool complete = have;
      for (const auto& seg : m.segments) {
        if (have && rm.has_op(seg.op_id)) continue;
        complete = false;
        int fd = -1;
        std::function<size_t(char*, size_t)> src;
        std::string path = storage_->data_dir() + "/files/" + m.file_id + "/" + seg.disk_name;
        make_file_source(fd, path, src);
        if (fd < 0) continue;
        Status rs = repl::replicate_segment(r.address, m.file_id, m.filename, m.create_op_id, seg,
                                            seg.size, src, seg.kind == SegKind::Base);
        ::close(fd);
        if (is_ok(rs)) complete = true;
      }
      if (complete) ++confirmed;
    }
    // safe handoff: if we are no longer a designated replica and all designated replicas have
    // the full file, drop our local copy.
    if (!self_is_replica && !reps.empty() &&
        confirmed == static_cast<int>(reps.size())) {
      storage_->remove_file(m.file_id);
      Metrics::instance().incr("handoff_delete");
    }
  }
}

}  // namespace aegisfs
