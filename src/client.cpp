#include "aegisfs/client.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "aegisfs/hash.hpp"
#include "aegisfs/net.hpp"

namespace aegisfs {

namespace {
std::string newOperationInstance() {
  static std::atomic<uint64_t> sequence{0};
  std::random_device rd;
  const uint64_t entropy = (static_cast<uint64_t>(rd()) << 32) ^ rd();
  return std::to_string(::getpid()) + "-" + std::to_string(sequence.fetch_add(1)) + "-" +
         std::to_string(entropy);
}
}  // namespace

// How long a client waits for a node to answer a request once connected.
static constexpr int kReplyTimeoutMs = 60000;

Client::Client(std::string entryAddr, std::string clientId)
    : entry(std::move(entryAddr)), clientIdValue(std::move(clientId)),
      operationInstance(newOperationInstance()) {}

static bool fileSize(const std::string& path, uint64_t& out) {
  struct stat st{};
  if (::stat(path.c_str(), &st) != 0) return false;
  out = static_cast<uint64_t>(st.st_size);
  return true;
}

static OpResult dialFail() { return OpResult{Status::NetworkError, {}, "cannot reach entry node"}; }

bool Client::retryable(Status s) const {
  return s == Status::NetworkError || s == Status::Timeout || s == Status::Unavailable ||
         s == Status::Degraded || s == Status::Stale;
}

OpResult Client::upload(MsgType t, const std::string& localPath, const std::string& name,
                        uint64_t offset, uint64_t length, const std::string& opIdPrefix,
                        bool stripeIndex) {
  uint64_t sz = 0;
  if (!fileSize(localPath, sz)) return OpResult{Status::NotFound, {}, "local file not found"};
  if (offset > sz) return OpResult{Status::InvalidArgument, {}, "offset past end of file"};
  sz = std::min<uint64_t>(sz - offset, length);
  if (retry.maxAttempts < 1) retry.maxAttempts = 1;
  // A stable operation id makes retries safe even after the first replica committed.
  const std::string opId = clientIdValue + "#session#" + operationInstance +
                           (opIdPrefix.empty() ? "#op#" : opIdPrefix) +
                           std::to_string(++opCounter);
  OpResult last = dialFail();
  for (int attempt = 0; attempt < retry.maxAttempts; ++attempt) {
    std::string host; int port;
    if (!parseEndpoint(entry, host, port)) return OpResult{Status::InvalidArgument, {}, "bad entry"};
    Conn c = Conn::dial(host, port, 10000);
    if (!c.valid()) last = dialFail();
    else {
      // Connecting should be quick, but the coordinator may spend several timeouts on replicas
      // it cannot reach before it answers, so wait much longer for the reply than for the dial.
      c.setRecvTimeout(kReplyTimeoutMs);
      int fd = ::open(localPath.c_str(), O_RDONLY);
      if (fd < 0) return OpResult{Status::IoError, {}, "open failed"};
      if (offset && ::lseek(fd, static_cast<off_t>(offset), SEEK_SET) < 0) {
        ::close(fd);
        return OpResult{Status::IoError, {}, "seek failed"};
      }
      Fields h;
      h.set("file", name);
      h.set("clientId", clientIdValue);
      h.set("opId", opId);
      if (stripeIndex) h.set("stripeIndex", "1");
      uint64_t remaining = sz;
      Status ws = writeFrameStreaming(c, t, h, sz, [&](char* b, size_t cap) -> size_t {
        ssize_t n = ::read(fd, b, static_cast<size_t>(std::min<uint64_t>(cap, remaining)));
        if (n <= 0) return 0;
        remaining -= static_cast<uint64_t>(n);
        return static_cast<size_t>(n);
      });
      ::close(fd);
      if (!isOk(ws)) last = OpResult{ws, {}, "upload failed"};
      else {
        Frame reply;
        Status rs = readFrame(c, reply);
        if (!isOk(rs)) last = OpResult{rs, {}, "no reply"};
        else {
          last.status = static_cast<Status>(reply.header.getI64("status", static_cast<int64_t>(Status::Internal)));
          last.info = reply.header;
          last.message.clear();
        }
      }
    }
    if (!retryable(last.status) || attempt + 1 == retry.maxAttempts) return last;
    std::this_thread::sleep_for(std::chrono::milliseconds(retry.backoffMs * (attempt + 1)));
  }
  return last;
}

namespace {
constexpr const char* kStripeHeader = "AEGISFS-STRIPES 1\n";
constexpr size_t kMaxIndexBytes = 64u * 1024 * 1024;
constexpr int kStripeParallelism = 4;

std::string tempPath(const std::string& tag) {
  static std::atomic<uint64_t> counter{0};
  return (std::filesystem::temp_directory_path() /
          ("aegisfs_" + tag + "_" + std::to_string(::getpid()) + "_" +
           std::to_string(counter.fetch_add(1))))
      .string();
}

std::string indexLines(const std::vector<std::pair<std::string, uint64_t>>& chunks) {
  std::string out;
  for (const auto& c : chunks) out += c.first + "\t" + std::to_string(c.second) + "\n";
  return out;
}

bool writeTextFile(const std::string& path, const std::string& text) {
  std::ofstream f(path, std::ios::binary | std::ios::trunc);
  f << text;
  return static_cast<bool>(f);
}

bool parseIndex(const std::string& text, std::vector<std::pair<std::string, uint64_t>>& chunks) {
  chunks.clear();
  std::istringstream in(text);
  std::string line;
  if (!std::getline(in, line) || line + "\n" != kStripeHeader) return false;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    auto tab = line.rfind('\t');
    if (tab == std::string::npos) return false;
    try {
      chunks.emplace_back(line.substr(0, tab), std::stoull(line.substr(tab + 1)));
    } catch (...) {
      return false;
    }
  }
  return true;
}

Status worse(Status a, Status b) {
  auto rank = [](Status s) { return s == Status::Ok ? 0 : s == Status::Degraded ? 1 : 2; };
  return rank(b) > rank(a) ? b : a;
}
}  // namespace

OpResult Client::uploadChunks(const std::string& localPath, const std::string& name, uint64_t size,
                              std::vector<std::pair<std::string, uint64_t>>& chunks) {
  const uint64_t count = (size + stripeBytes - 1) / stripeBytes;
  const std::string unique =
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  chunks.clear();
  for (uint64_t i = 0; i < count; ++i) {
    chunks.emplace_back(name + ".stripe." + clientIdValue + "." + unique + "." + std::to_string(i),
                        std::min<uint64_t>(stripeBytes, size - i * stripeBytes));
  }
  std::vector<OpResult> results(count);
  std::atomic<uint64_t> next{0};
  std::vector<std::thread> workers;
  const int n = static_cast<int>(std::min<uint64_t>(count, kStripeParallelism));
  for (int w = 0; w < n; ++w) {
    workers.emplace_back([&, w] {
      // Each worker has its own client identity so op ids and sequence numbers never collide.
      Client sub(entry, clientIdValue + "-s" + std::to_string(w) + "-" + unique);
      sub.retry = retry;
      sub.stripeBytes = 0;
      for (uint64_t i = next.fetch_add(1); i < count; i = next.fetch_add(1)) {
        results[i] = sub.upload(MsgType::Create, localPath, chunks[i].first, i * stripeBytes,
                                chunks[i].second);
      }
    });
  }
  for (auto& t : workers) t.join();
  OpResult total{Status::Ok, {}, ""};
  for (uint64_t i = 0; i < count; ++i) {
    if (results[i].status != Status::Ok && results[i].status != Status::Degraded) return results[i];
    total.status = worse(total.status, results[i].status);
  }
  return total;
}

OpResult Client::createStriped(const std::string& localPath, const std::string& name,
                               uint64_t size) {
  std::vector<std::pair<std::string, uint64_t>> chunks;
  OpResult data = uploadChunks(localPath, name, size, chunks);
  if (data.status != Status::Ok && data.status != Status::Degraded) return data;
  // The index is written last: it is the commit point, so a reader never sees a file whose
  // chunks are not all stored. A failure before this line only leaves unreferenced chunk files.
  std::string indexPath = tempPath("index");
  if (!writeTextFile(indexPath, std::string(kStripeHeader) + indexLines(chunks)))
    return OpResult{Status::IoError, {}, "cannot write index"};
  OpResult r = upload(MsgType::Create, indexPath, name, 0, UINT64_MAX, kStripeMarker);
  std::error_code ec;
  std::filesystem::remove(indexPath, ec);
  if (r.status == Status::Ok || r.status == Status::Degraded) {
    r.status = worse(r.status, data.status);
    r.info.setU64("stripes", chunks.size());
    r.info.setU64("size", size);
  }
  return r;
}

OpResult Client::appendStriped(const std::string& localPath, const std::string& name,
                               uint64_t size) {
  std::vector<std::pair<std::string, uint64_t>> chunks;
  OpResult data = uploadChunks(localPath, name, size, chunks);
  if (data.status != Status::Ok && data.status != Status::Degraded) return data;
  std::string indexPath = tempPath("index");
  if (!writeTextFile(indexPath, indexLines(chunks)))
    return OpResult{Status::IoError, {}, "cannot write index"};
  OpResult r = upload(MsgType::Append, indexPath, name, 0, UINT64_MAX, "#op#", /*stripeIndex=*/true);
  std::error_code ec;
  std::filesystem::remove(indexPath, ec);
  if (r.ok() || r.status == Status::Degraded) {
    uint64_t seq = r.info.getU64("clientSeq");
    if (seq > watermarks[name]) watermarks[name] = seq;
    r.status = worse(r.status, data.status);
    r.info.setU64("stripes", chunks.size());
  }
  return r;
}

OpResult Client::create(const std::string& localPath, const std::string& name) {
  uint64_t size = 0;
  if (stripeBytes > 0 && fileSize(localPath, size) && size > stripeBytes)
    return createStriped(localPath, name, size);
  return upload(MsgType::Create, localPath, name);
}

OpResult Client::append(const std::string& localPath, const std::string& name) {
  uint64_t size = 0;
  fileSize(localPath, size);
  OpResult r = upload(MsgType::Append, localPath, name);
  if (r.status == Status::InvalidArgument && r.info.getOr("striped") == "1") {
    // The file is striped, so this data has to become new chunks plus an index entry.
    if (stripeBytes == 0) return OpResult{Status::InvalidArgument, {}, "file is striped but striping is off"};
    return appendStriped(localPath, name, size);
  }
  if (r.ok() || r.status == Status::Degraded) {
    uint64_t seq = r.info.getU64("clientSeq");
    if (seq > watermarks[name]) watermarks[name] = seq;
  }
  return r;
}

OpResult Client::get(const std::string& name, const std::string& localPath) {
  return getImpl(name, localPath, false);
}

OpResult Client::getQuorum(const std::string& name, const std::string& localPath) {
  return getImpl(name, localPath, true);
}

OpResult Client::getImpl(const std::string& name, const std::string& localPath, bool quorum) {
  OpResult last = dialFail();
  int attempts = std::max(1, retry.maxAttempts);
  for (int attempt = 0; attempt < attempts; ++attempt) {
  std::string host; int port;
  if (!parseEndpoint(entry, host, port)) return OpResult{Status::InvalidArgument, {}, "bad entry"};
  Conn c = Conn::dial(host, port, 30000);
  if (!c.valid()) return dialFail();
  Fields h;
  h.set("file", name);
  h.set("clientId", clientIdValue);
  h.setU64("requiredSeq", watermarks.count(name) ? watermarks[name] : 0);
  if (quorum) h.set("readMode", "quorum");
  Status ws = writeFrame(c, MsgType::Get, h, "");
  if (!isOk(ws)) last = OpResult{ws, {}, "send failed"};
  else {
  MsgType rt; Fields rh; uint64_t plen = 0;
  Status hs = readMessageHead(c, rt, rh, plen);
  if (!isOk(hs)) last = OpResult{hs, {}, "no reply"};
  else {
    Status st = static_cast<Status>(rh.getI64("status", static_cast<int64_t>(Status::Internal)));
    if (!isOk(st)) { discardPayload(c, plen); last = OpResult{st, rh, "get failed"}; }
    else if (rh.getOr("striped") == "1") {
      // What the file holds is an index of chunk files; fetch them and stitch them together.
      if (plen > kMaxIndexBytes) { discardPayload(c, plen); return OpResult{Status::Internal, {}, "index too large"}; }
      std::string index;
      Status ss = streamPayloadIn(c, plen, [&](const char* b, size_t n) { index.append(b, n); return true; });
      if (!isOk(ss)) last = OpResult{ss, {}, "download failed"};
      else return assembleStriped(index, localPath, quorum);
    }
    else {
      int fd = ::open(localPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd < 0) { discardPayload(c, plen); return OpResult{Status::IoError, {}, "open out"}; }
      Sha256Stream hasher;
      Status ss = streamPayloadIn(c, plen, [&](const char* b, size_t n) {
        hasher.update(b, n);
        const char* p = b; size_t left = n;
        while (left) { ssize_t w = ::write(fd, p, left); if (w < 0) return false; p += w; left -= (size_t)w; }
        return true;
      });
      ::fsync(fd); ::close(fd);
      if (!isOk(ss)) last = OpResult{ss, {}, "download failed"};
      else { last = OpResult{Status::Ok, rh, ""}; last.info.set("sha256", toHex(hasher.finalize())); return last; }
    }
  }
  }
  if (!retryable(last.status) || attempt + 1 == attempts) return last;
  std::this_thread::sleep_for(std::chrono::milliseconds(retry.backoffMs * (attempt + 1)));
  }
  return last;
}

OpResult Client::assembleStriped(const std::string& index, const std::string& localPath,
                                 bool quorum) {
  std::vector<std::pair<std::string, uint64_t>> chunks;
  if (!parseIndex(index, chunks)) return OpResult{Status::Internal, {}, "bad stripe index"};
  const std::string base = tempPath("part");
  const uint64_t count = chunks.size();
  std::vector<OpResult> results(count);
  std::atomic<uint64_t> next{0};
  std::vector<std::thread> workers;
  const int n = static_cast<int>(std::min<uint64_t>(count, kStripeParallelism));
  for (int w = 0; w < n; ++w) {
    workers.emplace_back([&, w] {
      Client sub(entry, clientIdValue + "-g" + std::to_string(w));
      sub.retry = retry;
      sub.stripeBytes = 0;
      for (uint64_t i = next.fetch_add(1); i < count; i = next.fetch_add(1)) {
        results[i] = sub.getImpl(chunks[i].first, base + "." + std::to_string(i), quorum);
      }
    });
  }
  for (auto& t : workers) t.join();

  OpResult failure{Status::Ok, {}, ""};
  for (uint64_t i = 0; i < count && failure.ok(); ++i) {
    uint64_t got = 0;
    if (!results[i].ok()) failure = results[i];
    else if (!fileSize(base + "." + std::to_string(i), got) || got != chunks[i].second)
      failure = OpResult{Status::ChecksumMismatch, {}, "chunk size differs from the index"};
  }
  std::string tmpOut = localPath + ".partial";
  Sha256Stream hasher;
  if (failure.ok()) {
    int out = ::open(tmpOut.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out < 0) failure = OpResult{Status::IoError, {}, "open out"};
    std::vector<char> buf(kStreamChunkBytes);
    for (uint64_t i = 0; i < count && failure.ok(); ++i) {
      int in = ::open((base + "." + std::to_string(i)).c_str(), O_RDONLY);
      if (in < 0) { failure = OpResult{Status::IoError, {}, "open chunk"}; break; }
      ssize_t r;
      while ((r = ::read(in, buf.data(), buf.size())) > 0) {
        hasher.update(buf.data(), static_cast<size_t>(r));
        const char* p = buf.data(); size_t left = static_cast<size_t>(r);
        while (left) {
          ssize_t w = ::write(out, p, left);
          if (w < 0) { failure = OpResult{Status::IoError, {}, "write out"}; break; }
          p += w; left -= static_cast<size_t>(w);
        }
        if (!failure.ok()) break;
      }
      ::close(in);
    }
    if (out >= 0) { ::fsync(out); ::close(out); }
  }
  std::error_code ec;
  for (uint64_t i = 0; i < count; ++i) std::filesystem::remove(base + "." + std::to_string(i), ec);
  if (!failure.ok()) { std::filesystem::remove(tmpOut, ec); return failure; }
  if (::rename(tmpOut.c_str(), localPath.c_str()) != 0) {
    std::filesystem::remove(tmpOut, ec);
    return OpResult{Status::IoError, {}, "rename failed"};
  }
  OpResult ok{Status::Ok, {}, ""};
  ok.info.set("sha256", toHex(hasher.finalize()));
  ok.info.setU64("stripes", count);
  return ok;
}

OpResult Client::getFromReplica(const std::string& replicaAddr, const std::string& name,
                                  const std::string& localPath) {
  std::string host; int port;
  if (!parseEndpoint(replicaAddr, host, port)) return OpResult{Status::InvalidArgument, {}, "bad addr"};
  Conn c = Conn::dial(host, port, 30000);
  if (!c.valid()) return dialFail();
  Fields h;
  h.set("file", name);
  Status ws = writeFrame(c, MsgType::GetFromReplica, h, "");
  if (!isOk(ws)) return OpResult{ws, {}, "send failed"};
  MsgType rt; Fields rh; uint64_t plen = 0;
  Status hs = readMessageHead(c, rt, rh, plen);
  if (!isOk(hs)) return OpResult{hs, {}, "no reply"};
  Status st = static_cast<Status>(rh.getI64("status", static_cast<int64_t>(Status::Internal)));
  if (!isOk(st)) { discardPayload(c, plen); return OpResult{st, rh, "not found on replica"}; }
  int fd = ::open(localPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) { discardPayload(c, plen); return OpResult{Status::IoError, {}, "open out"}; }
  Sha256Stream hasher;
  Status ss = streamPayloadIn(c, plen, [&](const char* b, size_t n) {
    hasher.update(b, n);
    const char* p = b; size_t left = n;
    while (left) { ssize_t w = ::write(fd, p, left); if (w < 0) return false; p += w; left -= (size_t)w; }
    return true;
  });
  ::fsync(fd);
  ::close(fd);
  if (!isOk(ss)) return OpResult{ss, {}, "download failed"};
  OpResult res{Status::Ok, rh, ""};
  res.info.set("sha256", toHex(hasher.finalize()));
  return res;
}

static OpResult simpleRequest(const std::string& addr, MsgType t, const Fields& req,
                               const Client::RetryPolicy& policy) {
  OpResult last = dialFail();
  int attempts = std::max(1, policy.maxAttempts);
  for (int attempt = 0; attempt < attempts; ++attempt) {
  std::string host; int port;
  if (!parseEndpoint(addr, host, port)) return OpResult{Status::InvalidArgument, {}, "bad addr"};
  Conn c = Conn::dial(host, port, 10000);
  if (!c.valid()) last = dialFail();
  else {
    c.setRecvTimeout(kReplyTimeoutMs);
    Status ws = writeFrame(c, t, req, "");
    if (!isOk(ws)) last = OpResult{ws, {}, "send failed"};
    else {
      Frame reply;
      Status rs = readFrame(c, reply);
      if (!isOk(rs)) last = OpResult{rs, {}, "no reply"};
      else {
        last.status = static_cast<Status>(reply.header.getI64("status", static_cast<int64_t>(Status::Internal)));
        last.info = reply.header; last.message = reply.payload;
      }
    }
  }
  if (!((last.status == Status::NetworkError || last.status == Status::Timeout ||
         last.status == Status::Unavailable || last.status == Status::Stale) &&
        attempt + 1 < attempts)) return last;
  std::this_thread::sleep_for(std::chrono::milliseconds(policy.backoffMs * (attempt + 1)));
  }
  return last;
}

OpResult Client::merge(const std::string& name) {
  Fields req; req.set("file", name);
  return simpleRequest(entry, MsgType::Merge, req, retry);
}
OpResult Client::ls(const std::string& name) {
  Fields req; req.set("file", name);
  return simpleRequest(entry, MsgType::Ls, req, retry);
}
OpResult Client::listStore() { return simpleRequest(entry, MsgType::ListStore, {}, retry); }
OpResult Client::listMemIds() { return simpleRequest(entry, MsgType::ListMemIds, {}, retry); }
OpResult Client::status() { return simpleRequest(entry, MsgType::StatusQuery, {}, retry); }
OpResult Client::health() { return simpleRequest(entry, MsgType::Health, {}, retry); }
OpResult Client::leave() { return simpleRequest(entry, MsgType::Leave, {}, retry); }
OpResult Client::metrics() { return simpleRequest(entry, MsgType::Metrics, {}, retry); }

uint64_t Client::watermark(const std::string& name) const {
  auto it = watermarks.find(name);
  return it == watermarks.end() ? 0 : it->second;
}

}  // namespace aegisfs
