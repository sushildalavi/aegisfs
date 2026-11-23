#include "aegisfs/client.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <algorithm>
#include <thread>
#include <chrono>

#include "aegisfs/hash.hpp"
#include "aegisfs/net.hpp"

namespace aegisfs {

Client::Client(std::string entryAddr, std::string clientId)
    : entry(std::move(entryAddr)), clientIdValue(std::move(clientId)) {}

static bool file_size(const std::string& path, uint64_t& out) {
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

OpResult Client::upload(MsgType t, const std::string& localPath, const std::string& name) {
  uint64_t sz = 0;
  if (!file_size(localPath, sz)) return OpResult{Status::NotFound, {}, "local file not found"};
  if (retry.maxAttempts < 1) retry.maxAttempts = 1;
  // A stable operation id makes retries safe even after the first replica committed.
  const std::string opId = clientIdValue + "#op#" + std::to_string(++opCounter);
  OpResult last = dialFail();
  for (int attempt = 0; attempt < retry.maxAttempts; ++attempt) {
    std::string host; int port;
    if (!parseEndpoint(entry, host, port)) return OpResult{Status::InvalidArgument, {}, "bad entry"};
    Conn c = Conn::dial(host, port, 10000);
    if (!c.valid()) last = dialFail();
    else {
      int fd = ::open(localPath.c_str(), O_RDONLY);
      if (fd < 0) return OpResult{Status::IoError, {}, "open failed"};
      Fields h;
      h.set("file", name);
      h.set("clientId", clientIdValue);
      h.set("opId", opId);
      Status ws = writeFrameStreaming(c, t, h, sz, [&](char* b, size_t cap) -> size_t {
        ssize_t n = ::read(fd, b, cap);
        return n > 0 ? static_cast<size_t>(n) : 0;
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

OpResult Client::create(const std::string& localPath, const std::string& name) {
  return upload(MsgType::Create, localPath, name);
}

OpResult Client::append(const std::string& localPath, const std::string& name) {
  OpResult r = upload(MsgType::Append, localPath, name);
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
