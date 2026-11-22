#include "aegisfs/client.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>

#include "aegisfs/hash.hpp"
#include "aegisfs/net.hpp"

namespace aegisfs {

Client::Client(std::string entry_addr, std::string client_id)
    : entry_(std::move(entry_addr)), client_id_(std::move(client_id)) {}

static bool file_size(const std::string& path, uint64_t& out) {
  struct stat st{};
  if (::stat(path.c_str(), &st) != 0) return false;
  out = static_cast<uint64_t>(st.st_size);
  return true;
}

static OpResult dial_fail() { return OpResult{Status::NetworkError, {}, "cannot reach entry node"}; }

OpResult Client::upload(MsgType t, const std::string& local_path, const std::string& name) {
  uint64_t sz = 0;
  if (!file_size(local_path, sz)) return OpResult{Status::NotFound, {}, "local file not found"};
  std::string host; int port;
  if (!parse_endpoint(entry_, host, port)) return OpResult{Status::InvalidArgument, {}, "bad entry"};
  Conn c = Conn::dial(host, port, 10000);
  if (!c.valid()) return dial_fail();
  int fd = ::open(local_path.c_str(), O_RDONLY);
  if (fd < 0) return OpResult{Status::IoError, {}, "open failed"};
  Fields h;
  h.set("file", name);
  h.set("client_id", client_id_);
  Status ws = write_frame_streaming(c, t, h, sz, [&](char* b, size_t cap) -> size_t {
    ssize_t n = ::read(fd, b, cap);
    return n > 0 ? static_cast<size_t>(n) : 0;
  });
  ::close(fd);
  if (!is_ok(ws)) return OpResult{ws, {}, "upload failed"};
  Frame reply;
  Status rs = read_frame(c, reply);
  if (!is_ok(rs)) return OpResult{rs, {}, "no reply"};
  OpResult res;
  res.status = static_cast<Status>(reply.header.get_i64("status", static_cast<int64_t>(Status::Internal)));
  res.info = reply.header;
  return res;
}

OpResult Client::create(const std::string& local_path, const std::string& name) {
  return upload(MsgType::Create, local_path, name);
}

OpResult Client::append(const std::string& local_path, const std::string& name) {
  OpResult r = upload(MsgType::Append, local_path, name);
  if (r.ok() || r.status == Status::Degraded) {
    uint64_t seq = r.info.get_u64("client_seq");
    if (seq > watermark_[name]) watermark_[name] = seq;
  }
  return r;
}

OpResult Client::get(const std::string& name, const std::string& local_path) {
  std::string host; int port;
  if (!parse_endpoint(entry_, host, port)) return OpResult{Status::InvalidArgument, {}, "bad entry"};
  Conn c = Conn::dial(host, port, 30000);
  if (!c.valid()) return dial_fail();
  Fields h;
  h.set("file", name);
  h.set("client_id", client_id_);
  h.set_u64("required_seq", watermark_.count(name) ? watermark_[name] : 0);
  Status ws = write_frame(c, MsgType::Get, h, "");
  if (!is_ok(ws)) return OpResult{ws, {}, "send failed"};
  MsgType rt; Fields rh; uint64_t plen = 0;
  Status hs = read_message_head(c, rt, rh, plen);
  if (!is_ok(hs)) return OpResult{hs, {}, "no reply"};
  Status st = static_cast<Status>(rh.get_i64("status", static_cast<int64_t>(Status::Internal)));
  if (!is_ok(st)) { discard_payload(c, plen); return OpResult{st, rh, "get failed"}; }
  int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) { discard_payload(c, plen); return OpResult{Status::IoError, {}, "open out"}; }
  Sha256Stream hasher;
  Status ss = stream_payload_in(c, plen, [&](const char* b, size_t n) {
    hasher.update(b, n);
    const char* p = b; size_t left = n;
    while (left) { ssize_t w = ::write(fd, p, left); if (w < 0) return false; p += w; left -= (size_t)w; }
    return true;
  });
  ::fsync(fd);
  ::close(fd);
  if (!is_ok(ss)) return OpResult{ss, {}, "download failed"};
  OpResult res{Status::Ok, rh, ""};
  res.info.set("sha256", to_hex(hasher.finalize()));
  return res;
}

OpResult Client::get_from_replica(const std::string& replica_addr, const std::string& name,
                                  const std::string& local_path) {
  std::string host; int port;
  if (!parse_endpoint(replica_addr, host, port)) return OpResult{Status::InvalidArgument, {}, "bad addr"};
  Conn c = Conn::dial(host, port, 30000);
  if (!c.valid()) return dial_fail();
  Fields h;
  h.set("file", name);
  Status ws = write_frame(c, MsgType::GetFromReplica, h, "");
  if (!is_ok(ws)) return OpResult{ws, {}, "send failed"};
  MsgType rt; Fields rh; uint64_t plen = 0;
  Status hs = read_message_head(c, rt, rh, plen);
  if (!is_ok(hs)) return OpResult{hs, {}, "no reply"};
  Status st = static_cast<Status>(rh.get_i64("status", static_cast<int64_t>(Status::Internal)));
  if (!is_ok(st)) { discard_payload(c, plen); return OpResult{st, rh, "not found on replica"}; }
  int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) { discard_payload(c, plen); return OpResult{Status::IoError, {}, "open out"}; }
  Sha256Stream hasher;
  Status ss = stream_payload_in(c, plen, [&](const char* b, size_t n) {
    hasher.update(b, n);
    const char* p = b; size_t left = n;
    while (left) { ssize_t w = ::write(fd, p, left); if (w < 0) return false; p += w; left -= (size_t)w; }
    return true;
  });
  ::fsync(fd);
  ::close(fd);
  if (!is_ok(ss)) return OpResult{ss, {}, "download failed"};
  OpResult res{Status::Ok, rh, ""};
  res.info.set("sha256", to_hex(hasher.finalize()));
  return res;
}

static OpResult simple_request(const std::string& addr, MsgType t, const Fields& req) {
  std::string host; int port;
  if (!parse_endpoint(addr, host, port)) return OpResult{Status::InvalidArgument, {}, "bad addr"};
  Conn c = Conn::dial(host, port, 10000);
  if (!c.valid()) return dial_fail();
  Status ws = write_frame(c, t, req, "");
  if (!is_ok(ws)) return OpResult{ws, {}, "send failed"};
  Frame reply;
  Status rs = read_frame(c, reply);
  if (!is_ok(rs)) return OpResult{rs, {}, "no reply"};
  OpResult res;
  res.status = static_cast<Status>(reply.header.get_i64("status", static_cast<int64_t>(Status::Internal)));
  res.info = reply.header;
  res.message = reply.payload;  // some replies (membership) carry payload
  return res;
}

OpResult Client::merge(const std::string& name) {
  Fields req; req.set("file", name);
  return simple_request(entry_, MsgType::Merge, req);
}
OpResult Client::ls(const std::string& name) {
  Fields req; req.set("file", name);
  return simple_request(entry_, MsgType::Ls, req);
}
OpResult Client::liststore() { return simple_request(entry_, MsgType::ListStore, {}); }
OpResult Client::list_mem_ids() { return simple_request(entry_, MsgType::ListMemIds, {}); }
OpResult Client::status() { return simple_request(entry_, MsgType::Status_, {}); }
OpResult Client::health() { return simple_request(entry_, MsgType::Health, {}); }
OpResult Client::leave() { return simple_request(entry_, MsgType::Leave, {}); }
OpResult Client::metrics() { return simple_request(entry_, MsgType::Metrics, {}); }

uint64_t Client::watermark(const std::string& name) const {
  auto it = watermark_.find(name);
  return it == watermark_.end() ? 0 : it->second;
}

}  // namespace aegisfs
