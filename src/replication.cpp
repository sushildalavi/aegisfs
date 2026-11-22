#include "aegisfs/replication.hpp"

#include <cstring>

namespace aegisfs::repl {

void seg_to_fields(const SegmentRef& s, Fields& f) {
  f.set_i64("kind", static_cast<int64_t>(s.kind));
  f.set("op_id", s.op_id);
  f.set("client_id", s.client_id);
  f.set_u64("client_seq", s.client_seq);
  f.set_u64("lamport", s.lamport_ts);
  f.set_u64("size", s.size);
  f.set("sha", to_hex(s.sha));
  f.set("disk", s.disk_name);
}

bool seg_from_fields(const Fields& f, SegmentRef& s) {
  s.kind = static_cast<SegKind>(f.get_i64("kind", 1));
  s.op_id = f.get_or("op_id");
  s.client_id = f.get_or("client_id");
  s.client_seq = f.get_u64("client_seq");
  s.lamport_ts = f.get_u64("lamport");
  s.size = f.get_u64("size");
  s.disk_name = f.get_or("disk");
  if (s.op_id.empty() || s.disk_name.empty()) return false;
  return from_hex(f.get_or("sha"), s.sha);
}

Status send_reply(Conn& c, Status st, const Fields& extra) {
  Fields f = extra;
  f.set_i64("status", static_cast<int64_t>(st));
  return write_frame(c, MsgType::Reply, f, "");
}

Status read_reply(Conn& c, Fields& out) {
  Frame fr;
  Status s = read_frame(c, fr);
  if (!is_ok(s)) return s;
  out = fr.header;
  return static_cast<Status>(out.get_i64("status", static_cast<int64_t>(Status::Internal)));
}

Status fetch_manifest(const std::string& addr, const std::string& file_id, Manifest& out,
                      int timeout_ms) {
  std::string host; int port;
  if (!parse_endpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeout_ms);
  if (!c.valid()) return Status::NetworkError;
  Fields req;
  req.set("file_id", file_id);
  Status s = write_frame(c, MsgType::FetchManifest, req, "");
  if (!is_ok(s)) return s;
  Frame fr;
  s = read_frame(c, fr);
  if (!is_ok(s)) return s;
  Status rst = static_cast<Status>(fr.header.get_i64("status", static_cast<int64_t>(Status::Internal)));
  if (!is_ok(rst)) return rst;
  auto m = Manifest::parse(reinterpret_cast<const uint8_t*>(fr.payload.data()), fr.payload.size());
  if (!m) return Status::Internal;
  out = std::move(*m);
  return Status::Ok;
}

Status replicate_segment(const std::string& addr, const std::string& file_id,
                         const std::string& filename, const std::string& create_op_id,
                         const SegmentRef& seg, uint64_t payload_len,
                         const std::function<size_t(char*, size_t)>& source, bool is_base,
                         int timeout_ms) {
  std::string host; int port;
  if (!parse_endpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeout_ms);
  if (!c.valid()) return Status::NetworkError;
  Fields f;
  f.set("file_id", file_id);
  f.set("filename", filename);
  f.set("create_op_id", create_op_id);
  seg_to_fields(seg, f);
  MsgType t = is_base ? MsgType::ReplicatePut : MsgType::ReplicateAppend;
  Status s = write_frame_streaming(c, t, f, payload_len, source);
  if (!is_ok(s)) return s;
  Fields reply;
  return read_reply(c, reply);
}

Status fetch_segment(const std::string& addr, const std::string& file_id,
                     const std::string& disk_name,
                     const std::function<bool(const char*, size_t)>& sink, int timeout_ms) {
  std::string host; int port;
  if (!parse_endpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeout_ms);
  if (!c.valid()) return Status::NetworkError;
  Fields req;
  req.set("file_id", file_id);
  req.set("disk", disk_name);
  Status s = write_frame(c, MsgType::FetchSegment, req, "");
  if (!is_ok(s)) return s;
  MsgType t; Fields h; uint64_t plen = 0;
  // Server streams: first a tiny status-only response if not found, else the bytes.
  s = read_frame_streaming(c, t, h, plen, sink);
  if (!is_ok(s)) return s;
  return static_cast<Status>(h.get_i64("status", static_cast<int64_t>(Status::Ok)));
}

Status get_from_replica(const std::string& addr, const std::string& filename,
                        const std::function<bool(const char*, size_t)>& sink,
                        Sha256Digest& out_sha, int timeout_ms) {
  std::string host; int port;
  if (!parse_endpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeout_ms);
  if (!c.valid()) return Status::NetworkError;
  Fields req;
  req.set("file", filename);
  Status s = write_frame(c, MsgType::GetFromReplica, req, "");
  if (!is_ok(s)) return s;
  MsgType t; Fields h; uint64_t plen = 0;
  s = read_frame_streaming(c, t, h, plen, sink);
  if (!is_ok(s)) return s;
  Status rst = static_cast<Status>(h.get_i64("status", static_cast<int64_t>(Status::Ok)));
  from_hex(h.get_or("file_sha"), out_sha);
  return rst;
}

}  // namespace aegisfs::repl
