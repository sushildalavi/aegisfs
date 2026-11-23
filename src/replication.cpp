#include "aegisfs/replication.hpp"

#include <cstring>

namespace aegisfs::repl {

void segToFields(const SegmentRef& s, Fields& f) {
  f.setI64("kind", static_cast<int64_t>(s.kind));
  f.set("opId", s.opId);
  f.set("clientId", s.clientId);
  f.setU64("clientSeq", s.clientSeq);
  f.setU64("lamport", s.lamportTs);
  f.setU64("size", s.size);
  f.set("sha", toHex(s.sha));
  f.set("disk", s.diskName);
}

bool segFromFields(const Fields& f, SegmentRef& s) {
  int64_t kind = f.getI64("kind", 1);
  if (kind != static_cast<int64_t>(SegKind::Base) &&
      kind != static_cast<int64_t>(SegKind::Append)) {
    return false;
  }
  s.kind = static_cast<SegKind>(kind);
  s.opId = f.getOr("opId");
  s.clientId = f.getOr("clientId");
  s.clientSeq = f.getU64("clientSeq");
  s.lamportTs = f.getU64("lamport");
  s.size = f.getU64("size");
  s.diskName = f.getOr("disk");
  if (s.opId.empty() || s.diskName.empty()) return false;
  return fromHex(f.getOr("sha"), s.sha);
}

Status sendReply(Conn& c, Status st, const Fields& extra) {
  Fields f = extra;
  f.setI64("status", static_cast<int64_t>(st));
  return writeFrame(c, MsgType::Reply, f, "");
}

Status readReply(Conn& c, Fields& out) {
  Frame fr;
  Status s = readFrame(c, fr);
  if (!isOk(s)) return s;
  out = fr.header;
  return static_cast<Status>(out.getI64("status", static_cast<int64_t>(Status::Internal)));
}

Status fetchManifest(const std::string& addr, const std::string& fileId, Manifest& out,
                      int timeoutMs) {
  std::string host; int port;
  if (!parseEndpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeoutMs);
  if (!c.valid()) return Status::NetworkError;
  Fields req;
  req.set("fileId", fileId);
  Status s = writeFrame(c, MsgType::FetchManifest, req, "");
  if (!isOk(s)) return s;
  Frame fr;
  s = readFrame(c, fr);
  if (!isOk(s)) return s;
  Status rst = static_cast<Status>(fr.header.getI64("status", static_cast<int64_t>(Status::Internal)));
  if (!isOk(rst)) return rst;
  auto m = Manifest::parse(reinterpret_cast<const uint8_t*>(fr.payload.data()), fr.payload.size());
  if (!m) return Status::Internal;
  out = std::move(*m);
  return Status::Ok;
}

Status replicateSegment(const std::string& addr, const std::string& fileId,
                         const std::string& filename, const std::string& createOpId,
                         const SegmentRef& seg, uint64_t payloadLen,
                         const std::function<size_t(char*, size_t)>& source, bool isBase,
                         int timeoutMs) {
  std::string host; int port;
  if (!parseEndpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeoutMs);
  if (!c.valid()) return Status::NetworkError;
  Fields f;
  f.set("fileId", fileId);
  f.set("filename", filename);
  f.set("createOpId", createOpId);
  segToFields(seg, f);
  MsgType t = isBase ? MsgType::ReplicatePut : MsgType::ReplicateAppend;
  Status s = writeFrameStreaming(c, t, f, payloadLen, source);
  if (!isOk(s)) return s;
  Fields reply;
  return readReply(c, reply);
}

Status fetchSegment(const std::string& addr, const std::string& fileId,
                     const std::string& diskName,
                     const std::function<bool(const char*, size_t)>& sink, int timeoutMs) {
  std::string host; int port;
  if (!parseEndpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeoutMs);
  if (!c.valid()) return Status::NetworkError;
  Fields req;
  req.set("fileId", fileId);
  req.set("disk", diskName);
  Status s = writeFrame(c, MsgType::FetchSegment, req, "");
  if (!isOk(s)) return s;
  MsgType t; Fields h; uint64_t plen = 0;
  // Server streams: first a tiny status-only response if not found, else the bytes.
  s = readFrameStreaming(c, t, h, plen, sink);
  if (!isOk(s)) return s;
  return static_cast<Status>(h.getI64("status", static_cast<int64_t>(Status::Ok)));
}

Status getFromReplica(const std::string& addr, const std::string& filename,
                        const std::function<bool(const char*, size_t)>& sink,
                        Sha256Digest& outSha, int timeoutMs) {
  std::string host; int port;
  if (!parseEndpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, timeoutMs);
  if (!c.valid()) return Status::NetworkError;
  Fields req;
  req.set("file", filename);
  Status s = writeFrame(c, MsgType::GetFromReplica, req, "");
  if (!isOk(s)) return s;
  MsgType t; Fields h; uint64_t plen = 0;
  s = readFrameStreaming(c, t, h, plen, sink);
  if (!isOk(s)) return s;
  Status rst = static_cast<Status>(h.getI64("status", static_cast<int64_t>(Status::Ok)));
  fromHex(h.getOr("fileSha"), outSha);
  return rst;
}

}  // namespace aegisfs::repl
