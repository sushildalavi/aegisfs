#include "aegisfs/manifest.hpp"

#include <algorithm>
#include <cstdio>

#include "aegisfs/protocol.hpp"  // reuse LE encoders

namespace aegisfs {

bool SegmentRef::order_before(const SegmentRef& o) const {
  // Base always first.
  if (kind != o.kind) return kind == SegKind::Base;
  if (lamport_ts != o.lamport_ts) return lamport_ts < o.lamport_ts;
  if (client_id != o.client_id) return client_id < o.client_id;
  return client_seq < o.client_seq;
}

std::string make_file_id(const std::string& filename) {
  uint64_t r = ring_id(filename);
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(r));
  return std::string(buf);
}

bool Manifest::has_op(const std::string& op_id) const {
  for (const auto& s : segments) {
    if (s.op_id == op_id) return true;
  }
  return false;
}

bool Manifest::add_segment(const SegmentRef& seg) {
  if (has_op(seg.op_id)) return false;  // idempotent
  segments.push_back(seg);
  ++version;
  return true;
}

std::vector<const SegmentRef*> Manifest::ordered() const {
  std::vector<const SegmentRef*> v;
  v.reserve(segments.size());
  for (const auto& s : segments) v.push_back(&s);
  std::sort(v.begin(), v.end(),
            [](const SegmentRef* a, const SegmentRef* b) { return a->order_before(*b); });
  return v;
}

uint64_t Manifest::total_size() const {
  uint64_t t = 0;
  for (const auto& s : segments) t += s.size;
  return t;
}

static void put_str(std::string& out, const std::string& s) {
  put_u32(out, static_cast<uint32_t>(s.size()));
  out.append(s);
}

void Manifest::serialize(std::string& out) const {
  std::string body;
  put_str(body, filename);
  put_str(body, file_id);
  put_str(body, create_op_id);
  put_u64(body, version);
  put_u32(body, static_cast<uint32_t>(segments.size()));
  for (const auto& s : segments) {
    body.push_back(static_cast<char>(s.kind));
    put_str(body, s.op_id);
    put_str(body, s.client_id);
    put_u64(body, s.client_seq);
    put_u64(body, s.lamport_ts);
    put_u64(body, s.size);
    body.append(reinterpret_cast<const char*>(s.sha.data()), s.sha.size());
    put_str(body, s.disk_name);
  }
  // Frame: MAGIC, len, body, sha256(body) as trailing integrity check.
  put_u32(out, 0x4D414E46u);  // "MANF"
  put_u32(out, static_cast<uint32_t>(body.size()));
  out.append(body);
  Sha256Digest d = sha256(body.data(), body.size());
  out.append(reinterpret_cast<const char*>(d.data()), d.size());
}

std::optional<Manifest> Manifest::parse(const uint8_t* data, size_t len) {
  size_t pos = 0;
  auto need = [&](size_t n) { return pos + n <= len; };
  if (!need(8)) return std::nullopt;
  if (get_u32(data) != 0x4D414E46u) return std::nullopt;
  uint32_t blen = get_u32(data + 4);
  pos = 8;
  if (!need(blen + 32)) return std::nullopt;
  const uint8_t* body = data + pos;
  // verify trailing checksum
  Sha256Digest want = sha256(body, blen);
  const uint8_t* trailer = body + blen;
  for (int i = 0; i < 32; ++i) {
    if (trailer[i] != want[static_cast<size_t>(i)]) return std::nullopt;  // corrupt
  }
  // parse body
  size_t bp = 0;
  auto bneed = [&](size_t n) { return bp + n <= blen; };
  auto rd_str = [&](std::string& s) -> bool {
    if (!bneed(4)) return false;
    uint32_t l = get_u32(body + bp); bp += 4;
    if (!bneed(l)) return false;
    s.assign(reinterpret_cast<const char*>(body + bp), l); bp += l;
    return true;
  };
  auto rd_u64 = [&](uint64_t& v) -> bool {
    if (!bneed(8)) return false;
    v = get_u64(body + bp); bp += 8; return true;
  };
  Manifest m;
  if (!rd_str(m.filename) || !rd_str(m.file_id) || !rd_str(m.create_op_id)) return std::nullopt;
  if (!rd_u64(m.version)) return std::nullopt;
  if (!bneed(4)) return std::nullopt;
  uint32_t nseg = get_u32(body + bp); bp += 4;
  if (nseg > 10'000'000u) return std::nullopt;
  for (uint32_t i = 0; i < nseg; ++i) {
    SegmentRef s;
    if (!bneed(1)) return std::nullopt;
    s.kind = static_cast<SegKind>(body[bp]); bp += 1;
    if (!rd_str(s.op_id) || !rd_str(s.client_id)) return std::nullopt;
    if (!rd_u64(s.client_seq) || !rd_u64(s.lamport_ts) || !rd_u64(s.size)) return std::nullopt;
    if (!bneed(32)) return std::nullopt;
    std::copy(body + bp, body + bp + 32, s.sha.begin()); bp += 32;
    if (!rd_str(s.disk_name)) return std::nullopt;
    m.segments.push_back(std::move(s));
  }
  return m;
}

}  // namespace aegisfs
