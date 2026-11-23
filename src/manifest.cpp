#include "aegisfs/manifest.hpp"

#include <algorithm>
#include <cstdio>

#include "aegisfs/protocol.hpp"  // reuse LE encoders

namespace aegisfs {

static bool validDiskName(const std::string& name) {
  if (name.empty() || name == "." || name == "..") return false;
  for (unsigned char c : name) {
    if (c < 0x20 || c == '/' || c == '\\') return false;
  }
  return true;
}

bool SegmentRef::orderBefore(const SegmentRef& o) const {
  // Base always first.
  if (kind != o.kind) return kind == SegKind::Base;
  if (lamportTs != o.lamportTs) return lamportTs < o.lamportTs;
  if (clientId != o.clientId) return clientId < o.clientId;
  if (clientSeq != o.clientSeq) return clientSeq < o.clientSeq;
  // The documented ordering fields should normally be unique, but malformed or
  // concurrently generated metadata can still collide.  Keep the comparator a
  // strict total order so every replica reconstructs the same byte sequence.
  return opId < o.opId;
}

std::string makeFileId(const std::string& filename) {
  uint64_t r = ringId(filename);
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(r));
  return std::string(buf);
}

bool Manifest::hasOp(const std::string& opId) const {
  for (const auto& s : segments) {
    if (s.opId == opId) return true;
  }
  return false;
}

bool Manifest::addSegment(const SegmentRef& seg) {
  if (hasOp(seg.opId)) return false;  // idempotent
  // A manifest must never reference one physical file from two operations or
  // contain two bases.  Otherwise a later immutable-segment write could
  // overwrite bytes that an earlier manifest entry still references.
  for (const auto& existing : segments) {
    if (existing.diskName == seg.diskName) return false;
    if (seg.kind == SegKind::Base && existing.kind == SegKind::Base) return false;
  }
  segments.push_back(seg);
  ++version;
  return true;
}

std::vector<const SegmentRef*> Manifest::ordered() const {
  std::vector<const SegmentRef*> v;
  v.reserve(segments.size());
  for (const auto& s : segments) v.push_back(&s);
  std::sort(v.begin(), v.end(),
            [](const SegmentRef* a, const SegmentRef* b) { return a->orderBefore(*b); });
  return v;
}

uint64_t Manifest::totalSize() const {
  uint64_t t = 0;
  for (const auto& s : segments) t += s.size;
  return t;
}

static void putStr(std::string& out, const std::string& s) {
  putU32(out, static_cast<uint32_t>(s.size()));
  out.append(s);
}

void Manifest::serialize(std::string& out) const {
  std::string body;
  putStr(body, filename);
  putStr(body, fileId);
  putStr(body, createOpId);
  putU64(body, version);
  putU32(body, static_cast<uint32_t>(segments.size()));
  for (const auto& s : segments) {
    body.push_back(static_cast<char>(s.kind));
    putStr(body, s.opId);
    putStr(body, s.clientId);
    putU64(body, s.clientSeq);
    putU64(body, s.lamportTs);
    putU64(body, s.size);
    body.append(reinterpret_cast<const char*>(s.sha.data()), s.sha.size());
    putStr(body, s.diskName);
  }
  // Frame: MAGIC, len, body, sha256(body) as trailing integrity check.
  putU32(out, 0x4D414E46u);  // "MANF"
  putU32(out, static_cast<uint32_t>(body.size()));
  out.append(body);
  Sha256Digest d = sha256(body.data(), body.size());
  out.append(reinterpret_cast<const char*>(d.data()), d.size());
}

std::optional<Manifest> Manifest::parse(const uint8_t* data, size_t len) {
  size_t pos = 0;
  auto need = [&](size_t n) { return pos + n <= len; };
  if (!need(8)) return std::nullopt;
  if (getU32(data) != 0x4D414E46u) return std::nullopt;
  uint32_t blen = getU32(data + 4);
  pos = 8;
  if (blen > kMaxHeaderBytes || !need(blen + 32) || pos + blen + 32 != len) {
    return std::nullopt;
  }
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
  auto rdStr = [&](std::string& s) -> bool {
    if (!bneed(4)) return false;
    uint32_t l = getU32(body + bp); bp += 4;
    if (!bneed(l)) return false;
    s.assign(reinterpret_cast<const char*>(body + bp), l); bp += l;
    return true;
  };
  auto rdU64 = [&](uint64_t& v) -> bool {
    if (!bneed(8)) return false;
    v = getU64(body + bp); bp += 8; return true;
  };
  Manifest m;
  if (!rdStr(m.filename) || !rdStr(m.fileId) || !rdStr(m.createOpId)) return std::nullopt;
  if (!rdU64(m.version)) return std::nullopt;
  if (!bneed(4)) return std::nullopt;
  uint32_t nseg = getU32(body + bp); bp += 4;
  if (nseg > 10'000'000u) return std::nullopt;
  for (uint32_t i = 0; i < nseg; ++i) {
    SegmentRef s;
    if (!bneed(1)) return std::nullopt;
    s.kind = static_cast<SegKind>(body[bp]); bp += 1;
    if (s.kind != SegKind::Base && s.kind != SegKind::Append) return std::nullopt;
    if (!rdStr(s.opId) || !rdStr(s.clientId)) return std::nullopt;
    if (!rdU64(s.clientSeq) || !rdU64(s.lamportTs) || !rdU64(s.size)) return std::nullopt;
    if (!bneed(32)) return std::nullopt;
    std::copy(body + bp, body + bp + 32, s.sha.begin()); bp += 32;
    if (!rdStr(s.diskName)) return std::nullopt;
    if (s.opId.empty() || !validDiskName(s.diskName)) return std::nullopt;
    if (m.hasOp(s.opId)) return std::nullopt;
    for (const auto& existing : m.segments) {
      if (existing.diskName == s.diskName) return std::nullopt;
      if (s.kind == SegKind::Base && existing.kind == SegKind::Base) return std::nullopt;
    }
    m.segments.push_back(std::move(s));
  }
  if (bp != blen) return std::nullopt;
  return m;
}

}  // namespace aegisfs
