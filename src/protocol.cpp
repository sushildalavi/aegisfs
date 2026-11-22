#include "aegisfs/protocol.hpp"

#include <charconv>
#include <cstring>

namespace aegisfs {

void put_u16(std::string& b, uint16_t v) {
  b.push_back(static_cast<char>(v & 0xFF));
  b.push_back(static_cast<char>((v >> 8) & 0xFF));
}
void put_u32(std::string& b, uint32_t v) {
  for (int i = 0; i < 4; ++i) b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void put_u64(std::string& b, uint64_t v) {
  for (int i = 0; i < 8; ++i) b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
uint16_t get_u16(const uint8_t* p) {
  return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}
uint32_t get_u32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
  return v;
}
uint64_t get_u64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
  return v;
}

void Fields::set(const std::string& k, const std::string& v) {
  for (auto& kv : kv_) {
    if (kv.first == k) { kv.second = v; return; }
  }
  kv_.emplace_back(k, v);
}
void Fields::set_i64(const std::string& k, int64_t v) { set(k, std::to_string(v)); }
void Fields::set_u64(const std::string& k, uint64_t v) { set(k, std::to_string(v)); }

std::optional<std::string> Fields::get(const std::string& k) const {
  for (const auto& kv : kv_) {
    if (kv.first == k) return kv.second;
  }
  return std::nullopt;
}
std::string Fields::get_or(const std::string& k, const std::string& def) const {
  auto v = get(k);
  return v ? *v : def;
}
int64_t Fields::get_i64(const std::string& k, int64_t def) const {
  auto v = get(k);
  if (!v) return def;
  int64_t out = def;
  auto res = std::from_chars(v->data(), v->data() + v->size(), out);
  return res.ec == std::errc{} ? out : def;
}
uint64_t Fields::get_u64(const std::string& k, uint64_t def) const {
  auto v = get(k);
  if (!v) return def;
  uint64_t out = def;
  auto res = std::from_chars(v->data(), v->data() + v->size(), out);
  return res.ec == std::errc{} ? out : def;
}

void Fields::serialize(std::string& out) const {
  put_u32(out, static_cast<uint32_t>(kv_.size()));
  for (const auto& kv : kv_) {
    put_u32(out, static_cast<uint32_t>(kv.first.size()));
    out.append(kv.first);
    put_u32(out, static_cast<uint32_t>(kv.second.size()));
    out.append(kv.second);
  }
}

bool Fields::parse(const uint8_t* data, size_t len, Fields& out) {
  out.kv_.clear();
  size_t pos = 0;
  auto need = [&](size_t n) { return pos + n <= len; };
  if (!need(4)) return false;
  uint32_t count = get_u32(data + pos);
  pos += 4;
  if (count > 100000u) return false;  // sanity cap
  for (uint32_t i = 0; i < count; ++i) {
    if (!need(4)) return false;
    uint32_t klen = get_u32(data + pos); pos += 4;
    if (klen > kMaxHeaderBytes || !need(klen)) return false;
    std::string k(reinterpret_cast<const char*>(data + pos), klen); pos += klen;
    if (!need(4)) return false;
    uint32_t vlen = get_u32(data + pos); pos += 4;
    if (vlen > kMaxHeaderBytes || !need(vlen)) return false;
    std::string v(reinterpret_cast<const char*>(data + pos), vlen); pos += vlen;
    out.kv_.emplace_back(std::move(k), std::move(v));
  }
  return true;
}

}  // namespace aegisfs
