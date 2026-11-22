#include "aegisfs/hash.hpp"

#include <openssl/evp.h>
#include <openssl/sha.h>

#include <cstdio>
#include <stdexcept>

namespace aegisfs {

Sha256Digest sha256(const void* data, size_t len) {
  Sha256Digest out{};
  SHA256(static_cast<const unsigned char*>(data), len, out.data());
  return out;
}

Sha256Digest sha256(std::string_view s) { return sha256(s.data(), s.size()); }

std::string to_hex(const Sha256Digest& d) {
  static const char* kHex = "0123456789abcdef";
  std::string out;
  out.resize(d.size() * 2);
  for (size_t i = 0; i < d.size(); ++i) {
    out[2 * i] = kHex[(d[i] >> 4) & 0xF];
    out[2 * i + 1] = kHex[d[i] & 0xF];
  }
  return out;
}

static int hex_val(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

bool from_hex(std::string_view hex, Sha256Digest& out) {
  if (hex.size() != out.size() * 2) return false;
  for (size_t i = 0; i < out.size(); ++i) {
    int hi = hex_val(hex[2 * i]);
    int lo = hex_val(hex[2 * i + 1]);
    if (hi < 0 || lo < 0) return false;
    out[i] = static_cast<uint8_t>((hi << 4) | lo);
  }
  return true;
}

uint64_t ring_id(std::string_view key) {
  Sha256Digest d = sha256(key);
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | d[static_cast<size_t>(i)];
  }
  return v;
}

Sha256Stream::Sha256Stream() {
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  if (!ctx || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
    if (ctx) EVP_MD_CTX_free(ctx);
    throw std::runtime_error("failed to init SHA-256 context");
  }
  ctx_ = ctx;
}

Sha256Stream::~Sha256Stream() {
  if (ctx_) EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(ctx_));
}

void Sha256Stream::update(const void* data, size_t len) {
  if (len == 0) return;
  EVP_DigestUpdate(static_cast<EVP_MD_CTX*>(ctx_), data, len);
}

Sha256Digest Sha256Stream::finalize() {
  Sha256Digest out{};
  unsigned int n = 0;
  EVP_DigestFinal_ex(static_cast<EVP_MD_CTX*>(ctx_), out.data(), &n);
  return out;
}

}  // namespace aegisfs
