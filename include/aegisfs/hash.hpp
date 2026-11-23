#pragma once
#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace aegisfs {

// SHA-256 digest (32 bytes).
using Sha256Digest = std::array<uint8_t, 32>;

// Compute SHA-256 over a byte range.
Sha256Digest sha256(const void* data, size_t len);
Sha256Digest sha256(std::string_view s);

// Lowercase hex of a digest (64 chars).
std::string toHex(const Sha256Digest& d);

// Parse 64-char lowercase hex into a digest. Returns false on malformed input.
bool fromHex(std::string_view hex, Sha256Digest& out);

// Stable 64-bit ring identifier: the top 64 bits (big-endian) of SHA-256(key).
// Deterministic across processes and runs; never uses std::hash.
uint64_t ringId(std::string_view key);

// Incremental SHA-256 for streaming large files without buffering them.
class Sha256Stream {
 public:
  Sha256Stream();
  ~Sha256Stream();
  Sha256Stream(const Sha256Stream&) = delete;
  Sha256Stream& operator=(const Sha256Stream&) = delete;
  void update(const void* data, size_t len);
  Sha256Digest finalize();

 private:
  void* context;  // opaque EVP_MD_CTX*
};

}  // namespace aegisfs
