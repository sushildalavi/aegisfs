#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "aegisfs/common.hpp"

namespace aegisfs {

// RPC message types (wire-stable, uint16).
enum class MsgType : uint16_t {
  Ping = 1,
  Pong = 2,
  Membership = 3,       // gossip push of member table
  Create = 10,
  Get = 11,
  Append = 12,
  Merge = 13,
  Ls = 14,
  ListStore = 15,
  GetFromReplica = 16,
  ListMemIds = 17,
  Status_ = 18,
  Leave = 19,
  Health = 20,
  Metrics = 21,
  // replica-internal
  ReplicatePut = 30,    // store a full base segment
  ReplicateAppend = 31, // store an append segment
  FetchManifest = 32,
  FetchSegment = 33,
  AntiEntropy = 34,
  // generic reply
  Reply = 100,
};

// A length-prefixed, binary-safe field bag for message headers.
// Values are stored as strings (ints as decimal, digests as hex) — compact and robust.
class Fields {
 public:
  void set(const std::string& k, const std::string& v);
  void set_i64(const std::string& k, int64_t v);
  void set_u64(const std::string& k, uint64_t v);

  std::optional<std::string> get(const std::string& k) const;
  std::string get_or(const std::string& k, const std::string& def = "") const;
  int64_t get_i64(const std::string& k, int64_t def = 0) const;
  uint64_t get_u64(const std::string& k, uint64_t def = 0) const;

  void serialize(std::string& out) const;
  // Parse from buffer; returns false on malformed/over-limit input.
  static bool parse(const uint8_t* data, size_t len, Fields& out);

  const std::vector<std::pair<std::string, std::string>>& items() const { return kv_; }

 private:
  std::vector<std::pair<std::string, std::string>> kv_;
};

// Fixed frame prefix helpers. Layout:
//   MAGIC(4) VERSION(2) TYPE(2) HEADER_LEN(4) | HEADER | PAYLOAD_LEN(8) | PAYLOAD
struct FramePrefix {
  uint16_t type{0};
  uint32_t header_len{0};
};

// Little-endian encoders/decoders.
void put_u16(std::string& b, uint16_t v);
void put_u32(std::string& b, uint32_t v);
void put_u64(std::string& b, uint64_t v);
uint16_t get_u16(const uint8_t* p);
uint32_t get_u32(const uint8_t* p);
uint64_t get_u64(const uint8_t* p);

}  // namespace aegisfs
