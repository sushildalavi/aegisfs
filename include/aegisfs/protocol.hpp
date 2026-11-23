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
  StatusQuery = 18,
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
  void setI64(const std::string& k, int64_t v);
  void setU64(const std::string& k, uint64_t v);

  std::optional<std::string> get(const std::string& k) const;
  std::string getOr(const std::string& k, const std::string& def = "") const;
  int64_t getI64(const std::string& k, int64_t def = 0) const;
  uint64_t getU64(const std::string& k, uint64_t def = 0) const;

  void serialize(std::string& out) const;
  // Parse from buffer; returns false on malformed/over-limit input.
  static bool parse(const uint8_t* data, size_t len, Fields& out);

  const std::vector<std::pair<std::string, std::string>>& items() const { return keyValues; }

 private:
  std::vector<std::pair<std::string, std::string>> keyValues;
};

// Fixed frame prefix helpers. Layout:
//   MAGIC(4) VERSION(2) TYPE(2) HEADER_LEN(4) | HEADER | PAYLOAD_LEN(8) | PAYLOAD
struct FramePrefix {
  uint16_t type{0};
  uint32_t headerLen{0};
};

// Little-endian encoders/decoders.
void putU16(std::string& b, uint16_t v);
void putU32(std::string& b, uint32_t v);
void putU64(std::string& b, uint64_t v);
uint16_t getU16(const uint8_t* p);
uint32_t getU32(const uint8_t* p);
uint64_t getU64(const uint8_t* p);

}  // namespace aegisfs
