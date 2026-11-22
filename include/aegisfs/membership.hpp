#pragma once
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "aegisfs/ring.hpp"

namespace aegisfs {

enum class MemberState : uint8_t { Alive = 0, Suspect = 1, Failed = 2, Left = 3 };

std::string member_state_name(MemberState s);

struct Member {
  std::string node_id;
  std::string address;        // host:port
  uint64_t incarnation{0};    // monotonically increasing per node; refutes stale info
  MemberState state{MemberState::Alive};
  uint64_t last_change_us{0}; // local wall-time of last state change (for timeouts)
};

// Eventually-consistent membership table (SWIM-inspired) with incarnation numbers.
// Thread-safe. Failure *detection* (probing) is driven by Node; this class stores/merges state.
class Membership {
 public:
  Membership(std::string self_id, std::string self_addr);

  const std::string& self_id() const { return self_id_; }
  const std::string& self_addr() const { return self_addr_; }
  uint64_t self_incarnation() const;

  // Seed/learn a peer.
  void add_or_update(const Member& m);

  // Merge a gossiped member list. Returns true if anything changed.
  // Refutes false SUSPECT/FAILED about self by bumping our own incarnation.
  bool merge(const std::vector<Member>& incoming);

  // Local transitions driven by the failure detector.
  void mark_suspect(const std::string& node_id);
  void mark_failed(const std::string& node_id);
  void mark_left(const std::string& node_id);
  void mark_alive(const std::string& node_id, const std::string& addr, uint64_t incarnation);

  // Time-based maintenance: Suspect->Failed after suspect_ms; prune tombstones after reap_ms.
  // Returns node_ids newly transitioned to Failed (for re-replication triggers).
  std::vector<std::string> tick(uint64_t suspect_ms, uint64_t reap_ms);

  std::vector<Member> all() const;                 // every known member (incl. failed/left)
  std::vector<Member> alive() const;               // Alive only
  std::vector<RingNode> ring_nodes() const;        // Alive members as ring nodes
  size_t alive_count() const;
  bool is_alive(const std::string& node_id) const;
  bool lookup(const std::string& node_id, Member& out) const;

  // Serialize/parse the whole table for gossip payloads.
  std::string serialize() const;
  static bool parse(const std::string& buf, std::vector<Member>& out);

 private:
  mutable std::mutex mu_;
  std::string self_id_;
  std::string self_addr_;
  std::vector<Member> members_;  // includes self
  Member* find(const std::string& id);
  const Member* find(const std::string& id) const;
  static uint64_t now_us();
};

}  // namespace aegisfs
