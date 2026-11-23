#pragma once
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "aegisfs/ring.hpp"

namespace aegisfs {

enum class MemberState : uint8_t { Alive = 0, Suspect = 1, Failed = 2, Left = 3 };

std::string memberStateName(MemberState s);

struct Member {
  std::string nodeId;
  std::string address;        // host:port
  uint64_t incarnation{0};    // monotonically increasing per node; refutes stale info
  MemberState state{MemberState::Alive};
  uint64_t lastChangeUs{0}; // local wall-time of last state change (for timeouts)
};

// Eventually-consistent membership table (SWIM-inspired) with incarnation numbers.
// Thread-safe. Failure *detection* (probing) is driven by Node; this class stores/merges state.
class Membership {
 public:
  Membership(std::string selfId, std::string selfAddr);

  const std::string& selfId() const { return selfIdValue; }
  const std::string& selfAddr() const { return selfAddrValue; }
  uint64_t selfIncarnation() const;

  // Seed/learn a peer.
  void addOrUpdate(const Member& m);

  // Merge a gossiped member list. Returns true if anything changed.
  // Refutes false SUSPECT/FAILED about self by bumping our own incarnation.
  bool merge(const std::vector<Member>& incoming);

  // Local transitions driven by the failure detector.
  void markSuspect(const std::string& nodeId);
  void markFailed(const std::string& nodeId);
  void markLeft(const std::string& nodeId);
  void markAlive(const std::string& nodeId, const std::string& addr, uint64_t incarnation);

  // Time-based maintenance: Suspect->Failed after suspectMs; prune tombstones after reapMs.
  // Returns nodeIds newly transitioned to Failed (for re-replication triggers).
  std::vector<std::string> tick(uint64_t suspectMs, uint64_t reapMs);

  std::vector<Member> all() const;                 // every known member (incl. failed/left)
  std::vector<Member> alive() const;               // Alive only
  std::vector<RingNode> ringNodes() const;        // Alive members as ring nodes
  size_t aliveCount() const;
  bool isAlive(const std::string& nodeId) const;
  bool lookup(const std::string& nodeId, Member& out) const;

  // Serialize/parse the whole table for gossip payloads.
  std::string serialize() const;
  static bool parse(const std::string& buf, std::vector<Member>& out);

 private:
  mutable std::mutex mutex;
  std::string selfIdValue;
  std::string selfAddrValue;
  std::vector<Member> members;  // includes self
  Member* find(const std::string& id);
  const Member* find(const std::string& id) const;
  static uint64_t nowUs();
};

}  // namespace aegisfs
