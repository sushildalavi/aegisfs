#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace aegisfs {

// A node as seen on the hash ring.
struct RingNode {
  std::string nodeId;  // stable unique identity (uuid-ish)
  std::string address;  // host:port
  uint64_t rid{0};      // ringId(nodeId)
  bool operator==(const RingNode& o) const {
    return nodeId == o.nodeId && address == o.address && rid == o.rid;
  }
};

// Number of virtual points each physical node occupies on the ring. More vnodes => smoother key
// distribution (lower variance) at the cost of a larger sorted ring. 64 keeps 10-node imbalance
// well within ~2x of ideal (see RingTest.LoadIsReasonablyBalanced).
inline constexpr int kVirtualNodesPerNode = 64;

// Consistent-hash ring over the 64-bit identifier space, using virtual nodes for balance.
// Deterministic: given the same member set, every node computes identical placement.
class Ring {
 public:
  // Rebuild the ring from a member list (order-independent). Ring IDs are recomputed
  // from nodeId so callers cannot inject inconsistent ids.
  void setMembers(std::vector<RingNode> members);

  // Number of distinct members currently on the ring.
  size_t size() const { return ringNodes.size(); }
  bool empty() const { return ringNodes.empty(); }

  const std::vector<RingNode>& members() const { return ringNodes; }

  // The ordered set of up to `rf` distinct successor nodes responsible for `filename`.
  // Primary is the first node with rid >= ringId(filename), wrapping around the ring.
  // Returns fewer than rf only when the ring has fewer than rf members. O(log N) lookup.
  std::vector<RingNode> successors(const std::string& filename, int rf) const;

  // The ring position (64-bit id) for a filename.
  uint64_t fileRid(const std::string& filename) const;

  // True if `nodeId` is among the rf successors of `filename`.
  bool isReplica(const std::string& filename, const std::string& nodeId, int rf) const;

  // Index of the first node with rid >= target (wrapping). Precondition: !empty().
  size_t firstAtOrAfter(uint64_t target) const;

 private:
  std::vector<RingNode> ringNodes;  // sorted by (rid, nodeId), distinct nodeId
};

}  // namespace aegisfs
