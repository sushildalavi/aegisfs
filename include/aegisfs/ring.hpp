#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace aegisfs {

// A node as seen on the hash ring.
struct RingNode {
  std::string node_id;  // stable unique identity (uuid-ish)
  std::string address;  // host:port
  uint64_t rid{0};      // ring_id(node_id)
  bool operator==(const RingNode& o) const {
    return node_id == o.node_id && address == o.address && rid == o.rid;
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
  // from node_id so callers cannot inject inconsistent ids.
  void set_members(std::vector<RingNode> members);

  // Number of distinct members currently on the ring.
  size_t size() const { return nodes_.size(); }
  bool empty() const { return nodes_.empty(); }

  const std::vector<RingNode>& members() const { return nodes_; }

  // The ordered set of up to `rf` distinct successor nodes responsible for `filename`.
  // Primary is the first node with rid >= ring_id(filename), wrapping around the ring.
  // Returns fewer than rf only when the ring has fewer than rf members. O(log N) lookup.
  std::vector<RingNode> successors(const std::string& filename, int rf) const;

  // The ring position (64-bit id) for a filename.
  uint64_t file_rid(const std::string& filename) const;

  // True if `node_id` is among the rf successors of `filename`.
  bool is_replica(const std::string& filename, const std::string& node_id, int rf) const;

  // Index of the first node with rid >= target (wrapping). Precondition: !empty().
  size_t first_at_or_after(uint64_t target) const;

 private:
  std::vector<RingNode> nodes_;  // sorted by (rid, node_id), distinct node_id
};

}  // namespace aegisfs
