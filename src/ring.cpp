#include "aegisfs/ring.hpp"

#include <algorithm>

#include "aegisfs/hash.hpp"

namespace aegisfs {

void Ring::setMembers(std::vector<RingNode> members) {
  // De-duplicate physical members by nodeId (last wins for address updates).
  std::vector<RingNode> phys;
  for (auto& m : members) {
    auto it = std::find_if(phys.begin(), phys.end(),
                           [&](const RingNode& n) { return n.nodeId == m.nodeId; });
    if (it == phys.end()) phys.push_back(m);
    else it->address = m.address;
  }
  // Expand each physical node into kVirtualNodesPerNode virtual points on the ring; each vnode keeps
  // the physical nodeId/address but gets a distinct ring id from hashing "nodeId#v".
  std::vector<RingNode> vnodes;
  vnodes.reserve(phys.size() * kVirtualNodesPerNode);
  for (const auto& m : phys) {
    for (int v = 0; v < kVirtualNodesPerNode; ++v) {
      RingNode vn = m;
      vn.rid = ringId(m.nodeId + "#" + std::to_string(v));
      vnodes.push_back(vn);
    }
  }
  std::sort(vnodes.begin(), vnodes.end(), [](const RingNode& a, const RingNode& b) {
    if (a.rid != b.rid) return a.rid < b.rid;
    return a.nodeId < b.nodeId;  // stable tie-break on rid collision
  });
  ringNodes = std::move(vnodes);
}

uint64_t Ring::fileRid(const std::string& filename) const { return ringId(filename); }

size_t Ring::firstAtOrAfter(uint64_t target) const {
  // Binary search for first node with rid >= target; wrap to 0 if none.
  size_t lo = 0, hi = ringNodes.size();
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (ringNodes[mid].rid < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo == ringNodes.size()) lo = 0;  // wraparound
  return lo;
}

std::vector<RingNode> Ring::successors(const std::string& filename, int rf) const {
  std::vector<RingNode> out;
  if (ringNodes.empty() || rf <= 0) return out;
  const size_t n = ringNodes.size();
  const int want = std::min<int>(rf, static_cast<int>(n));
  size_t start = firstAtOrAfter(ringId(filename));
  for (size_t i = 0; i < n && static_cast<int>(out.size()) < want; ++i) {
    const RingNode& cand = ringNodes[(start + i) % n];
    // distinct by nodeId (de-duped already, but keep guard for safety)
    bool dup = false;
    for (const auto& o : out) {
      if (o.nodeId == cand.nodeId) { dup = true; break; }
    }
    if (!dup) out.push_back(cand);
  }
  return out;
}

bool Ring::isReplica(const std::string& filename, const std::string& nodeId, int rf) const {
  for (const auto& r : successors(filename, rf)) {
    if (r.nodeId == nodeId) return true;
  }
  return false;
}

}  // namespace aegisfs
