#include "aegisfs/ring.hpp"

#include <algorithm>

#include "aegisfs/hash.hpp"

namespace aegisfs {

void Ring::set_members(std::vector<RingNode> members) {
  // Recompute ring ids from node_id so they are always consistent, and de-duplicate
  // by node_id (last wins for address updates).
  std::vector<RingNode> uniq;
  uniq.reserve(members.size());
  for (auto& m : members) {
    m.rid = ring_id(m.node_id);
    auto it = std::find_if(uniq.begin(), uniq.end(),
                           [&](const RingNode& n) { return n.node_id == m.node_id; });
    if (it == uniq.end()) {
      uniq.push_back(m);
    } else {
      *it = m;  // update address
    }
  }
  std::sort(uniq.begin(), uniq.end(), [](const RingNode& a, const RingNode& b) {
    if (a.rid != b.rid) return a.rid < b.rid;
    return a.node_id < b.node_id;  // stable tie-break on rid collision
  });
  nodes_ = std::move(uniq);
}

uint64_t Ring::file_rid(const std::string& filename) const { return ring_id(filename); }

size_t Ring::first_at_or_after(uint64_t target) const {
  // Binary search for first node with rid >= target; wrap to 0 if none.
  size_t lo = 0, hi = nodes_.size();
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (nodes_[mid].rid < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo == nodes_.size()) lo = 0;  // wraparound
  return lo;
}

std::vector<RingNode> Ring::successors(const std::string& filename, int rf) const {
  std::vector<RingNode> out;
  if (nodes_.empty() || rf <= 0) return out;
  const size_t n = nodes_.size();
  const int want = std::min<int>(rf, static_cast<int>(n));
  size_t start = first_at_or_after(ring_id(filename));
  for (size_t i = 0; i < n && static_cast<int>(out.size()) < want; ++i) {
    const RingNode& cand = nodes_[(start + i) % n];
    // distinct by node_id (de-duped already, but keep guard for safety)
    bool dup = false;
    for (const auto& o : out) {
      if (o.node_id == cand.node_id) { dup = true; break; }
    }
    if (!dup) out.push_back(cand);
  }
  return out;
}

bool Ring::is_replica(const std::string& filename, const std::string& node_id, int rf) const {
  for (const auto& r : successors(filename, rf)) {
    if (r.node_id == node_id) return true;
  }
  return false;
}

}  // namespace aegisfs
