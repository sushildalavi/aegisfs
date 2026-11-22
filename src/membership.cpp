#include "aegisfs/membership.hpp"

#include <algorithm>
#include <chrono>

#include "aegisfs/protocol.hpp"  // LE encoders

namespace aegisfs {

std::string member_state_name(MemberState s) {
  switch (s) {
    case MemberState::Alive: return "ALIVE";
    case MemberState::Suspect: return "SUSPECT";
    case MemberState::Failed: return "FAILED";
    case MemberState::Left: return "LEFT";
  }
  return "UNKNOWN";
}

static int state_rank(MemberState s) { return static_cast<int>(s); }

uint64_t Membership::now_us() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

Membership::Membership(std::string self_id, std::string self_addr)
    : self_id_(std::move(self_id)), self_addr_(std::move(self_addr)) {
  members_.push_back(Member{self_id_, self_addr_, 1, MemberState::Alive, now_us()});
}

Member* Membership::find(const std::string& id) {
  for (auto& m : members_) {
    if (m.node_id == id) return &m;
  }
  return nullptr;
}
const Member* Membership::find(const std::string& id) const {
  for (const auto& m : members_) {
    if (m.node_id == id) return &m;
  }
  return nullptr;
}

uint64_t Membership::self_incarnation() const {
  std::lock_guard<std::mutex> lk(mu_);
  const Member* m = find(self_id_);
  return m ? m->incarnation : 0;
}

void Membership::add_or_update(const Member& m) {
  std::lock_guard<std::mutex> lk(mu_);
  Member* ex = find(m.node_id);
  if (!ex) {
    Member nm = m;
    nm.last_change_us = now_us();
    members_.push_back(nm);
  } else {
    ex->address = m.address;
    if (m.incarnation > ex->incarnation) {
      ex->incarnation = m.incarnation;
      ex->state = m.state;
      ex->last_change_us = now_us();
    }
  }
}

bool Membership::merge(const std::vector<Member>& incoming) {
  std::lock_guard<std::mutex> lk(mu_);
  bool changed = false;
  for (const auto& in : incoming) {
    if (in.node_id == self_id_) {
      // Refute false rumors about ourselves.
      Member* self = find(self_id_);
      if (self && (in.state == MemberState::Suspect || in.state == MemberState::Failed) &&
          in.incarnation >= self->incarnation) {
        self->incarnation = in.incarnation + 1;
        self->state = MemberState::Alive;
        self->last_change_us = now_us();
        changed = true;
      }
      continue;
    }
    Member* ex = find(in.node_id);
    if (!ex) {
      Member nm = in;
      nm.last_change_us = now_us();
      members_.push_back(nm);
      changed = true;
      continue;
    }
    ex->address = in.address;
    if (in.incarnation > ex->incarnation) {
      ex->incarnation = in.incarnation;
      ex->state = in.state;
      ex->last_change_us = now_us();
      changed = true;
    } else if (in.incarnation == ex->incarnation &&
               state_rank(in.state) > state_rank(ex->state)) {
      ex->state = in.state;  // escalate (Alive<Suspect<Failed<Left)
      ex->last_change_us = now_us();
      changed = true;
    }
  }
  return changed;
}

void Membership::mark_suspect(const std::string& id) {
  std::lock_guard<std::mutex> lk(mu_);
  Member* m = find(id);
  if (m && m->state == MemberState::Alive) {
    m->state = MemberState::Suspect;
    m->last_change_us = now_us();
  }
}
void Membership::mark_failed(const std::string& id) {
  std::lock_guard<std::mutex> lk(mu_);
  Member* m = find(id);
  if (m && m->state != MemberState::Left && m->state != MemberState::Failed) {
    m->state = MemberState::Failed;
    m->last_change_us = now_us();
  }
}
void Membership::mark_left(const std::string& id) {
  std::lock_guard<std::mutex> lk(mu_);
  Member* m = find(id);
  if (m) {
    m->state = MemberState::Left;
    m->last_change_us = now_us();
  }
}
void Membership::mark_alive(const std::string& id, const std::string& addr, uint64_t inc) {
  std::lock_guard<std::mutex> lk(mu_);
  Member* m = find(id);
  if (!m) {
    members_.push_back(Member{id, addr, inc, MemberState::Alive, now_us()});
    return;
  }
  if (inc >= m->incarnation) {
    m->incarnation = inc;
    m->state = MemberState::Alive;
    m->address = addr;
    m->last_change_us = now_us();
  }
}

std::vector<std::string> Membership::tick(uint64_t suspect_ms, uint64_t reap_ms) {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<std::string> newly_failed;
  uint64_t now = now_us();
  for (auto& m : members_) {
    if (m.node_id == self_id_) continue;
    uint64_t age_ms = (now - m.last_change_us) / 1000;
    if (m.state == MemberState::Suspect && age_ms >= suspect_ms) {
      m.state = MemberState::Failed;
      m.last_change_us = now;
      newly_failed.push_back(m.node_id);
    }
  }
  // reap tombstones (Failed/Left) long past their change time
  members_.erase(std::remove_if(members_.begin(), members_.end(),
                                [&](const Member& m) {
                                  if (m.node_id == self_id_) return false;
                                  if (m.state != MemberState::Failed && m.state != MemberState::Left)
                                    return false;
                                  return (now - m.last_change_us) / 1000 >= reap_ms;
                                }),
                 members_.end());
  return newly_failed;
}

std::vector<Member> Membership::all() const {
  std::lock_guard<std::mutex> lk(mu_);
  return members_;
}
std::vector<Member> Membership::alive() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<Member> out;
  for (const auto& m : members_) {
    if (m.state == MemberState::Alive) out.push_back(m);
  }
  return out;
}
std::vector<RingNode> Membership::ring_nodes() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<RingNode> out;
  for (const auto& m : members_) {
    if (m.state == MemberState::Alive) out.push_back(RingNode{m.node_id, m.address, 0});
  }
  return out;
}
size_t Membership::alive_count() const {
  std::lock_guard<std::mutex> lk(mu_);
  size_t c = 0;
  for (const auto& m : members_) {
    if (m.state == MemberState::Alive) ++c;
  }
  return c;
}
bool Membership::is_alive(const std::string& id) const {
  std::lock_guard<std::mutex> lk(mu_);
  const Member* m = find(id);
  return m && m->state == MemberState::Alive;
}
bool Membership::lookup(const std::string& id, Member& out) const {
  std::lock_guard<std::mutex> lk(mu_);
  const Member* m = find(id);
  if (!m) return false;
  out = *m;
  return true;
}

static void put_s(std::string& b, const std::string& s) {
  put_u32(b, static_cast<uint32_t>(s.size()));
  b.append(s);
}

std::string Membership::serialize() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::string b;
  put_u32(b, static_cast<uint32_t>(members_.size()));
  for (const auto& m : members_) {
    put_s(b, m.node_id);
    put_s(b, m.address);
    put_u64(b, m.incarnation);
    b.push_back(static_cast<char>(m.state));
  }
  return b;
}

bool Membership::parse(const std::string& buf, std::vector<Member>& out) {
  out.clear();
  const uint8_t* d = reinterpret_cast<const uint8_t*>(buf.data());
  size_t len = buf.size(), pos = 0;
  auto need = [&](size_t n) { return pos + n <= len; };
  if (!need(4)) return false;
  uint32_t n = get_u32(d); pos += 4;
  if (n > 1'000'000u) return false;
  for (uint32_t i = 0; i < n; ++i) {
    Member m;
    if (!need(4)) return false;
    uint32_t l = get_u32(d + pos); pos += 4;
    if (!need(l)) return false;
    m.node_id.assign(reinterpret_cast<const char*>(d + pos), l); pos += l;
    if (!need(4)) return false;
    l = get_u32(d + pos); pos += 4;
    if (!need(l)) return false;
    m.address.assign(reinterpret_cast<const char*>(d + pos), l); pos += l;
    if (!need(9)) return false;
    m.incarnation = get_u64(d + pos); pos += 8;
    m.state = static_cast<MemberState>(d[pos]); pos += 1;
    out.push_back(std::move(m));
  }
  return true;
}

}  // namespace aegisfs
