#include "aegisfs/membership.hpp"

#include <algorithm>
#include <chrono>

#include "aegisfs/protocol.hpp"  // LE encoders

namespace aegisfs {

std::string memberStateName(MemberState s) {
  switch (s) {
    case MemberState::Alive: return "ALIVE";
    case MemberState::Suspect: return "SUSPECT";
    case MemberState::Failed: return "FAILED";
    case MemberState::Left: return "LEFT";
  }
  return "UNKNOWN";
}

static int stateRank(MemberState s) { return static_cast<int>(s); }

uint64_t Membership::nowUs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

Membership::Membership(std::string selfId, std::string selfAddr)
    : selfIdValue(std::move(selfId)), selfAddrValue(std::move(selfAddr)) {
  members.push_back(Member{selfIdValue, selfAddrValue, 1, MemberState::Alive, nowUs()});
}

Member* Membership::find(const std::string& id) {
  for (auto& m : members) {
    if (m.nodeId == id) return &m;
  }
  return nullptr;
}
const Member* Membership::find(const std::string& id) const {
  for (const auto& m : members) {
    if (m.nodeId == id) return &m;
  }
  return nullptr;
}

uint64_t Membership::selfIncarnation() const {
  std::lock_guard<std::mutex> lk(mutex);
  const Member* m = find(selfIdValue);
  return m ? m->incarnation : 0;
}

void Membership::addOrUpdate(const Member& m) {
  std::lock_guard<std::mutex> lk(mutex);
  Member* ex = find(m.nodeId);
  if (!ex) {
    Member nm = m;
    nm.lastChangeUs = nowUs();
    members.push_back(nm);
  } else {
    ex->address = m.address;
    if (m.incarnation > ex->incarnation) {
      ex->incarnation = m.incarnation;
      ex->state = m.state;
      ex->lastChangeUs = nowUs();
    }
  }
}

bool Membership::merge(const std::vector<Member>& incoming) {
  std::lock_guard<std::mutex> lk(mutex);
  bool changed = false;
  for (const auto& in : incoming) {
    if (in.nodeId == selfIdValue) {
      // Refute false rumors about ourselves.
      Member* self = find(selfIdValue);
      if (self && (in.state == MemberState::Suspect || in.state == MemberState::Failed) &&
          in.incarnation >= self->incarnation) {
        self->incarnation = in.incarnation + 1;
        self->state = MemberState::Alive;
        self->lastChangeUs = nowUs();
        changed = true;
      }
      continue;
    }
    Member* ex = find(in.nodeId);
    if (!ex) {
      Member nm = in;
      nm.lastChangeUs = nowUs();
      members.push_back(nm);
      changed = true;
      continue;
    }
    ex->address = in.address;
    if (in.incarnation > ex->incarnation) {
      ex->incarnation = in.incarnation;
      ex->state = in.state;
      ex->lastChangeUs = nowUs();
      changed = true;
    } else if (in.incarnation == ex->incarnation &&
               stateRank(in.state) > stateRank(ex->state)) {
      ex->state = in.state;  // escalate (Alive<Suspect<Failed<Left)
      ex->lastChangeUs = nowUs();
      changed = true;
    }
  }
  return changed;
}

void Membership::markSuspect(const std::string& id) {
  std::lock_guard<std::mutex> lk(mutex);
  Member* m = find(id);
  if (m && m->state == MemberState::Alive) {
    m->state = MemberState::Suspect;
    m->lastChangeUs = nowUs();
  }
}
void Membership::markFailed(const std::string& id) {
  std::lock_guard<std::mutex> lk(mutex);
  Member* m = find(id);
  if (m && m->state != MemberState::Left && m->state != MemberState::Failed) {
    m->state = MemberState::Failed;
    m->lastChangeUs = nowUs();
  }
}
void Membership::markLeft(const std::string& id) {
  std::lock_guard<std::mutex> lk(mutex);
  Member* m = find(id);
  if (m) {
    m->state = MemberState::Left;
    m->lastChangeUs = nowUs();
  }
}
void Membership::markAlive(const std::string& id, const std::string& addr, uint64_t inc) {
  std::lock_guard<std::mutex> lk(mutex);
  Member* m = find(id);
  if (!m) {
    members.push_back(Member{id, addr, inc, MemberState::Alive, nowUs()});
    return;
  }
  if (inc >= m->incarnation) {
    m->incarnation = inc;
    m->state = MemberState::Alive;
    m->address = addr;
    m->lastChangeUs = nowUs();
  }
}

std::vector<std::string> Membership::tick(uint64_t suspectMs, uint64_t reapMs) {
  std::lock_guard<std::mutex> lk(mutex);
  std::vector<std::string> newlyFailed;
  uint64_t now = nowUs();
  for (auto& m : members) {
    if (m.nodeId == selfIdValue) continue;
    uint64_t ageMs = (now - m.lastChangeUs) / 1000;
    if (m.state == MemberState::Suspect && ageMs >= suspectMs) {
      m.state = MemberState::Failed;
      m.lastChangeUs = now;
      newlyFailed.push_back(m.nodeId);
    }
  }
  // reap tombstones (Failed/Left) long past their change time
  members.erase(std::remove_if(members.begin(), members.end(),
                                [&](const Member& m) {
                                  if (m.nodeId == selfIdValue) return false;
                                  if (m.state != MemberState::Failed && m.state != MemberState::Left)
                                    return false;
                                  return (now - m.lastChangeUs) / 1000 >= reapMs;
                                }),
                 members.end());
  return newlyFailed;
}

std::vector<Member> Membership::all() const {
  std::lock_guard<std::mutex> lk(mutex);
  return members;
}
std::vector<Member> Membership::alive() const {
  std::lock_guard<std::mutex> lk(mutex);
  std::vector<Member> out;
  for (const auto& m : members) {
    if (m.state == MemberState::Alive) out.push_back(m);
  }
  return out;
}
std::vector<RingNode> Membership::ringNodes() const {
  std::lock_guard<std::mutex> lk(mutex);
  std::vector<RingNode> out;
  for (const auto& m : members) {
    if (m.state == MemberState::Alive) out.push_back(RingNode{m.nodeId, m.address, 0});
  }
  return out;
}
size_t Membership::aliveCount() const {
  std::lock_guard<std::mutex> lk(mutex);
  size_t c = 0;
  for (const auto& m : members) {
    if (m.state == MemberState::Alive) ++c;
  }
  return c;
}
bool Membership::isAlive(const std::string& id) const {
  std::lock_guard<std::mutex> lk(mutex);
  const Member* m = find(id);
  return m && m->state == MemberState::Alive;
}
bool Membership::lookup(const std::string& id, Member& out) const {
  std::lock_guard<std::mutex> lk(mutex);
  const Member* m = find(id);
  if (!m) return false;
  out = *m;
  return true;
}

static void putS(std::string& b, const std::string& s) {
  putU32(b, static_cast<uint32_t>(s.size()));
  b.append(s);
}

std::string Membership::serialize() const {
  std::lock_guard<std::mutex> lk(mutex);
  std::string b;
  putU32(b, static_cast<uint32_t>(members.size()));
  for (const auto& m : members) {
    putS(b, m.nodeId);
    putS(b, m.address);
    putU64(b, m.incarnation);
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
  uint32_t n = getU32(d); pos += 4;
  if (n > 1'000'000u) return false;
  for (uint32_t i = 0; i < n; ++i) {
    Member m;
    if (!need(4)) return false;
    uint32_t l = getU32(d + pos); pos += 4;
    if (!need(l)) return false;
    m.nodeId.assign(reinterpret_cast<const char*>(d + pos), l); pos += l;
    if (!need(4)) return false;
    l = getU32(d + pos); pos += 4;
    if (!need(l)) return false;
    m.address.assign(reinterpret_cast<const char*>(d + pos), l); pos += l;
    if (!need(9)) return false;
    m.incarnation = getU64(d + pos); pos += 8;
    m.state = static_cast<MemberState>(d[pos]); pos += 1;
    out.push_back(std::move(m));
  }
  return true;
}

}  // namespace aegisfs
