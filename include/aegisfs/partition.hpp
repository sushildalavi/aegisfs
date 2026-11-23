#pragma once
#include <string>

namespace aegisfs::fault {

// Test-only fault injection: simulate a network partition between two node addresses for the
// failure detector's direct probes, without needing OS-level firewall rules or root access.
// `Node::probeOnce` consults this before dialing; nothing else does, so the data path (replicate,
// fetch, get) is never affected, only direct liveness probes between the two blocked addresses
// fail, exactly as they would if the two processes couldn't reach each other.
// No-op in production: nothing calls partitionBlock() outside of tests.
void partitionBlock(const std::string& a, const std::string& b);
void partitionHealAll();
bool partitionBlocked(const std::string& a, const std::string& b);

// A stronger, one-sided fault: make an address completely undialable, as if that process had
// vanished from the network entirely (no accept, no RST, nothing). Unlike partitionBlock, this
// is checked inside Conn::dial itself, so it affects every outbound connection attempt to that
// address, including from the blocked node's own failure-detector probes of others, since
// Conn::dial is shared code. Use this to simulate "this node died mid-request" without the
// delay of actually tearing down and rejoining threads.
void blockAddress(const std::string& addr);
void unblockAllAddresses();
bool addressBlocked(const std::string& addr);

}  // namespace aegisfs::fault
