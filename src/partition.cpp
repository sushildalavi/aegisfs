#include "aegisfs/partition.hpp"

#include <mutex>
#include <set>
#include <utility>

namespace aegisfs::fault {

namespace {
std::mutex gMutex;
std::set<std::pair<std::string, std::string>> gBlocked;
std::set<std::string> gBlockedAddresses;

std::pair<std::string, std::string> ordered(const std::string& a, const std::string& b) {
  return a < b ? std::make_pair(a, b) : std::make_pair(b, a);
}
}  // namespace

void partitionBlock(const std::string& a, const std::string& b) {
  std::lock_guard<std::mutex> lk(gMutex);
  gBlocked.insert(ordered(a, b));
}

void partitionHealAll() {
  std::lock_guard<std::mutex> lk(gMutex);
  gBlocked.clear();
}

bool partitionBlocked(const std::string& a, const std::string& b) {
  std::lock_guard<std::mutex> lk(gMutex);
  return gBlocked.count(ordered(a, b)) > 0;
}

void blockAddress(const std::string& addr) {
  std::lock_guard<std::mutex> lk(gMutex);
  gBlockedAddresses.insert(addr);
}

void unblockAllAddresses() {
  std::lock_guard<std::mutex> lk(gMutex);
  gBlockedAddresses.clear();
}

bool addressBlocked(const std::string& addr) {
  std::lock_guard<std::mutex> lk(gMutex);
  return gBlockedAddresses.count(addr) > 0;
}

}  // namespace aegisfs::fault
