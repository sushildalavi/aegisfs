#include "aegisfs/storage.hpp"

#include "aegisfs/partition.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace aegisfs {

StorageEngine::StorageEngine(std::string dataRoot) : dataRoot(std::move(dataRoot)) {}

bool StorageEngine::validFilename(const std::string& name) {
  if (name.empty() || name.size() > 1024) return false;
  if (name.front() == '/') return false;             // no absolute paths
  if (name.find('\0') != std::string::npos) return false;
  // reject control chars
  for (unsigned char c : name) {
    if (c < 0x20) return false;
  }
  // reject traversal components
  size_t start = 0;
  while (start <= name.size()) {
    size_t slash = name.find('/', start);
    std::string comp = name.substr(start, slash == std::string::npos ? std::string::npos
                                                                     : slash - start);
    if (comp == ".." || comp == ".") return false;
    if (slash == std::string::npos) break;
    start = slash + 1;
  }
  return true;
}

std::string StorageEngine::filePath(const std::string& fileId, const std::string& name) const {
  return filesDir() + "/" + fileId + "/" + name;
}

static Status mkdirs(const std::string& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  return ec ? Status::IoError : Status::Ok;
}

static bool validComponent(const std::string& s) {
  if (s.empty() || s == "." || s == "..") return false;
  for (unsigned char c : s) {
    if (c < 0x20 || c == '/' || c == '\\') return false;
  }
  return true;
}

static bool fsyncPath(const std::string& p) {
  int fd = ::open(p.c_str(), O_RDONLY);
  if (fd < 0) return false;
#ifdef F_FULLFSYNC
  int rc = ::fcntl(fd, F_FULLFSYNC);
  if (rc != 0) rc = ::fsync(fd);
#else
  int rc = ::fsync(fd);
#endif
  ::close(fd);
  return rc == 0;
}

Status StorageEngine::init() {
  if (!isOk(mkdirs(filesDir()))) return Status::IoError;
  if (!isOk(mkdirs(tmpDir()))) return Status::IoError;
  return recover();
}

Status StorageEngine::recover() {
  std::error_code ec;
  // 1) wipe stale staging files
  for (auto& e : fs::directory_iterator(tmpDir(), ec)) {
    fs::remove_all(e.path(), ec);
  }
  // 2) for each file dir, drop orphan segments not referenced by the committed manifest
  for (auto& e : fs::directory_iterator(filesDir(), ec)) {
    if (!e.is_directory()) continue;
    std::string fileId = e.path().filename().string();
    Manifest m;
    bool have = loadManifest(fileId, m);
    if (!have) {
      // No committed manifest => nothing was acknowledged for this file; drop dir.
      fs::remove_all(e.path(), ec);
      continue;
    }
    std::vector<std::string> referenced;
    for (const auto& s : m.segments) referenced.push_back(s.diskName);
    for (auto& f : fs::directory_iterator(e.path(), ec)) {
      std::string nm = f.path().filename().string();
      if (nm == "manifest") continue;
      bool ref = false;
      for (const auto& r : referenced) {
        if (r == nm) { ref = true; break; }
      }
      if (!ref) fs::remove(f.path(), ec);  // orphan from a crash before manifest commit
    }
  }
  return Status::Ok;
}

bool StorageEngine::exists(const std::string& fileId) const {
  if (!validComponent(fileId)) return false;
  std::error_code ec;
  return fs::exists(filePath(fileId, "manifest"), ec);
}

bool StorageEngine::loadManifest(const std::string& fileId, Manifest& out) const {
  if (!validComponent(fileId)) return false;
  std::string path = filePath(fileId, "manifest");
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  std::string buf;
  char tmp[65536];
  ssize_t n;
  while ((n = ::read(fd, tmp, sizeof(tmp))) > 0) buf.append(tmp, static_cast<size_t>(n));
  ::close(fd);
  auto m = Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
  if (!m) return false;
  out = std::move(*m);
  return true;
}

static std::atomic<uint64_t> gTmpSeq{0};

Status StorageEngine::commitManifest(const Manifest& m) {
  if (!validComponent(m.fileId)) return Status::InvalidArgument;
  for (const auto& seg : m.segments) {
    if (!validComponent(seg.diskName)) return Status::InvalidArgument;
  }
  std::string dir = filesDir() + "/" + m.fileId;
  if (!isOk(mkdirs(dir))) return Status::IoError;
  std::string bytes;
  m.serialize(bytes);
  std::string tmp = tmpDir() + "/manifest." + std::to_string(gTmpSeq.fetch_add(1)) + "." +
                    std::to_string(::getpid());
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::IoError;
  const char* p = bytes.data();
  size_t left = bytes.size();
  while (left) {
    ssize_t w = ::write(fd, p, left);
    if (w < 0) { ::close(fd); ::unlink(tmp.c_str()); return Status::IoError; }
    p += w; left -= static_cast<size_t>(w);
  }
  if (fault::crashPoint("manifest.afterWrite")) { ::close(fd); return Status::IoError; }
  if (::fsync(fd) != 0) {
    ::close(fd);
    ::unlink(tmp.c_str());
    return Status::IoError;
  }
  ::close(fd);
  if (fault::crashPoint("manifest.afterFsync")) return Status::IoError;
  std::string dst = dir + "/manifest";
  if (::rename(tmp.c_str(), dst.c_str()) != 0) { ::unlink(tmp.c_str()); return Status::IoError; }
  if (fault::crashPoint("manifest.afterRename")) return Status::IoError;
  if (!fsyncPath(dir)) return Status::IoError;
  return Status::Ok;
}

Status StorageEngine::removeFile(const std::string& fileId) {
  if (!validComponent(fileId)) return Status::InvalidArgument;
  std::error_code ec;
  fs::remove_all(filesDir() + "/" + fileId, ec);
  return ec ? Status::IoError : Status::Ok;
}

std::vector<Manifest> StorageEngine::listManifests() const {
  std::vector<Manifest> out;
  std::error_code ec;
  for (auto& e : fs::directory_iterator(filesDir(), ec)) {
    if (!e.is_directory()) continue;
    Manifest m;
    if (loadManifest(e.path().filename().string(), m)) out.push_back(std::move(m));
  }
  return out;
}

Status StorageEngine::putSegmentStream(const std::string& fileId, const std::string& diskName,
                                         uint64_t payloadLen,
                                         const std::function<size_t(char*, size_t)>& source,
                                         Sha256Digest& outSha) {
  if (!validComponent(fileId) || !validComponent(diskName)) return Status::InvalidArgument;
  std::string dir = filesDir() + "/" + fileId;
  if (!isOk(mkdirs(dir))) return Status::IoError;
  std::string tmp = tmpDir() + "/seg." + std::to_string(gTmpSeq.fetch_add(1)) + "." +
                    std::to_string(::getpid());
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::IoError;
  Sha256Stream hasher;
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t got = 0;
  while (got < payloadLen) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payloadLen - got));
    size_t n = source(buf.data(), want);
    if (n == 0 || n > want) {
      ::close(fd);
      ::unlink(tmp.c_str());
      return Status::IoError;
    }
    hasher.update(buf.data(), n);
    const char* p = buf.data();
    size_t left = n;
    while (left) {
      ssize_t w = ::write(fd, p, left);
      if (w <= 0) { ::close(fd); ::unlink(tmp.c_str()); return Status::IoError; }
      p += w; left -= static_cast<size_t>(w);
    }
    got += n;
  }
  if (fault::crashPoint("segment.afterWrite")) { ::close(fd); return Status::IoError; }
  if (::fsync(fd) != 0) {
    ::close(fd);
    ::unlink(tmp.c_str());
    return Status::IoError;
  }
  ::close(fd);
  outSha = hasher.finalize();
  if (fault::crashPoint("segment.afterFsync")) return Status::IoError;
  std::string dst = dir + "/" + diskName;
  if (::rename(tmp.c_str(), dst.c_str()) != 0) { ::unlink(tmp.c_str()); return Status::IoError; }
  if (fault::crashPoint("segment.afterRename")) return Status::IoError;
  if (!fsyncPath(dir)) return Status::IoError;
  return Status::Ok;
}

Status StorageEngine::commitSpooledSegment(const std::string& fileId, const std::string& diskName,
                                            const std::string& spooledPath) {
  if (!validComponent(fileId) || !validComponent(diskName)) return Status::InvalidArgument;
  std::string dir = filesDir() + "/" + fileId;
  if (!isOk(mkdirs(dir))) return Status::IoError;
  std::string dst = dir + "/" + diskName;
  if (fault::crashPoint("spooled.beforeRename")) return Status::IoError;
  if (::rename(spooledPath.c_str(), dst.c_str()) != 0) return Status::IoError;
  if (fault::crashPoint("spooled.afterRename")) return Status::IoError;
  if (!fsyncPath(dir)) return Status::IoError;
  return Status::Ok;
}

Status StorageEngine::putSegment(const std::string& fileId, const std::string& diskName,
                                  const std::string& bytes, const Sha256Digest& expectedSha) {
  size_t off = 0;
  Sha256Digest got;
  Status s = putSegmentStream(
      fileId, diskName, bytes.size(),
      [&](char* b, size_t cap) -> size_t {
        size_t n = std::min(cap, bytes.size() - off);
        std::memcpy(b, bytes.data() + off, n);
        off += n;
        return n;
      },
      got);
  if (!isOk(s)) return s;
  if (got != expectedSha) {
    // Remove the bad segment so it is not referenced.
    std::error_code ec;
    fs::remove(filePath(fileId, diskName), ec);
    return Status::ChecksumMismatch;
  }
  return Status::Ok;
}

bool StorageEngine::segmentSize(const std::string& fileId, const std::string& diskName,
                                 uint64_t& out) const {
  if (!validComponent(fileId) || !validComponent(diskName)) return false;
  struct stat st;
  if (::stat(filePath(fileId, diskName).c_str(), &st) != 0) return false;
  out = static_cast<uint64_t>(st.st_size);
  return true;
}

Status StorageEngine::readSegment(const std::string& fileId, const std::string& diskName,
                                   std::string& out) const {
  out.clear();
  return readSegmentStream(fileId, diskName,
                             [&](const char* b, size_t n) { out.append(b, n); return true; });
}

Status StorageEngine::readSegmentStream(
    const std::string& fileId, const std::string& diskName,
    const std::function<bool(const char*, size_t)>& sink) const {
  if (!validComponent(fileId) || !validComponent(diskName)) return Status::InvalidArgument;
  std::string path = filePath(fileId, diskName);
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::NotFound;
  std::vector<char> buf(kStreamChunkBytes);
  ssize_t n;
  while ((n = ::read(fd, buf.data(), buf.size())) > 0) {
    if (!sink(buf.data(), static_cast<size_t>(n))) { ::close(fd); return Status::IoError; }
  }
  ::close(fd);
  return n < 0 ? Status::IoError : Status::Ok;
}

Status StorageEngine::reconstruct(const Manifest& m,
                                  const std::function<bool(const char*, size_t)>& sink,
                                  Sha256Digest& outFileSha) const {
  Sha256Stream hasher;
  for (const SegmentRef* s : m.ordered()) {
    Sha256Stream segHasher;
    Status rs = readSegmentStream(m.fileId, s->diskName, [&](const char* b, size_t n) {
      segHasher.update(b, n);
      hasher.update(b, n);
      return sink(b, n);
    });
    if (!isOk(rs)) return rs;
    if (segHasher.finalize() != s->sha) return Status::ChecksumMismatch;
  }
  outFileSha = hasher.finalize();
  return Status::Ok;
}

Status StorageEngine::fileSha(const Manifest& m, Sha256Digest& out) const {
  return reconstruct(m, [](const char*, size_t) { return true; }, out);
}

}  // namespace aegisfs
