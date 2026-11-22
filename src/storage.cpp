#include "aegisfs/storage.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace aegisfs {

StorageEngine::StorageEngine(std::string data_dir) : data_dir_(std::move(data_dir)) {}

bool StorageEngine::valid_filename(const std::string& name) {
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

std::string StorageEngine::file_path(const std::string& file_id, const std::string& name) const {
  return files_dir() + "/" + file_id + "/" + name;
}

static Status mkdirs(const std::string& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  return ec ? Status::IoError : Status::Ok;
}

static bool fsync_path(const std::string& p) {
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
  if (!is_ok(mkdirs(files_dir()))) return Status::IoError;
  if (!is_ok(mkdirs(tmp_dir()))) return Status::IoError;
  return recover();
}

Status StorageEngine::recover() {
  std::error_code ec;
  // 1) wipe stale staging files
  for (auto& e : fs::directory_iterator(tmp_dir(), ec)) {
    fs::remove_all(e.path(), ec);
  }
  // 2) for each file dir, drop orphan segments not referenced by the committed manifest
  for (auto& e : fs::directory_iterator(files_dir(), ec)) {
    if (!e.is_directory()) continue;
    std::string file_id = e.path().filename().string();
    Manifest m;
    bool have = load_manifest(file_id, m);
    if (!have) {
      // No committed manifest => nothing was acknowledged for this file; drop dir.
      fs::remove_all(e.path(), ec);
      continue;
    }
    std::vector<std::string> referenced;
    for (const auto& s : m.segments) referenced.push_back(s.disk_name);
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

bool StorageEngine::exists(const std::string& file_id) const {
  std::error_code ec;
  return fs::exists(file_path(file_id, "manifest"), ec);
}

bool StorageEngine::load_manifest(const std::string& file_id, Manifest& out) const {
  std::string path = file_path(file_id, "manifest");
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

static std::atomic<uint64_t> g_tmp_seq{0};

Status StorageEngine::commit_manifest(const Manifest& m) {
  std::string dir = files_dir() + "/" + m.file_id;
  if (!is_ok(mkdirs(dir))) return Status::IoError;
  std::string bytes;
  m.serialize(bytes);
  std::string tmp = tmp_dir() + "/manifest." + std::to_string(g_tmp_seq.fetch_add(1)) + "." +
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
  ::fsync(fd);
  ::close(fd);
  std::string dst = dir + "/manifest";
  if (::rename(tmp.c_str(), dst.c_str()) != 0) { ::unlink(tmp.c_str()); return Status::IoError; }
  fsync_path(dir);
  return Status::Ok;
}

Status StorageEngine::remove_file(const std::string& file_id) {
  std::error_code ec;
  fs::remove_all(files_dir() + "/" + file_id, ec);
  return ec ? Status::IoError : Status::Ok;
}

std::vector<Manifest> StorageEngine::list_manifests() const {
  std::vector<Manifest> out;
  std::error_code ec;
  for (auto& e : fs::directory_iterator(files_dir(), ec)) {
    if (!e.is_directory()) continue;
    Manifest m;
    if (load_manifest(e.path().filename().string(), m)) out.push_back(std::move(m));
  }
  return out;
}

Status StorageEngine::put_segment_stream(const std::string& file_id, const std::string& disk_name,
                                         uint64_t payload_len,
                                         const std::function<size_t(char*, size_t)>& source,
                                         Sha256Digest& out_sha) {
  std::string dir = files_dir() + "/" + file_id;
  if (!is_ok(mkdirs(dir))) return Status::IoError;
  std::string tmp = tmp_dir() + "/seg." + std::to_string(g_tmp_seq.fetch_add(1)) + "." +
                    std::to_string(::getpid());
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::IoError;
  Sha256Stream hasher;
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t got = 0;
  while (got < payload_len) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payload_len - got));
    size_t n = source(buf.data(), want);
    if (n == 0) { ::close(fd); ::unlink(tmp.c_str()); return Status::IoError; }
    hasher.update(buf.data(), n);
    const char* p = buf.data();
    size_t left = n;
    while (left) {
      ssize_t w = ::write(fd, p, left);
      if (w < 0) { ::close(fd); ::unlink(tmp.c_str()); return Status::IoError; }
      p += w; left -= static_cast<size_t>(w);
    }
    got += n;
  }
  ::fsync(fd);
  ::close(fd);
  out_sha = hasher.finalize();
  std::string dst = dir + "/" + disk_name;
  if (::rename(tmp.c_str(), dst.c_str()) != 0) { ::unlink(tmp.c_str()); return Status::IoError; }
  fsync_path(dir);
  return Status::Ok;
}

Status StorageEngine::put_segment(const std::string& file_id, const std::string& disk_name,
                                  const std::string& bytes, const Sha256Digest& expected_sha) {
  size_t off = 0;
  Sha256Digest got;
  Status s = put_segment_stream(
      file_id, disk_name, bytes.size(),
      [&](char* b, size_t cap) -> size_t {
        size_t n = std::min(cap, bytes.size() - off);
        std::memcpy(b, bytes.data() + off, n);
        off += n;
        return n;
      },
      got);
  if (!is_ok(s)) return s;
  if (got != expected_sha) {
    // Remove the bad segment so it is not referenced.
    std::error_code ec;
    fs::remove(file_path(file_id, disk_name), ec);
    return Status::ChecksumMismatch;
  }
  return Status::Ok;
}

Status StorageEngine::read_segment(const std::string& file_id, const std::string& disk_name,
                                   std::string& out) const {
  out.clear();
  return read_segment_stream(file_id, disk_name,
                             [&](const char* b, size_t n) { out.append(b, n); return true; });
}

Status StorageEngine::read_segment_stream(
    const std::string& file_id, const std::string& disk_name,
    const std::function<bool(const char*, size_t)>& sink) const {
  std::string path = file_path(file_id, disk_name);
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
                                  Sha256Digest& out_file_sha) const {
  Sha256Stream hasher;
  for (const SegmentRef* s : m.ordered()) {
    Sha256Stream seg_hasher;
    Status rs = read_segment_stream(m.file_id, s->disk_name, [&](const char* b, size_t n) {
      seg_hasher.update(b, n);
      hasher.update(b, n);
      return sink(b, n);
    });
    if (!is_ok(rs)) return rs;
    if (seg_hasher.finalize() != s->sha) return Status::ChecksumMismatch;
  }
  out_file_sha = hasher.finalize();
  return Status::Ok;
}

Status StorageEngine::file_sha(const Manifest& m, Sha256Digest& out) const {
  return reconstruct(m, [](const char*, size_t) { return true; }, out);
}

}  // namespace aegisfs
