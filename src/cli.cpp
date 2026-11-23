#include "aegisfs/cli.hpp"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "aegisfs/client.hpp"
#include "aegisfs/hash.hpp"
#include "aegisfs/node.hpp"
#include "aegisfs/version.hpp"

namespace aegisfs {

namespace {
std::atomic<bool> gStop{false};
void onSignal(int) { gStop = true; }

std::vector<std::string> splitCsv(const std::string& s) {
  std::vector<std::string> out;
  std::string cur;
  std::stringstream ss(s);
  while (std::getline(ss, cur, ',')) {
    if (!cur.empty()) out.push_back(cur);
  }
  return out;
}

std::string argValue(const std::vector<std::string>& args, const std::string& key,
                      const std::string& def = "") {
  for (size_t i = 0; i + 1 < args.size(); ++i) {
    if (args[i] == key) return args[i + 1];
  }
  return def;
}

int exitCode(Status s) { return isOk(s) ? 0 : (static_cast<int>(s) == 0 ? 1 : static_cast<int>(s)); }

void printHelp() {
  std::cout <<
      "AegisFS: fault-tolerant distributed file system\n\n"
      "Run a node:\n"
      "  aegisfs serve --data <dir> [--host 127.0.0.1] [--port N] [--id NAME]\n"
      "                [--seeds host:port,host:port] [--rf 3]\n"
      "                [--scrub-interval-ms 300000]\n\n"
      "  TLS node flags: --tls-cert FILE --tls-key FILE [--tls-ca FILE] [--tls-require-client]\n"
      "Client commands (prefix with --entry host:port):\n"
      "  create <localfile> <name>         upload a new file (first-create-only)\n"
      "  get <name> <localfile>            download (read-your-writes)\n"
      "  getQuorum <name> <localfile>      majority read; selects highest replica version\n"
      "  append <localfile> <name>         append bytes to an existing file\n"
      "  merge <name>                      reconcile all replicas to identical bytes\n"
      "  ls <name>                         show file id + designated replicas\n"
      "  listStore                         list files stored on the entry node\n"
      "  getFromReplica <addr> <name> <localfile>   pure replica read (no repair)\n"
      "  listMemIds                      list members by ring id + status\n"
      "  multiAppend <name> <node> <file> [<node> <file> ...]  concurrent appends\n"
      "  status | health                   node status / liveness\n"
      "  help                              this message\n";
}

int runServe(const std::vector<std::string>& args) {
  NodeConfig cfg;
  cfg.dataRoot = argValue(args, "--data", "./data");
  cfg.host = argValue(args, "--host", "127.0.0.1");
  cfg.port = std::atoi(argValue(args, "--port", "0").c_str());
  cfg.nodeId = argValue(args, "--id", "");
  cfg.rf = std::atoi(argValue(args, "--rf", "3").c_str());
  cfg.parallelReplication = argValue(args, "--parallel", "1") != "0";
  cfg.scrubIntervalMs = std::atoi(argValue(args, "--scrub-interval-ms", "300000").c_str());
  cfg.seeds = splitCsv(argValue(args, "--seeds", ""));
  TlsConfig tls;
  tls.certificate = argValue(args, "--tls-cert");
  tls.privateKey = argValue(args, "--tls-key");
  tls.caFile = argValue(args, "--tls-ca");
  tls.enabled = !tls.certificate.empty() && !tls.privateKey.empty();
  tls.requireClientCertificate = argValue(args, "--tls-require-client", "0") != "0";
  if (tls.enabled) configureTls(std::move(tls));

  Node node(std::move(cfg));
  Status s = node.start();
  if (!isOk(s)) {
    std::fprintf(stderr, "failed to start node: %s\n", std::string(statusName(s)).c_str());
    return 1;
  }
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  std::cout << "AegisFS node " << node.nodeId() << " listening on " << node.address() << std::endl;
  std::cout.flush();
  while (!gStop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::cout << "shutting down " << node.nodeId() << std::endl;
  node.stop();
  return 0;
}

void printInfo(const OpResult& r) {
  std::cout << "status=" << statusName(r.status) << "\n";
  for (const auto& kv : r.info.items()) {
    if (kv.first == "status") continue;
    std::cout << kv.first << "=" << kv.second << "\n";
  }
  if (!r.message.empty() && r.message.size() < 256) {
    // only print short textual messages, not binary payloads
    bool printable = true;
    for (char ch : r.message) if (static_cast<unsigned char>(ch) < 9) { printable = false; break; }
    if (printable) std::cout << "note=" << r.message << "\n";
  }
}

int runClient(const std::string& entry, std::vector<std::string> cmd) {
  if (cmd.empty()) { printHelp(); return 2; }
  std::string clientId = "cli-" + std::to_string(::getpid());
  Client cli(entry, clientId);
  const std::string& op = cmd[0];

  if (op == "help") { printHelp(); return 0; }
  if (op == "create" && cmd.size() >= 3) { auto r = cli.create(cmd[1], cmd[2]); printInfo(r); return exitCode(r.status); }
  if (op == "append" && cmd.size() >= 3) { auto r = cli.append(cmd[1], cmd[2]); printInfo(r); return exitCode(r.status); }
  if (op == "get" && cmd.size() >= 3) { auto r = cli.get(cmd[1], cmd[2]); printInfo(r); return exitCode(r.status); }
  if (op == "getQuorum" && cmd.size() >= 3) { auto r = cli.getQuorum(cmd[1], cmd[2]); printInfo(r); return exitCode(r.status); }
  if (op == "merge" && cmd.size() >= 2) { auto r = cli.merge(cmd[1]); printInfo(r); return exitCode(r.status); }
  if (op == "ls" && cmd.size() >= 2) { auto r = cli.ls(cmd[1]); printInfo(r); return exitCode(r.status); }
  if (op == "listStore") { auto r = cli.listStore(); printInfo(r); return exitCode(r.status); }
  if (op == "status") { auto r = cli.status(); printInfo(r); return exitCode(r.status); }
  if (op == "health") { auto r = cli.health(); printInfo(r); return exitCode(r.status); }
  if (op == "leave") { auto r = cli.leave(); printInfo(r); return exitCode(r.status); }
  if (op == "metrics") { auto r = cli.metrics(); printInfo(r); return exitCode(r.status); }
  if (op == "getFromReplica" && cmd.size() >= 4) {
    auto r = cli.getFromReplica(cmd[1], cmd[2], cmd[3]); printInfo(r); return exitCode(r.status);
  }
  if (op == "listMemIds") {
    auto r = cli.listMemIds();
    std::vector<Member> members;
    if (Membership::parse(r.message, members)) {
      std::sort(members.begin(), members.end(), [](const Member& a, const Member& b) {
        return ringId(a.nodeId) < ringId(b.nodeId);
      });
      for (const auto& m : members) {
        std::printf("%016llx  %-20s  %-14s  inc=%llu\n",
                    (unsigned long long)ringId(m.nodeId), m.nodeId.c_str(),
                    memberStateName(m.state).c_str(), (unsigned long long)m.incarnation);
      }
    }
    return exitCode(r.status);
  }
  if (op == "multiAppend" && cmd.size() >= 4) {
    std::string name = cmd[1];
    std::vector<std::pair<std::string, std::string>> jobs;  // (nodeAddr, localfile)
    for (size_t i = 2; i + 1 < cmd.size(); i += 2) jobs.emplace_back(cmd[i], cmd[i + 1]);
    std::vector<OpResult> results(jobs.size());
    std::vector<std::thread> threads;
    for (size_t i = 0; i < jobs.size(); ++i) {
      threads.emplace_back([&, i] {
        Client c(jobs[i].first, "mclient-" + std::to_string(i) + "-" + std::to_string(::getpid()));
        results[i] = c.append(jobs[i].second, name);
      });
    }
    for (auto& t : threads) t.join();
    int failures = 0;
    for (size_t i = 0; i < jobs.size(); ++i) {
      std::printf("append node=%s file=%s status=%s seq=%s\n", jobs[i].first.c_str(),
                  jobs[i].second.c_str(), std::string(statusName(results[i].status)).c_str(),
                  results[i].info.getOr("clientSeq", "-").c_str());
      if (!results[i].ok() && results[i].status != Status::Degraded) ++failures;
    }
    return failures == 0 ? 0 : 1;
  }
  std::fprintf(stderr, "unknown or malformed command: %s\n", op.c_str());
  printHelp();
  return 2;
}
}  // namespace

int runShell(const std::string& entry) {
  std::cout << "AegisFS shell: entry " << entry << " (type 'help', 'quit' to exit)\n";
  std::string line;
  while (true) {
    std::cout << "aegisfs> " << std::flush;
    if (!std::getline(std::cin, line)) break;
    if (line == "quit" || line == "exit") break;
    if (line.empty()) continue;
    std::vector<std::string> cmd;
    std::stringstream ss(line);
    std::string tok;
    while (ss >> tok) cmd.push_back(tok);
    if (cmd.empty()) continue;
    runClient(entry, cmd);
  }
  return 0;
}

int cliMain(int argc, char** argv) {
  std::vector<std::string> args(argv + 1, argv + argc);
  if (args.empty() || args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
    printHelp();
    return 0;
  }
  if (args[0] == "version" || args[0] == "--version" || args[0] == "-v") {
    std::cout << kBuildName << " " << kVersion << " (protocol v" << kProtocolVersion << ")\n";
    return 0;
  }
  if (args[0] == "serve") {
    return runServe(std::vector<std::string>(args.begin() + 1, args.end()));
  }
  // client mode: expect --entry host:port then a command
  TlsConfig tls;
  tls.caFile = argValue(args, "--tls-ca");
  tls.clientCertificate = argValue(args, "--tls-client-cert");
  tls.clientPrivateKey = argValue(args, "--tls-client-key");
  tls.enabled = !tls.caFile.empty() || (!tls.clientCertificate.empty() && !tls.clientPrivateKey.empty());
  if (tls.enabled) configureTls(std::move(tls));
  std::string entry = argValue(args, "--entry", "127.0.0.1:9100");
  if (args[0] == "shell") return runShell(entry);
  std::vector<std::string> cmd;
  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == "--entry") { ++i; continue; }
    cmd.push_back(args[i]);
  }
  return runClient(entry, cmd);
}

}  // namespace aegisfs
