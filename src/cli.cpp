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
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }

std::vector<std::string> split_csv(const std::string& s) {
  std::vector<std::string> out;
  std::string cur;
  std::stringstream ss(s);
  while (std::getline(ss, cur, ',')) {
    if (!cur.empty()) out.push_back(cur);
  }
  return out;
}

std::string arg_value(const std::vector<std::string>& args, const std::string& key,
                      const std::string& def = "") {
  for (size_t i = 0; i + 1 < args.size(); ++i) {
    if (args[i] == key) return args[i + 1];
  }
  return def;
}

int exit_code(Status s) { return is_ok(s) ? 0 : (static_cast<int>(s) == 0 ? 1 : static_cast<int>(s)); }

void print_help() {
  std::cout <<
      "AegisFS — fault-tolerant distributed file system\n\n"
      "Run a node:\n"
      "  aegisfs serve --data <dir> [--host 127.0.0.1] [--port N] [--id NAME]\n"
      "                [--seeds host:port,host:port] [--rf 3]\n\n"
      "Client commands (prefix with --entry host:port):\n"
      "  create <localfile> <name>         upload a new file (first-create-only)\n"
      "  get <name> <localfile>            download (read-your-writes)\n"
      "  append <localfile> <name>         append bytes to an existing file\n"
      "  merge <name>                      reconcile all replicas to identical bytes\n"
      "  ls <name>                         show file id + designated replicas\n"
      "  liststore                         list files stored on the entry node\n"
      "  getfromreplica <addr> <name> <localfile>   pure replica read (no repair)\n"
      "  list_mem_ids                      list members by ring id + status\n"
      "  multiappend <name> <node> <file> [<node> <file> ...]  concurrent appends\n"
      "  status | health                   node status / liveness\n"
      "  help                              this message\n";
}

int run_serve(const std::vector<std::string>& args) {
  NodeConfig cfg;
  cfg.data_dir = arg_value(args, "--data", "./data");
  cfg.host = arg_value(args, "--host", "127.0.0.1");
  cfg.port = std::atoi(arg_value(args, "--port", "0").c_str());
  cfg.node_id = arg_value(args, "--id", "");
  cfg.rf = std::atoi(arg_value(args, "--rf", "3").c_str());
  cfg.parallel_replication = arg_value(args, "--parallel", "1") != "0";
  cfg.seeds = split_csv(arg_value(args, "--seeds", ""));

  Node node(std::move(cfg));
  Status s = node.start();
  if (!is_ok(s)) {
    std::fprintf(stderr, "failed to start node: %s\n", std::string(status_name(s)).c_str());
    return 1;
  }
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::cout << "AegisFS node " << node.node_id() << " listening on " << node.address() << std::endl;
  std::cout.flush();
  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::cout << "shutting down " << node.node_id() << std::endl;
  node.stop();
  return 0;
}

void print_info(const OpResult& r) {
  std::cout << "status=" << status_name(r.status) << "\n";
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

int run_client(const std::string& entry, std::vector<std::string> cmd) {
  if (cmd.empty()) { print_help(); return 2; }
  std::string client_id = "cli-" + std::to_string(::getpid());
  Client cli(entry, client_id);
  const std::string& op = cmd[0];

  if (op == "help") { print_help(); return 0; }
  if (op == "create" && cmd.size() >= 3) { auto r = cli.create(cmd[1], cmd[2]); print_info(r); return exit_code(r.status); }
  if (op == "append" && cmd.size() >= 3) { auto r = cli.append(cmd[1], cmd[2]); print_info(r); return exit_code(r.status); }
  if (op == "get" && cmd.size() >= 3) { auto r = cli.get(cmd[1], cmd[2]); print_info(r); return exit_code(r.status); }
  if (op == "merge" && cmd.size() >= 2) { auto r = cli.merge(cmd[1]); print_info(r); return exit_code(r.status); }
  if (op == "ls" && cmd.size() >= 2) { auto r = cli.ls(cmd[1]); print_info(r); return exit_code(r.status); }
  if (op == "liststore") { auto r = cli.liststore(); print_info(r); return exit_code(r.status); }
  if (op == "status") { auto r = cli.status(); print_info(r); return exit_code(r.status); }
  if (op == "health") { auto r = cli.health(); print_info(r); return exit_code(r.status); }
  if (op == "getfromreplica" && cmd.size() >= 4) {
    auto r = cli.get_from_replica(cmd[1], cmd[2], cmd[3]); print_info(r); return exit_code(r.status);
  }
  if (op == "list_mem_ids") {
    auto r = cli.list_mem_ids();
    std::vector<Member> members;
    if (Membership::parse(r.message, members)) {
      std::sort(members.begin(), members.end(), [](const Member& a, const Member& b) {
        return ring_id(a.node_id) < ring_id(b.node_id);
      });
      for (const auto& m : members) {
        std::printf("%016llx  %-20s  %-14s  inc=%llu\n",
                    (unsigned long long)ring_id(m.node_id), m.node_id.c_str(),
                    member_state_name(m.state).c_str(), (unsigned long long)m.incarnation);
      }
    }
    return exit_code(r.status);
  }
  if (op == "multiappend" && cmd.size() >= 4) {
    std::string name = cmd[1];
    std::vector<std::pair<std::string, std::string>> jobs;  // (node_addr, localfile)
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
                  jobs[i].second.c_str(), std::string(status_name(results[i].status)).c_str(),
                  results[i].info.get_or("client_seq", "-").c_str());
      if (!results[i].ok() && results[i].status != Status::Degraded) ++failures;
    }
    return failures == 0 ? 0 : 1;
  }
  std::fprintf(stderr, "unknown or malformed command: %s\n", op.c_str());
  print_help();
  return 2;
}
}  // namespace

int cli_main(int argc, char** argv) {
  std::vector<std::string> args(argv + 1, argv + argc);
  if (args.empty() || args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
    print_help();
    return 0;
  }
  if (args[0] == "version" || args[0] == "--version" || args[0] == "-v") {
    std::cout << kBuildName << " " << kVersion << " (protocol v" << kProtocolVersion << ")\n";
    return 0;
  }
  if (args[0] == "serve") {
    return run_serve(std::vector<std::string>(args.begin() + 1, args.end()));
  }
  // client mode: expect --entry host:port then a command
  std::string entry = arg_value(args, "--entry", "127.0.0.1:9100");
  std::vector<std::string> cmd;
  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == "--entry") { ++i; continue; }
    cmd.push_back(args[i]);
  }
  return run_client(entry, cmd);
}

}  // namespace aegisfs
