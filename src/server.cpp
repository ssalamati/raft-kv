// raft_server - one node of a raft-kv cluster.
//
//   raft_server --id=1 --port=50051 --db_path=/data
//               --peers=1:node1:50051,2:node2:50052,3:node3:50053
//
// --peers lists the WHOLE cluster as id:host:port, this node included. Every
// node is started with the same list; only --id (and --port/--db_path) differ.
// The host:port given for a node is also what clients receive as `leader_hint`.
#include <pthread.h>
#include <signal.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "raft_node.h"
#include "storage.h"

namespace {

using raftkv::PeerInfo;

struct Options {
  int32_t id = 0;
  int port = 0;
  std::string db_path;
  std::vector<PeerInfo> peers;
};

void PrintUsage(const char* prog) {
  std::cerr << "Usage: " << prog << " --id=N --port=PORT --db_path=DIR --peers=ID:HOST:PORT[,...]\n"
            << "  --id       this node's id (positive integer, must appear in --peers)\n"
            << "  --port     port to listen on (all interfaces); serves Raft and client RPCs\n"
            << "  --db_path  directory for the RocksDB data (created if missing)\n"
            << "  --peers    the whole cluster, e.g. 1:node1:50051,2:node2:50052,3:node3:50053\n";
}

bool ParseInt(const std::string& text, int* out) {
  try {
    size_t used = 0;
    const int value = std::stoi(text, &used);
    if (used != text.size()) return false;
    *out = value;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

// "1:node1:50051,2:node2:50052" -> {{1, "node1:50051"}, {2, "node2:50052"}}
bool ParsePeers(const std::string& spec, std::vector<PeerInfo>* peers, std::string* error) {
  std::set<int32_t> seen;
  size_t start = 0;
  while (start <= spec.size()) {
    size_t end = spec.find(',', start);
    if (end == std::string::npos) end = spec.size();
    const std::string entry = spec.substr(start, end - start);
    start = end + 1;

    const size_t first = entry.find(':');
    const size_t last = entry.rfind(':');
    int id = 0;
    if (first == std::string::npos || first == last || last == first + 1 || last + 1 == entry.size() ||
        !ParseInt(entry.substr(0, first), &id) || id <= 0) {
      *error = "bad --peers entry '" + entry + "' (expected ID:HOST:PORT)";
      return false;
    }
    if (!seen.insert(id).second) {
      *error = "duplicate node id " + std::to_string(id) + " in --peers";
      return false;
    }
    peers->push_back({id, entry.substr(first + 1)});
  }
  return true;
}

bool ParseArgs(int argc, char** argv, Options* opts, std::string* error) {
  std::string peers_spec;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg.rfind("--", 0) != 0) {
      *error = "unexpected argument '" + arg + "'";
      return false;
    }
    std::string name = arg.substr(2);
    std::string value;
    const size_t eq = name.find('=');
    if (eq != std::string::npos) {  // --name=value
      value = name.substr(eq + 1);
      name.resize(eq);
    } else if (i + 1 < argc) {      // --name value
      value = argv[++i];
    } else {
      *error = "missing value for --" + name;
      return false;
    }

    int number = 0;
    if (name == "id") {
      if (!ParseInt(value, &number) || number <= 0) {
        *error = "--id must be a positive integer";
        return false;
      }
      opts->id = number;
    } else if (name == "port") {
      if (!ParseInt(value, &number) || number < 1 || number > 65535) {
        *error = "--port must be between 1 and 65535";
        return false;
      }
      opts->port = number;
    } else if (name == "db_path") {
      opts->db_path = value;
    } else if (name == "peers") {
      peers_spec = value;
    } else {
      *error = "unknown flag --" + name;
      return false;
    }
  }

  if (opts->id == 0) { *error = "--id is required"; return false; }
  if (opts->port == 0) { *error = "--port is required"; return false; }
  if (opts->db_path.empty()) { *error = "--db_path is required"; return false; }
  if (peers_spec.empty()) { *error = "--peers is required"; return false; }
  if (!ParsePeers(peers_spec, &opts->peers, error)) return false;

  const bool has_self = std::any_of(opts->peers.begin(), opts->peers.end(),
                                    [&](const PeerInfo& p) { return p.id == opts->id; });
  if (!has_self) {
    *error = "--peers must include this node's own id (" + std::to_string(opts->id) + ")";
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return 0;
    }
  }

  Options opts;
  std::string error;
  if (!ParseArgs(argc, argv, &opts, &error)) {
    std::cerr << "error: " << error << "\n\n";
    PrintUsage(argv[0]);
    return 2;
  }

  // Block SIGINT/SIGTERM before any thread exists so that every thread inherits
  // the mask; the main thread then collects them synchronously with sigwait().
  // (`docker stop` sends SIGTERM, Ctrl-C sends SIGINT.)
  sigset_t shutdown_signals;
  sigemptyset(&shutdown_signals);
  sigaddset(&shutdown_signals, SIGINT);
  sigaddset(&shutdown_signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &shutdown_signals, nullptr);

  try {
    // Declaration order matters: destruction runs in reverse, so the gRPC
    // server stops first, then the node, then the database closes.
    raftkv::Storage storage(opts.db_path);

    raftkv::RaftConfig config;
    config.id = opts.id;
    config.peers = opts.peers;
    raftkv::RaftNode node(config, storage);

    grpc::ServerBuilder builder;
    int bound_port = 0;
    builder.AddListeningPort("0.0.0.0:" + std::to_string(opts.port), grpc::InsecureServerCredentials(),
                             &bound_port);
    builder.RegisterService(static_cast<raftkv::RaftService::Service*>(&node));
    builder.RegisterService(static_cast<raftkv::KVService::Service*>(&node));
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server || bound_port == 0) {
      std::cerr << "fatal: cannot listen on port " << opts.port << '\n';
      return 1;
    }

    node.Start();
    std::cerr << "raft_server: node " << opts.id << " listening on port " << opts.port << ", cluster of "
              << opts.peers.size() << ", data in " << opts.db_path << '\n';

    int signal_number = 0;
    sigwait(&shutdown_signals, &signal_number);
    std::cerr << "raft_server: node " << opts.id << " caught signal " << signal_number << ", shutting down\n";

    node.Stop();  // Stop the Raft threads first; this also releases clients blocked on a commit.
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << '\n';
    return 1;
  }
  return 0;
}
