// raft_client - command-line client for raft-kv.
//
//   raft_client [--server=HOST:PORT[,HOST:PORT...]] put KEY VALUE
//   raft_client [--server=...]                      get KEY
//   raft_client [--server=...]                      delete KEY
//
// Any node can be contacted. A follower answers KV_NOT_LEADER plus the leader's
// address, and the client transparently retries there. Put and Delete are
// idempotent, so retrying after a timeout or a leadership change is safe.
//
// Options:
//   --server=LIST      node(s) to try first (default localhost:50051). Several
//                      may be given; the next one is used if one is unreachable.
//   --retries=N        total attempts (default 15, ~200ms apart, which spans an election)
//   --timeout_ms=N     deadline per RPC attempt (default 3000)
//   --hint_host=HOST   replace the host in leader hints, keeping the port. Needed
//                      when the cluster advertises names the client cannot resolve
//                      (e.g. Docker service names, from the host machine).
//
// Exit status: 0 success, 1 key not found, 2 usage error, 3 gave up.
#include <chrono>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "raft_kv.grpc.pb.h"

namespace {

using raftkv::KVService;
using raftkv::KVStatus;

struct Options {
  std::vector<std::string> servers{"localhost:50051"};
  int retries = 15;
  int timeout_ms = 3000;
  std::string hint_host;
  std::vector<std::string> command;  // e.g. {"put", "key", "value"}
};

void PrintUsage() {
  std::cerr << "Usage: raft_client [--server=HOST:PORT[,...]] [--retries=N] [--timeout_ms=N]\n"
               "                   [--hint_host=HOST] put KEY VALUE | get KEY | delete KEY\n";
}

std::vector<std::string> Split(const std::string& text, char sep) {
  std::vector<std::string> parts;
  size_t start = 0;
  while (start <= text.size()) {
    size_t end = text.find(sep, start);
    if (end == std::string::npos) end = text.size();
    if (end > start) parts.push_back(text.substr(start, end - start));
    start = end + 1;
  }
  return parts;
}

bool ParseArgs(int argc, char** argv, Options* opts, std::string* error) {
  bool only_positional = false;  // After "--", everything is a key or value.
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (only_positional || arg.rfind("--", 0) != 0) {
      opts->command.push_back(arg);
      continue;
    }
    if (arg == "--") {
      only_positional = true;
      continue;
    }
    std::string name = arg.substr(2);
    std::string value;
    const size_t eq = name.find('=');
    if (eq != std::string::npos) {
      value = name.substr(eq + 1);
      name.resize(eq);
    } else if (i + 1 < argc) {
      value = argv[++i];
    } else {
      *error = "missing value for --" + name;
      return false;
    }

    try {
      if (name == "server") {
        opts->servers = Split(value, ',');
        if (opts->servers.empty()) throw std::invalid_argument("empty");
      } else if (name == "retries") {
        opts->retries = std::stoi(value);
        if (opts->retries < 1) throw std::invalid_argument("range");
      } else if (name == "timeout_ms") {
        opts->timeout_ms = std::stoi(value);
        if (opts->timeout_ms < 1) throw std::invalid_argument("range");
      } else if (name == "hint_host") {
        opts->hint_host = value;
      } else {
        *error = "unknown flag --" + name;
        return false;
      }
    } catch (const std::exception&) {
      *error = "invalid value '" + value + "' for --" + name;
      return false;
    }
  }

  const std::vector<std::string>& cmd = opts->command;
  const size_t want = cmd.empty() ? 0 : (cmd[0] == "put" ? 3 : (cmd[0] == "get" || cmd[0] == "delete") ? 2 : 0);
  if (want == 0 || cmd.size() != want) {
    *error = "expected one of: put KEY VALUE | get KEY | delete KEY";
    return false;
  }
  return true;
}

struct Outcome {
  KVStatus status = raftkv::KV_ERROR;
  std::string leader_hint;
  std::string value;
};

// Performs one RPC against `stub` and fills `Outcome`.
using Op = std::function<grpc::Status(KVService::Stub&, grpc::ClientContext&, Outcome*)>;

// "node2:50052" -> "localhost:50052" when host == "localhost".
std::string RewriteHint(const std::string& hint, const std::string& host) {
  const size_t colon = hint.rfind(':');
  if (host.empty() || colon == std::string::npos) return hint;
  return host + hint.substr(colon);
}

void Pause() { std::this_thread::sleep_for(std::chrono::milliseconds(200)); }

// Runs `op` until it gets a definitive answer: follows leader hints, rotates
// through --server when a node is unreachable, and retries on timeouts.
// Returns false once the attempts are used up.
bool Execute(const Options& opts, const Op& op, Outcome* out) {
  std::string target = opts.servers[0];
  size_t next_server = 1;

  for (int attempt = 1; attempt <= opts.retries; ++attempt) {
    auto stub = KVService::NewStub(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(opts.timeout_ms));

    const grpc::Status rpc = op(*stub, ctx, out);
    if (!rpc.ok()) {
      std::cerr << "cannot reach " << target << ": " << rpc.error_message() << '\n';
      target = opts.servers[next_server++ % opts.servers.size()];
      Pause();
      continue;
    }

    switch (out->status) {
      case raftkv::KV_NOT_LEADER:
        if (out->leader_hint.empty()) {
          std::cerr << target << " is not the leader and no leader is known yet; retrying\n";
          Pause();
        } else {
          target = RewriteHint(out->leader_hint, opts.hint_host);
          std::cerr << "not the leader; following hint to " << target << '\n';
        }
        break;
      case raftkv::KV_TIMEOUT:
        std::cerr << target << " could not reach a quorum in time; retrying\n";
        break;
      default:
        return true;  // OK, NOT_FOUND or ERROR: a definitive answer.
    }
  }
  return false;
}

}  // namespace

int main(int argc, char** argv) {
  Options opts;
  std::string error;
  if (!ParseArgs(argc, argv, &opts, &error)) {
    std::cerr << "error: " << error << "\n\n";
    PrintUsage();
    return 2;
  }

  const std::string& cmd = opts.command[0];
  const std::string& key = opts.command[1];
  Op op;
  if (cmd == "put") {
    const std::string& value = opts.command[2];
    op = [&](KVService::Stub& stub, grpc::ClientContext& ctx, Outcome* out) {
      raftkv::PutRequest req;
      req.set_key(key);
      req.set_value(value);
      raftkv::PutResponse resp;
      const grpc::Status rpc = stub.Put(&ctx, req, &resp);
      out->status = resp.status();
      out->leader_hint = resp.leader_hint();
      return rpc;
    };
  } else if (cmd == "get") {
    op = [&](KVService::Stub& stub, grpc::ClientContext& ctx, Outcome* out) {
      raftkv::GetRequest req;
      req.set_key(key);
      raftkv::GetResponse resp;
      const grpc::Status rpc = stub.Get(&ctx, req, &resp);
      out->status = resp.status();
      out->leader_hint = resp.leader_hint();
      out->value = resp.value();
      return rpc;
    };
  } else {
    op = [&](KVService::Stub& stub, grpc::ClientContext& ctx, Outcome* out) {
      raftkv::DeleteRequest req;
      req.set_key(key);
      raftkv::DeleteResponse resp;
      const grpc::Status rpc = stub.Delete(&ctx, req, &resp);
      out->status = resp.status();
      out->leader_hint = resp.leader_hint();
      return rpc;
    };
  }

  Outcome out;
  if (!Execute(opts, op, &out)) {
    std::cerr << "error: gave up after " << opts.retries << " attempts (cluster unreachable, no leader, or no quorum)\n";
    return 3;
  }
  switch (out.status) {
    case raftkv::KV_OK:
      if (cmd == "get") {
        std::cout << out.value << '\n';
      } else {
        std::cout << "OK\n";
      }
      return 0;
    case raftkv::KV_NOT_FOUND:
      std::cerr << "(not found)\n";
      return 1;
    default:
      std::cerr << "error: the server reported an internal error\n";
      return 3;
  }
}
