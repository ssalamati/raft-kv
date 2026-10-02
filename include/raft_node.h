#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "raft_kv.grpc.pb.h"
#include "storage.h"

namespace raftkv {

// One member of the static cluster.
struct PeerInfo {
  int32_t id = 0;
  std::string addr;  // host:port; also what clients receive as `leader_hint`
};

struct RaftConfig {
  int32_t id = 0;                // This node's id; must appear in `peers`.
  std::vector<PeerInfo> peers;   // The WHOLE cluster, including this node.

  // Raft needs  heartbeat << election timeout  to keep a stable leader.
  std::chrono::milliseconds election_timeout_min{150};
  std::chrono::milliseconds election_timeout_max{300};
  std::chrono::milliseconds heartbeat_interval{50};
  std::chrono::milliseconds rpc_timeout{100};      // Per node-to-node RPC.
  std::chrono::milliseconds client_timeout{2000};  // How long a client call waits for a commit.
  size_t max_entries_per_rpc = 64;                 // Caps one AppendEntries batch.
};

enum class Role { kFollower, kCandidate, kLeader };
const char* RoleName(Role role);

// A Raft replica that also serves the client KV API.
//
// Concurrency model (deliberately simple):
//   * ONE mutex, `mu_`, guards every piece of Raft state below.
//   * Threads: one election-timer thread, plus one worker thread per remote
//     peer. A worker owns all outbound RPCs to its peer (vote requests while we
//     are a candidate, AppendEntries/heartbeats while we are the leader), so a
//     slow or dead peer can never delay the others.
//   * gRPC handler threads (RequestVote, AppendEntries, Get/Put/Delete) take
//     `mu_` for the whole handler.
//   * Network calls are made with `mu_` RELEASED; the state is re-validated
//     (role and term unchanged) after re-locking. Disk writes happen under
//     `mu_`, which guarantees "persist before replying".
//
// Register the SAME object with a grpc::ServerBuilder as both services:
//   builder.RegisterService(static_cast<RaftService::Service*>(&node));
//   builder.RegisterService(static_cast<KVService::Service*>(&node));
class RaftNode final : public RaftService::Service, public KVService::Service {
 public:
  RaftNode(RaftConfig config, Storage& storage);
  ~RaftNode();

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  void Start();  // Launches the background threads.
  void Stop();   // Stops and joins them. Idempotent; also called by the destructor.

  // Introspection (tests, logging).
  Role GetRole() const;
  uint64_t GetTerm() const;

  // ---- RaftService (node <-> node) ----
  grpc::Status RequestVote(grpc::ServerContext* context, const RequestVoteRequest* request,
                           RequestVoteResponse* response) override;
  grpc::Status AppendEntries(grpc::ServerContext* context, const AppendEntriesRequest* request,
                             AppendEntriesResponse* response) override;

  // ---- KVService (clients) ----
  grpc::Status Get(grpc::ServerContext* context, const GetRequest* request,
                   GetResponse* response) override;
  grpc::Status Put(grpc::ServerContext* context, const PutRequest* request,
                   PutResponse* response) override;
  grpc::Status Delete(grpc::ServerContext* context, const DeleteRequest* request,
                      DeleteResponse* response) override;

 private:
  using Clock = std::chrono::steady_clock;
  static constexpr int32_t kNoLeader = -1;

  // A remote node and the worker thread that talks to it.
  struct Peer {
    int32_t id = 0;
    std::string addr;
    std::unique_ptr<RaftService::Stub> stub;
    std::thread worker;
    bool wake = false;                 // Poke: act now instead of waiting for the heartbeat tick.
    uint64_t vote_requested_term = 0;  // Last term in which we asked this peer for a vote.
  };

  // ---- thread bodies ----
  void ElectionTimerLoop();
  void PeerLoop(Peer* peer);

  // ---- state transitions (caller holds mu_) ----
  void ResetElectionDeadlineLocked();
  void StartElectionLocked();
  void BecomeFollowerLocked(uint64_t term);
  void BecomeLeaderLocked();
  void WakeWorkersLocked();

  // ---- outbound RPCs: take the lock in, release it during the call, hold it again on return ----
  void RequestVoteFromPeer(std::unique_lock<std::mutex>& lock, Peer& peer);
  void ReplicateToPeer(std::unique_lock<std::mutex>& lock, Peer& peer);

  // ---- commit and apply (caller holds mu_) ----
  void AdvanceCommitIndexLocked();
  void ApplyCommittedLocked();

  // ---- client path ----
  // Appends `cmd` to the log (leader only) and blocks until it is committed and
  // applied. Returns KV_OK, KV_NOT_LEADER (with `leader_hint`), KV_TIMEOUT or KV_ERROR.
  KVStatus Propose(const Command& cmd, std::string* leader_hint);
  std::string LeaderHintLocked() const;

  size_t Quorum() const { return (peers_.size() + 1) / 2 + 1; }
  void Log(const std::string& message) const;

  // ---- immutable after construction ----
  const RaftConfig cfg_;
  Storage& storage_;
  std::unordered_map<int32_t, std::string> addr_by_id_;  // Every node, including self.
  std::vector<std::unique_ptr<Peer>> peers_;             // Every node EXCEPT self.

  // ---- guarded by mu_ ----
  mutable std::mutex mu_;
  std::condition_variable election_cv_;  // Election timer: deadline moved / role changed / stop.
  std::condition_variable worker_cv_;    // Peer workers: poked or stop.
  std::condition_variable applied_cv_;   // Clients: commit progressed / leadership lost / stop.
  bool started_ = false;
  bool stop_ = false;

  Role role_ = Role::kFollower;
  uint64_t current_term_ = 0;             // Persistent (via storage_).
  int32_t voted_for_ = Storage::kNoVote;  // Persistent (via storage_).
  uint64_t commit_index_ = 0;             // Highest index known to be committed.
  uint64_t last_applied_ = 0;             // Highest index applied to the KV state machine.
  int32_t leader_id_ = kNoLeader;         // Best-known leader, for client redirects.

  // Leader-only, reinitialised on every election win. Keyed by peer id.
  std::map<int32_t, uint64_t> next_index_;   // Next log index to send to the peer.
  std::map<int32_t, uint64_t> match_index_;  // Highest index known replicated on the peer.

  size_t votes_granted_ = 0;  // Candidate-only, for the current term.
  Clock::time_point election_deadline_;
  std::mt19937 rng_;

  std::thread election_thread_;
};

}  // namespace raftkv
