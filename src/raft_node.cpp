#include "raft_node.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <stdexcept>

namespace raftkv {

const char* RoleName(Role role) {
  switch (role) {
    case Role::kFollower:  return "follower";
    case Role::kCandidate: return "candidate";
    case Role::kLeader:    return "leader";
  }
  return "unknown";
}

// ============================================================================
// Lifecycle
// ============================================================================

RaftNode::RaftNode(RaftConfig config, Storage& storage)
    : cfg_(std::move(config)),
      storage_(storage),
      rng_(std::random_device{}() + static_cast<unsigned>(cfg_.id)) {
  bool found_self = false;
  for (const PeerInfo& info : cfg_.peers) {
    addr_by_id_[info.id] = info.addr;
    if (info.id == cfg_.id) {
      found_self = true;
      continue;
    }
    auto peer = std::make_unique<Peer>();
    peer->id = info.id;
    peer->addr = info.addr;

    // Reconnect quickly so a peer that starts late is noticed within ~100ms.
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 100);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
    peer->stub = RaftService::NewStub(
        grpc::CreateCustomChannel(info.addr, grpc::InsecureChannelCredentials(), args));
    peers_.push_back(std::move(peer));
  }
  if (!found_self) throw std::invalid_argument("peer list must include this node's own id");

  // Recover persistent state. Volatile state (commit_index_, last_applied_)
  // restarts at 0 and is rebuilt from the leader / by replaying the log.
  current_term_ = storage_.CurrentTerm();
  voted_for_ = storage_.VotedFor();
  ResetElectionDeadlineLocked();  // No other thread exists yet, so no lock is needed.
}

RaftNode::~RaftNode() { Stop(); }

void RaftNode::Start() {
  std::lock_guard<std::mutex> lock(mu_);
  if (started_) return;
  started_ = true;
  ResetElectionDeadlineLocked();
  election_thread_ = std::thread(&RaftNode::ElectionTimerLoop, this);
  for (auto& peer : peers_) peer->worker = std::thread(&RaftNode::PeerLoop, this, peer.get());
}

void RaftNode::Stop() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
  }
  election_cv_.notify_all();
  worker_cv_.notify_all();
  applied_cv_.notify_all();
  if (election_thread_.joinable()) election_thread_.join();
  for (auto& peer : peers_) {
    if (peer->worker.joinable()) peer->worker.join();
  }
}

Role RaftNode::GetRole() const {
  std::lock_guard<std::mutex> lock(mu_);
  return role_;
}

uint64_t RaftNode::GetTerm() const {
  std::lock_guard<std::mutex> lock(mu_);
  return current_term_;
}

void RaftNode::Log(const std::string& message) const {
  std::cerr << "[node " << cfg_.id << " term " << current_term_ << "] " << message << '\n';
}

// ============================================================================
// Background threads
// ============================================================================

// Fires an election when the randomized deadline passes without us hearing from
// a leader (or granting a vote). Leaders have no election timer.
void RaftNode::ElectionTimerLoop() {
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_) {
    if (role_ == Role::kLeader) {
      election_cv_.wait(lock);  // Woken when we step down or stop.
      continue;
    }
    if (Clock::now() >= election_deadline_) {
      StartElectionLocked();
      continue;
    }
    // The deadline may be pushed back while we sleep; we simply re-check on wake-up.
    election_cv_.wait_until(lock, election_deadline_);
  }
}

// One worker per remote peer. Wakes every heartbeat interval (or sooner when
// poked) and does whatever the current role requires toward that peer.
void RaftNode::PeerLoop(Peer* peer) {
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_) {
    worker_cv_.wait_for(lock, cfg_.heartbeat_interval, [&] { return stop_ || peer->wake; });
    if (stop_) break;
    peer->wake = false;

    if (role_ == Role::kLeader) {
      ReplicateToPeer(lock, *peer);  // Heartbeat if there is nothing new to send.
    } else if (role_ == Role::kCandidate && peer->vote_requested_term != current_term_) {
      RequestVoteFromPeer(lock, *peer);
    }
  }
}

// ============================================================================
// Role transitions (mu_ held)
// ============================================================================

void RaftNode::ResetElectionDeadlineLocked() {
  std::uniform_int_distribution<std::chrono::milliseconds::rep> dist(
      cfg_.election_timeout_min.count(), cfg_.election_timeout_max.count());
  election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng_));
}

void RaftNode::WakeWorkersLocked() {
  for (auto& peer : peers_) peer->wake = true;
  worker_cv_.notify_all();
}

// Raft rules for all servers (§5.1): a newer term always demotes us to follower
// and clears our vote. Term and vote are persisted together before we act on them.
void RaftNode::BecomeFollowerLocked(uint64_t term) {
  if (term > current_term_) {
    current_term_ = term;
    voted_for_ = Storage::kNoVote;
    leader_id_ = kNoLeader;
    storage_.SaveTermAndVote(current_term_, voted_for_);
  }
  if (role_ != Role::kFollower) {
    Log(std::string("stepping down from ") + RoleName(role_));
    role_ = Role::kFollower;
    leader_id_ = kNoLeader;
    ResetElectionDeadlineLocked();  // Leaders' deadlines are stale; start a fresh timeout.
    election_cv_.notify_all();
    applied_cv_.notify_all();       // Fail in-flight client requests promptly.
  }
}

// Election timeout: increment the term, vote for ourselves, ask everyone else (§5.2).
void RaftNode::StartElectionLocked() {
  role_ = Role::kCandidate;
  ++current_term_;
  voted_for_ = cfg_.id;
  leader_id_ = kNoLeader;
  votes_granted_ = 1;  // Our own vote.
  storage_.SaveTermAndVote(current_term_, voted_for_);  // Durable before any RequestVote leaves.
  ResetElectionDeadlineLocked();
  Log("election timeout: became candidate");

  if (votes_granted_ >= Quorum()) {  // Single-node cluster.
    BecomeLeaderLocked();
    return;
  }
  WakeWorkersLocked();  // Workers send the RequestVote RPCs.
}

void RaftNode::BecomeLeaderLocked() {
  role_ = Role::kLeader;
  leader_id_ = cfg_.id;
  const uint64_t last_index = storage_.LastLog().index;
  for (auto& peer : peers_) {
    next_index_[peer->id] = last_index + 1;  // Optimistic: assume followers are caught up.
    match_index_[peer->id] = 0;              // Pessimistic: nothing confirmed yet.
  }

  // A fresh leader cannot commit entries from earlier terms directly (§5.4.2);
  // it commits a NOOP from its own term, which commits everything before it.
  LogEntry noop;
  noop.set_term(current_term_);
  noop.set_index(last_index + 1);
  noop.mutable_command()->set_type(Command::NOOP);
  storage_.AppendLog({noop});

  Log("won election: became leader");
  AdvanceCommitIndexLocked();  // Matters only for a single-node cluster.
  WakeWorkersLocked();         // Announce leadership immediately.
}

// ============================================================================
// Outbound RPCs (run on peer worker threads)
// ============================================================================

void RaftNode::RequestVoteFromPeer(std::unique_lock<std::mutex>& lock, Peer& peer) {
  const LogPosition last = storage_.LastLog();
  RequestVoteRequest req;
  req.set_term(current_term_);
  req.set_candidate_id(static_cast<uint32_t>(cfg_.id));
  req.set_last_log_index(last.index);
  req.set_last_log_term(last.term);
  peer.vote_requested_term = current_term_;  // Ask at most once per term.

  lock.unlock();  // ---- never hold mu_ across the network ----
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + cfg_.rpc_timeout);
  RequestVoteResponse resp;
  const grpc::Status status = peer.stub->RequestVote(&ctx, req, &resp);
  lock.lock();

  if (stop_ || !status.ok()) return;  // Unreachable peer: no vote from it this term.
  if (resp.term() > current_term_) {
    BecomeFollowerLocked(resp.term());
    return;
  }
  // Ignore the reply if the world moved on while we were waiting.
  if (role_ != Role::kCandidate || current_term_ != req.term()) return;
  if (resp.vote_granted() && ++votes_granted_ >= Quorum()) BecomeLeaderLocked();
}

void RaftNode::ReplicateToPeer(std::unique_lock<std::mutex>& lock, Peer& peer) {
  const uint64_t term = current_term_;
  const uint64_t next = next_index_[peer.id];

  AppendEntriesRequest req;
  req.set_term(term);
  req.set_leader_id(static_cast<uint32_t>(cfg_.id));
  req.set_prev_log_index(next - 1);
  req.set_prev_log_term(storage_.TermAt(next - 1).value_or(0));
  req.set_leader_commit(commit_index_);
  for (LogEntry& entry : storage_.GetEntries(next, cfg_.max_entries_per_rpc)) {
    *req.add_entries() = std::move(entry);  // Empty batch => plain heartbeat.
  }

  lock.unlock();  // ---- never hold mu_ across the network ----
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + cfg_.rpc_timeout);
  AppendEntriesResponse resp;
  const grpc::Status status = peer.stub->AppendEntries(&ctx, req, &resp);
  lock.lock();

  if (stop_ || !status.ok()) return;  // Peer unreachable: retry on the next heartbeat tick.
  if (resp.term() > current_term_) {
    BecomeFollowerLocked(resp.term());
    return;
  }
  if (role_ != Role::kLeader || current_term_ != term) return;  // Stale reply.

  if (resp.success()) {
    // The follower now matches us through the last entry we sent. max() keeps
    // match_index_ monotonic even if replies are reordered.
    const uint64_t matched = req.prev_log_index() + static_cast<uint64_t>(req.entries_size());
    match_index_[peer.id] = std::max(match_index_[peer.id], matched);
    next_index_[peer.id] = match_index_[peer.id] + 1;
    AdvanceCommitIndexLocked();
    if (next_index_[peer.id] <= storage_.LastLog().index) peer.wake = true;  // Still behind: keep going.
    return;
  }

  // Log mismatch: back up. The follower's conflict_index lets us skip a whole
  // term at once; without a hint we step back by one. We sent prev_log_index =
  // next - 1, so the new next_index must be <= that.
  const uint64_t prev = req.prev_log_index();
  if (prev == 0) return;  // Index 0 always matches; nothing further to back up to.
  const uint64_t hint = resp.conflict_index();
  next_index_[peer.id] = std::max<uint64_t>(1, hint == 0 ? prev : std::min(hint, prev));
  peer.wake = true;  // Retry right away instead of waiting a heartbeat.
}

// ============================================================================
// Commit and apply (mu_ held)
// ============================================================================

// Leader rule (§5.3, §5.4.2): commit_index_ moves to the highest N replicated on
// a majority, but only if entry N is from the CURRENT term. The quorum-th largest
// of everyone's match index (our own being the end of our log) is that N.
void RaftNode::AdvanceCommitIndexLocked() {
  if (role_ != Role::kLeader) return;

  std::vector<uint64_t> matches{storage_.LastLog().index};
  for (const auto& [id, match] : match_index_) matches.push_back(match);
  std::sort(matches.begin(), matches.end(), std::greater<uint64_t>());

  const uint64_t n = matches[Quorum() - 1];
  if (n > commit_index_ && storage_.TermAt(n) == current_term_) {
    commit_index_ = n;
    ApplyCommittedLocked();
  }
}

// Applies every committed-but-unapplied entry to the KV state machine, in order,
// then wakes any client waiting for its entry.
void RaftNode::ApplyCommittedLocked() {
  if (last_applied_ >= commit_index_) return;

  const uint64_t count = commit_index_ - last_applied_;
  const std::vector<LogEntry> entries = storage_.GetEntries(last_applied_ + 1, count);
  if (entries.size() != count) throw std::logic_error("committed entries missing from the log");

  for (const LogEntry& entry : entries) {
    const Command& cmd = entry.command();
    switch (cmd.type()) {
      case Command::PUT:    storage_.Put(cmd.key(), cmd.value()); break;
      case Command::DELETE: storage_.Delete(cmd.key()); break;
      default:              break;  // NOOP (and unknown types) change nothing.
    }
    last_applied_ = entry.index();
  }
  applied_cv_.notify_all();
}

// ============================================================================
// RaftService handlers
// ============================================================================

grpc::Status RaftNode::RequestVote(grpc::ServerContext*, const RequestVoteRequest* req,
                                   RequestVoteResponse* resp) {
  std::lock_guard<std::mutex> lock(mu_);
  resp->set_vote_granted(false);

  if (req->term() < current_term_) {  // Stale candidate.
    resp->set_term(current_term_);
    return grpc::Status::OK;
  }
  if (req->term() > current_term_) BecomeFollowerLocked(req->term());

  // Election restriction (§5.4.1): only vote for a candidate whose log is at
  // least as up-to-date as ours (later last term wins; equal terms: longer log wins).
  const LogPosition last = storage_.LastLog();
  const bool log_ok = req->last_log_term() > last.term ||
                      (req->last_log_term() == last.term && req->last_log_index() >= last.index);
  const int32_t candidate = static_cast<int32_t>(req->candidate_id());

  // One vote per term, and a repeated request from the same candidate is fine.
  if (log_ok && (voted_for_ == Storage::kNoVote || voted_for_ == candidate)) {
    if (voted_for_ != candidate) {
      voted_for_ = candidate;
      storage_.SaveTermAndVote(current_term_, voted_for_);  // Durable before we reply "yes".
    }
    resp->set_vote_granted(true);
    ResetElectionDeadlineLocked();  // Granting a vote counts as hearing from a viable leader.
  }
  resp->set_term(current_term_);
  return grpc::Status::OK;
}

grpc::Status RaftNode::AppendEntries(grpc::ServerContext*, const AppendEntriesRequest* req,
                                     AppendEntriesResponse* resp) {
  std::lock_guard<std::mutex> lock(mu_);
  resp->set_success(false);

  if (req->term() < current_term_) {  // Deposed leader.
    resp->set_term(current_term_);
    return grpc::Status::OK;
  }

  // req->term() >= ours: this is the legitimate leader. Candidates and stale
  // leaders of the same or older term fall in line.
  BecomeFollowerLocked(req->term());
  leader_id_ = static_cast<int32_t>(req->leader_id());
  ResetElectionDeadlineLocked();
  resp->set_term(current_term_);

  // Consistency check (§5.3): we must already hold the entry that precedes the new ones.
  const LogPosition last = storage_.LastLog();
  if (req->prev_log_index() > last.index) {
    resp->set_conflict_index(last.index + 1);  // Our log is too short.
    return grpc::Status::OK;
  }
  const uint64_t prev_term = storage_.TermAt(req->prev_log_index()).value_or(0);
  if (prev_term != req->prev_log_term()) {
    // Report the first index of the conflicting term so the leader can skip it
    // wholesale. Committed entries never conflict, so never look below them.
    uint64_t first = req->prev_log_index();
    while (first > commit_index_ + 1 && storage_.TermAt(first - 1) == prev_term) --first;
    resp->set_conflict_index(first);
    return grpc::Status::OK;
  }

  // Skip entries we already have; at the first mismatch (or the first entry past
  // our log) drop our conflicting suffix and take the leader's entries (§5.3).
  // A reordered or duplicate RPC therefore never truncates anything it matches.
  const int total = req->entries_size();
  int i = 0;
  while (i < total && storage_.TermAt(req->entries(i).index()) == req->entries(i).term()) ++i;
  if (i < total) {
    std::vector<LogEntry> fresh;
    for (int k = i; k < total; ++k) fresh.push_back(req->entries(k));
    storage_.TruncateAndAppend(fresh.front().index(), fresh);  // Durable before we reply.
  }

  // Follow the leader's commit point, bounded by the last entry it just verified for us.
  if (req->leader_commit() > commit_index_) {
    const uint64_t last_new = req->prev_log_index() + static_cast<uint64_t>(total);
    commit_index_ = std::max(commit_index_, std::min(req->leader_commit(), last_new));
    ApplyCommittedLocked();
  }
  resp->set_success(true);
  return grpc::Status::OK;
}

// ============================================================================
// KVService handlers
// ============================================================================

std::string RaftNode::LeaderHintLocked() const {
  const auto it = addr_by_id_.find(leader_id_);
  return it == addr_by_id_.end() ? std::string() : it->second;
}

KVStatus RaftNode::Propose(const Command& cmd, std::string* leader_hint) {
  std::unique_lock<std::mutex> lock(mu_);
  if (role_ != Role::kLeader) {
    *leader_hint = LeaderHintLocked();
    return KV_NOT_LEADER;
  }

  // 1. Append to our own log (durable), remember where it landed and in which term.
  const uint64_t term = current_term_;
  LogEntry entry;
  entry.set_term(term);
  entry.set_index(storage_.LastLog().index + 1);
  *entry.mutable_command() = cmd;
  storage_.AppendLog({entry});
  const uint64_t index = entry.index();

  // 2. Replicate: poke the workers; a one-node cluster commits right here.
  AdvanceCommitIndexLocked();
  WakeWorkersLocked();

  // 3. Wait until the entry is applied, or we can no longer expect it to be.
  applied_cv_.wait_for(lock, cfg_.client_timeout, [&] {
    return stop_ || last_applied_ >= index || role_ != Role::kLeader || current_term_ != term;
  });

  if (last_applied_ >= index) {
    // Log Matching: if the entry at `index` still carries our term, it is ours.
    // Otherwise a newer leader overwrote it and our command was never committed.
    if (storage_.TermAt(index) == term) return KV_OK;
    *leader_hint = LeaderHintLocked();
    return KV_NOT_LEADER;
  }
  if (stop_) return KV_ERROR;
  if (role_ != Role::kLeader || current_term_ != term) {
    // We lost leadership with the entry in flight; it may or may not survive.
    // Put/Delete are idempotent, so the client can safely retry at the new leader.
    *leader_hint = LeaderHintLocked();
    return KV_NOT_LEADER;
  }
  return KV_TIMEOUT;  // No quorum reachable within client_timeout; outcome unknown.
}

grpc::Status RaftNode::Put(grpc::ServerContext*, const PutRequest* req, PutResponse* resp) {
  Command cmd;
  cmd.set_type(Command::PUT);
  cmd.set_key(req->key());
  cmd.set_value(req->value());
  std::string hint;
  resp->set_status(Propose(cmd, &hint));
  resp->set_leader_hint(hint);
  return grpc::Status::OK;
}

grpc::Status RaftNode::Delete(grpc::ServerContext*, const DeleteRequest* req, DeleteResponse* resp) {
  Command cmd;
  cmd.set_type(Command::DELETE);
  cmd.set_key(req->key());
  std::string hint;
  resp->set_status(Propose(cmd, &hint));
  resp->set_leader_hint(hint);
  return grpc::Status::OK;
}

// Linearizable read without a ReadIndex protocol: commit a NOOP as a barrier.
// Its commit proves we were still the leader after the request arrived, and
// everything committed before it is already applied, so reading the state
// machine now cannot be stale. (Cost: one log entry per read.)
grpc::Status RaftNode::Get(grpc::ServerContext*, const GetRequest* req, GetResponse* resp) {
  Command barrier;
  barrier.set_type(Command::NOOP);
  std::string hint;
  const KVStatus status = Propose(barrier, &hint);
  resp->set_status(status);
  resp->set_leader_hint(hint);
  if (status != KV_OK) return grpc::Status::OK;

  const std::optional<std::string> value = storage_.Get(req->key());
  if (value) {
    resp->set_value(*value);
  } else {
    resp->set_status(KV_NOT_FOUND);
  }
  return grpc::Status::OK;
}

}  // namespace raftkv
