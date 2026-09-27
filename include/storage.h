#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "raft_kv.pb.h"

namespace rocksdb {
class DB;
}

namespace raftkv {

// Thrown on any RocksDB failure or on-disk corruption. A Raft node that cannot
// persist its state must not answer the RPC that caused the write, so callers
// should let this propagate (or crash) rather than swallow it.
class StorageError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// (index, term) of the last log entry. {0, 0} means the log is empty.
struct LogPosition {
  uint64_t index = 0;
  uint64_t term = 0;
};

// One RocksDB instance holding two disjoint key spaces:
//
//   kv_<key>                  KV state machine data
//   raft_current_term         decimal uint64
//   raft_voted_for            decimal int32, kNoVote (-1) if none
//   raft_log_<20-digit index> serialized raftkv::LogEntry (1-based indices;
//                             zero-padded so key order == index order)
//
// Durability contract:
//   * Raft state (term, vote, log) is written with sync=true: when SaveTermAndVote /
//     AppendLog / TruncateAndAppend return, the data has been fsynced, so the
//     node may safely reply to the RPC.
//   * KV writes use sync=false. The log is the source of truth: after a crash
//     the committed log is re-applied, and Put/Delete replay is idempotent.
//
// Thread safety: every method may be called from any thread. Each call is
// atomic on its own (multi-key updates use a RocksDB WriteBatch). A sequence of
// calls (e.g. LastLog() then GetEntries()) is not atomic; the Raft node is
// expected to serialize those with its own mutex.
class Storage {
 public:
  static constexpr int32_t kNoVote = -1;

  // Opens (creating if needed) the database at `db_path`. Throws StorageError.
  explicit Storage(const std::string& db_path);
  ~Storage();

  Storage(const Storage&) = delete;
  Storage& operator=(const Storage&) = delete;

  // ---- Persistent Raft state -------------------------------------------------

  // 0 if never saved.
  uint64_t CurrentTerm() const;
  // kNoVote if never saved.
  int32_t VotedFor() const;
  // Atomically persists both values. Call whenever either changes, e.g. when
  // adopting a higher term (voted_for = kNoVote) or granting a vote.
  void SaveTermAndVote(uint64_t term, int32_t voted_for);

  // ---- Raft log (1-based; index 0 is the implicit empty-log sentinel) --------

  LogPosition LastLog() const;
  std::optional<LogEntry> GetEntry(uint64_t index) const;
  // Term of the entry at `index`; TermAt(0) == 0; nullopt if no such entry.
  std::optional<uint64_t> TermAt(uint64_t index) const;
  // Up to `max_entries` entries starting at `from_index`, in index order.
  std::vector<LogEntry> GetEntries(uint64_t from_index, size_t max_entries) const;

  // Appends entries whose indices must continue the log (LastLog().index + 1,
  // +2, ...). Throws std::invalid_argument otherwise.
  void AppendLog(const std::vector<LogEntry>& entries);

  // Atomically deletes every entry with index >= from_index and then writes
  // `entries`, whose indices must be from_index, from_index + 1, ... Used by
  // followers to overwrite a conflicting suffix. `from_index` may be at most
  // LastLog().index + 1. Throws std::invalid_argument on violations.
  void TruncateAndAppend(uint64_t from_index, const std::vector<LogEntry>& entries);

  // ---- KV state machine (apply committed commands here) ---------------------

  std::optional<std::string> Get(const std::string& key) const;
  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);  // Deleting a missing key is a no-op.

 private:
  void WriteSuffixLocked(uint64_t from_index, const std::vector<LogEntry>& entries);

  std::unique_ptr<rocksdb::DB> db_;
  std::mutex log_mu_;  // Serializes log read-modify-write operations.
};

}  // namespace raftkv
