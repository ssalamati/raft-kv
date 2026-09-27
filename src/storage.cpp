#include "storage.h"

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include <cstdio>
#include <filesystem>
#include <limits>

namespace raftkv {
namespace {

constexpr char kKvPrefix[] = "kv_";
constexpr char kTermKey[] = "raft_current_term";
constexpr char kVoteKey[] = "raft_voted_for";
constexpr char kLogPrefix[] = "raft_log_";

void Check(const rocksdb::Status& s, const char* what) {
  if (!s.ok()) throw StorageError(std::string(what) + ": " + s.ToString());
}

// Raft state must hit disk before we answer an RPC.
rocksdb::WriteOptions SyncWrites() {
  rocksdb::WriteOptions w;
  w.sync = true;
  return w;
}

// Zero-padded to 20 digits (enough for UINT64_MAX) so that RocksDB's
// lexicographic key order matches numeric index order.
std::string LogKey(uint64_t index) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(index));
  return std::string(kLogPrefix) + buf;
}

std::optional<std::string> GetRaw(rocksdb::DB& db, const std::string& key) {
  std::string value;
  rocksdb::Status s = db.Get(rocksdb::ReadOptions(), key, &value);
  if (s.IsNotFound()) return std::nullopt;
  Check(s, "get");
  return value;
}

LogEntry ParseEntry(const rocksdb::Slice& bytes) {
  LogEntry entry;
  if (!entry.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
    throw StorageError("corrupt log entry");
  }
  return entry;
}

}  // namespace

Storage::Storage(const std::string& db_path) {
  std::error_code ec;
  std::filesystem::create_directories(db_path, ec);  // RocksDB only creates one level.
  if (ec) throw StorageError("create " + db_path + ": " + ec.message());

  rocksdb::Options options;
  options.create_if_missing = true;
  options.compression = rocksdb::kNoCompression;  // No optional codec dependencies.

  rocksdb::DB* raw = nullptr;
  Check(rocksdb::DB::Open(options, db_path, &raw), "open db");
  db_.reset(raw);
}

Storage::~Storage() = default;

// ---- Persistent Raft state --------------------------------------------------

uint64_t Storage::CurrentTerm() const {
  auto v = GetRaw(*db_, kTermKey);
  if (!v) return 0;
  try {
    return std::stoull(*v);
  } catch (const std::exception&) {
    throw StorageError("corrupt raft_current_term");
  }
}

int32_t Storage::VotedFor() const {
  auto v = GetRaw(*db_, kVoteKey);
  if (!v) return kNoVote;
  try {
    return std::stoi(*v);
  } catch (const std::exception&) {
    throw StorageError("corrupt raft_voted_for");
  }
}

void Storage::SaveTermAndVote(uint64_t term, int32_t voted_for) {
  rocksdb::WriteBatch batch;  // One batch => term and vote can never diverge on disk.
  batch.Put(kTermKey, std::to_string(term));
  batch.Put(kVoteKey, std::to_string(voted_for));
  Check(db_->Write(SyncWrites(), &batch), "save term/vote");
}

// ---- Raft log ---------------------------------------------------------------

LogPosition Storage::LastLog() const {
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  // LogKey(UINT64_MAX) sorts after every real log key, so this lands on the
  // last entry (or on a key outside the log prefix if the log is empty).
  it->SeekForPrev(LogKey(std::numeric_limits<uint64_t>::max()));
  Check(it->status(), "seek last log entry");
  if (!it->Valid() || !it->key().starts_with(kLogPrefix)) return {};
  LogEntry last = ParseEntry(it->value());
  return {last.index(), last.term()};
}

std::optional<LogEntry> Storage::GetEntry(uint64_t index) const {
  auto v = GetRaw(*db_, LogKey(index));
  if (!v) return std::nullopt;
  return ParseEntry(*v);
}

std::optional<uint64_t> Storage::TermAt(uint64_t index) const {
  if (index == 0) return 0;
  auto entry = GetEntry(index);
  if (!entry) return std::nullopt;
  return entry->term();
}

std::vector<LogEntry> Storage::GetEntries(uint64_t from_index, size_t max_entries) const {
  std::vector<LogEntry> out;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  for (it->Seek(LogKey(from_index));
       out.size() < max_entries && it->Valid() && it->key().starts_with(kLogPrefix);
       it->Next()) {
    out.push_back(ParseEntry(it->value()));
  }
  Check(it->status(), "scan log");
  return out;
}

void Storage::AppendLog(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return;
  std::lock_guard<std::mutex> lock(log_mu_);
  WriteSuffixLocked(LastLog().index + 1, entries);
}

void Storage::TruncateAndAppend(uint64_t from_index, const std::vector<LogEntry>& entries) {
  std::lock_guard<std::mutex> lock(log_mu_);
  WriteSuffixLocked(from_index, entries);
}

// Caller must hold log_mu_.
void Storage::WriteSuffixLocked(uint64_t from_index, const std::vector<LogEntry>& entries) {
  if (from_index == 0) throw std::invalid_argument("log indices start at 1");
  if (from_index > LastLog().index + 1) throw std::invalid_argument("write would leave a gap in the log");
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].index() != from_index + i) {
      throw std::invalid_argument("log entries must be contiguous starting at from_index");
    }
  }

  rocksdb::WriteBatch batch;
  {
    // Delete the old suffix. Deletes precede the puts in the batch, so an
    // entry that is rewritten at the same index ends up with its new value.
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(LogKey(from_index)); it->Valid() && it->key().starts_with(kLogPrefix); it->Next()) {
      batch.Delete(it->key());
    }
    Check(it->status(), "scan log suffix");
  }

  std::string bytes;
  for (const LogEntry& entry : entries) {
    if (!entry.SerializeToString(&bytes)) throw StorageError("serialize log entry");
    batch.Put(LogKey(entry.index()), bytes);
  }
  Check(db_->Write(SyncWrites(), &batch), "write log");
}

// ---- KV state machine -------------------------------------------------------

std::optional<std::string> Storage::Get(const std::string& key) const {
  return GetRaw(*db_, kKvPrefix + key);
}

void Storage::Put(const std::string& key, const std::string& value) {
  Check(db_->Put(rocksdb::WriteOptions(), kKvPrefix + key, value), "kv put");
}

void Storage::Delete(const std::string& key) {
  Check(db_->Delete(rocksdb::WriteOptions(), kKvPrefix + key), "kv delete");
}

}  // namespace raftkv
