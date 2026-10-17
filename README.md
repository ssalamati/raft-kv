# raft-kv

A small, readable distributed key-value store built on the Raft consensus
algorithm, with gRPC for transport and RocksDB for storage. C++17, about 1,000
lines of code, written to be read end to end and explained in a systems-design
discussion.

It implements the core of Raft from Ongaro & Ousterhout's paper (leader
election and log replication) and deliberately omits snapshotting and dynamic
cluster membership. See [Design Decisions & Scope](#design-decisions--scope).

## Architecture

```
                            +-------------------+
                            |    raft_client    |
                            +-------------------+
                                      |
                                      |  gRPC: KVService.Put / Get / Delete
                                      |  (follower replies with leader_hint)
                                      v
  +--------------------------------------------------------------------------+
  |  node1  (LEADER)                                                         |
  |                                                                          |
  |  +--------------------------------------+       +----------------------+ |
  |  | RaftNode                             |       | Storage              | |
  |  | KVService + RaftService handlers     | fsync | RocksDB (1 instance) | |
  |  | election timer thread                | ----->| raft_* + kv_* keys   | |
  |  | 1 replication worker per peer        |       +----------------------+ |
  |  +--------------------------------------+                                |
  |                                                                          |
  +----------------+-------------------------------------+-------------------+
                   |                                     |
                   |   gRPC: RaftService.AppendEntries   |
                   |   (heartbeats + log replication;    |
                   |    RequestVote during elections)    |
                   v                                     v
  +----------------------------------+  +----------------------------------+
  | node2  (FOLLOWER)                |  | node3  (FOLLOWER)                |
  |                                  |  |                                  |
  | RaftNode --> Storage --> RocksDB |  | RaftNode --> Storage --> RocksDB |
  | (log fsync'd before each ack)    |  | (log fsync'd before each ack)    |
  +----------------------------------+  +----------------------------------+
```

Each node runs the same binary and hosts both gRPC services on one port:
`KVService` for clients and `RaftService` for the other nodes. The cluster is a
static list of peers passed on the command line.

**Life of a write** (`Put` / `Delete`):

1. The client sends the request to any node. A follower replies `KV_NOT_LEADER`
   with the leader's address, and the client retries there.
2. The leader appends the command to its log and fsyncs it to RocksDB.
3. One replication worker per follower sends `AppendEntries`. The follower
   checks that its log matches, overwrites any conflicting suffix, fsyncs, and
   acknowledges.
4. When a majority holds the entry (and it is from the leader's current term),
   the leader advances `commitIndex`, applies the command to the `kv_` state
   machine in RocksDB, and replies `KV_OK`.
5. Followers learn the new `commitIndex` from the next `AppendEntries` or
   heartbeat and apply the entry themselves.

A `Get` takes the same path with a no-op entry as a barrier, then reads the
state machine (see [Consistency](#consistency-and-failure-semantics)).

**Threads and locking.** All Raft state sits behind a single `std::mutex`.
Each node runs one election-timer thread and one worker thread per remote peer;
a worker owns every outbound RPC to its peer (vote requests while the node is a
candidate, `AppendEntries` and heartbeats while it is the leader), so a slow or
dead peer never delays the others. Network calls are made with the lock
released, and state is re-validated (role and term unchanged) when the lock is
re-taken.

## Key Features

- **Leader election with randomized timeouts.** A follower that hears nothing
  for a random 150-300 ms becomes a candidate, increments its term, persists
  its self-vote, and requests votes. A node grants at most one vote per term and
  only to a candidate whose log is at least as up to date as its own, and
  randomization keeps split votes rare. A new leader immediately commits a no-op
  from its own term so it can safely commit entries from earlier terms.
- **Log replication.** `AppendEntries` carries the standard consistency check
  (`prev_log_index` / `prev_log_term`). Followers overwrite conflicting suffixes
  and report a `conflict_index` so the leader can back up a whole term at a time
  instead of one entry per round trip. The leader only advances `commitIndex`
  over entries from its current term.
- **RocksDB persistence for both the Raft WAL and the KV state machine.**
  `currentTerm`, `votedFor`, and every log entry are fsynced before the node
  replies to the RPC that caused them. The KV data lives in the same database.
  Nodes recover their term, vote, and log from disk on restart.
- **Automatic client leader redirection.** Responses carry a `leader_hint`. The
  bundled client follows it, rotates through several `--server` addresses when
  one is unreachable, and tolerates a stale hint during an election.
- **Linearizable reads.** `Get` commits a no-op barrier through the log before
  reading, so a deposed leader cannot serve stale data.
- **Small enough to explain.** One mutex, two services, one proto file.

## Quickstart

### Prerequisites

Docker with the Compose plugin (`docker compose`).

### Start a 3-node cluster

```bash
docker compose up --build -d
docker compose ps          # node1, node2, node3
```

The nodes reach each other by service name on the compose network and listen on
ports 50051, 50052, and 50053 (also published to the host).

### Read and write

The `client` service runs `raft_client` inside the cluster network:

```bash
docker compose run --rm client put mykey myval     # OK
docker compose run --rm client get mykey           # myval
docker compose run --rm client delete mykey        # OK
docker compose run --rm client get mykey           # (not found), exit status 1
```

If the client first reaches a follower, it prints a line such as
`not the leader; following hint to node2:50052` on stderr and carries on.

### Fault-tolerance walkthrough

```bash
# 1. Write a key.
docker compose run --rm client put mykey myval

# 2. Find the leader: the node with the most recent "became leader" line
#    (and no later "stepping down" line from itself).
docker compose logs --no-color | grep -E "became leader|stepping down"

# 3. Kill the leader. Assuming it is node2:
docker compose stop node2          # equivalent: docker stop raft-kv-node2-1

# 4. Read the key from a surviving node. A new leader is normally elected
#    within a few hundred milliseconds; the client retries for several seconds.
docker compose run --rm client --server=node1:50051,node3:50053 get mykey   # myval

# 5. The cluster still accepts writes with 2 of 3 nodes up.
docker compose run --rm client --server=node1:50051,node3:50053 put second value

# 6. Bring the old leader back. It reloads its term, vote, and log from its
#    volume, rejoins as a follower, and receives the entries it missed.
docker compose start node2
```

Container names default to `<project>-<service>-1`, and the project name
defaults to the directory name, so the `docker stop` form above assumes the
repository directory is called `raft-kv`.

To see what losing the majority looks like, stop two nodes and try a write:

```bash
docker compose stop node1 node2
docker compose run --rm client --server=node3:50053 --retries=3 put third value
echo $?     # 3: no quorum, so nothing can commit and the client gives up
docker compose start node1 node2
```

Clean up (including the data volumes) with `docker compose down -v`.

### Using the client from the host

Leader hints contain compose service names (`node2:50052`), which the host cannot
resolve. `--hint_host` keeps the port and swaps in a host name the client can use:

```bash
./build/raft_client --server=localhost:50051 --hint_host=localhost put mykey myval
```

### Building and running without Docker

Tested on Ubuntu 24.04:

```bash
sudo apt-get install -y g++ cmake ninja-build pkg-config \
    libprotobuf-dev protobuf-compiler libgrpc++-dev protobuf-compiler-grpc librocksdb-dev
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build

PEERS=1:127.0.0.1:50051,2:127.0.0.1:50052,3:127.0.0.1:50053
./build/raft_server --id=1 --port=50051 --db_path=/tmp/raft/n1 --peers=$PEERS &
./build/raft_server --id=2 --port=50052 --db_path=/tmp/raft/n2 --peers=$PEERS &
./build/raft_server --id=3 --port=50053 --db_path=/tmp/raft/n3 --peers=$PEERS &

./build/raft_client --server=127.0.0.1:50051 put hello world
./build/raft_client --server=127.0.0.1:50052 get hello
```

`SIGINT` and `SIGTERM` shut a node down cleanly.

## Reference

### `raft_server`

| Flag | Meaning |
|---|---|
| `--id` | This node's id, a positive integer that must appear in `--peers`. |
| `--port` | Port to listen on (all interfaces); serves Raft and client RPCs. |
| `--db_path` | Directory for the RocksDB data; created if missing. |
| `--peers` | The whole cluster, this node included, as `id:host:port,...`. Every node gets the same list. The `host:port` is also what clients receive as `leader_hint`. |

### `raft_client`

```
raft_client [--server=HOST:PORT[,...]] [--retries=N] [--timeout_ms=N]
            [--hint_host=HOST]  put KEY VALUE | get KEY | delete KEY
```

| Flag | Default | Meaning |
|---|---|---|
| `--server` | `localhost:50051` | Node(s) to try first. The next one is used when one is unreachable. |
| `--retries` | 15 | Total attempts, about 200 ms apart on redirects and failures. |
| `--timeout_ms` | 3000 | Deadline for each RPC attempt. |
| `--hint_host` | none | Replace the host in leader hints, keeping the port. |

Exit status: `0` success, `1` key not found, `2` usage error, `3` gave up.
Use `--` before a key or value that starts with dashes.

### Timing parameters

These are defaults in `RaftConfig` (`include/raft_node.h`); they are not exposed as
flags.

| Parameter | Default |
|---|---|
| Election timeout | 150-300 ms, randomized per node |
| Heartbeat interval | 50 ms |
| Node-to-node RPC deadline | 100 ms |
| Client request wait for commit | 2 s |
| Max entries per `AppendEntries` | 64 |

## Consistency and failure semantics

Writes are acknowledged only after a majority has fsynced the entry and the
leader has applied it. Reads go through the leader and are linearizable.

| Status | Meaning | What the client does |
|---|---|---|
| `KV_OK` | Committed and applied. | Done. |
| `KV_NOT_FOUND` | `Get` on a missing key. (`Delete` of a missing key is `KV_OK`.) | Exits with status 1. |
| `KV_NOT_LEADER` | Not the leader, or leadership was lost mid-request. `leader_hint` is empty during an election. | Follows the hint, or waits and retries. |
| `KV_TIMEOUT` | No commit within 2 s. For a write, the outcome is unknown: the entry may still commit later. | Retries. |
| `KV_ERROR` | Internal error. | Exits with status 3. |

| Failure | Effect |
|---|---|
| Follower crashes | None for clients. It catches up from the leader on restart. |
| Leader crashes | A new election follows within a few hundred milliseconds. In-flight requests receive `KV_NOT_LEADER` or `KV_TIMEOUT` and are retried. |
| Minority of nodes lost | Cluster stays available. |
| Majority of nodes lost | Reads and writes cannot commit; clients time out. No data is lost, and service resumes when a quorum returns. |

There is no request deduplication. A retry after `KV_TIMEOUT` or a leader change
can apply a command twice. For `Put` and `Delete` the final state of that key is
the same, but a duplicate can land after a concurrent writer's update. Exactly-once
semantics would need client session ids, which are out of scope.

## Design Decisions & Scope

### One RocksDB instance, two key spaces

Both the persistent Raft state and the KV state machine live in a single RocksDB
database per node, separated by key prefix:

| Key | Value | Write durability |
|---|---|---|
| `raft_current_term` | decimal `uint64` | fsync, in one batch with the vote |
| `raft_voted_for` | decimal `int32` (`-1` = none) | fsync, in one batch with the term |
| `raft_log_<20-digit index>` | serialized `LogEntry` (term, index, command) | fsync, one batch per append or overwrite |
| `kv_<user key>` | user value | no fsync |

Why a single instance:

- **Atomic multi-key updates.** `currentTerm` and `votedFor` are written in one
  `WriteBatch`, so a crash can never leave a new term paired with an old vote.
  Truncating a conflicting log suffix and appending the leader's entries is one
  batch as well. The same property would let you store `lastApplied` atomically
  with an applied KV write later (not implemented).
- **One durability domain.** One directory (one volume in Docker), one open
  handle, one recovery path, one thing to back up.
- **Easy to explain and inspect.** The prefixes make the two roles obvious, and
  user keys can never collide with Raft state because every KV key is stored
  under `kv_`. Log indices are zero-padded to 20 digits so RocksDB's lexicographic
  order equals numeric order, which makes "last entry", "entries from N", and
  "truncate from N" simple iterator scans.
- **Durability policy per key space.** Raft state must be on disk before the node
  replies, so those writes use `sync=true`. KV writes skip the fsync because the
  log is the source of truth: after a crash the committed log is re-applied, and
  replaying `Put`/`Delete` in order is idempotent.

The cost: log traffic and KV traffic share one write-ahead log and one
compaction pipeline, they cannot be tuned independently, and the state machine
cannot be dropped and rebuilt without touching Raft state. Larger systems often
separate them. Here `Storage` is the only class that touches RocksDB, so splitting
into two instances or column families would be a change local to `storage.cpp`.

### Why no snapshotting

Without snapshots the log is never compacted. It grows without bound, a restarted
node re-applies the whole log to its state machine, and a node that falls far
behind is caught up entry by entry from index 1. Adding snapshots means a
state-machine snapshot with a last-included index and term, log truncation that
changes how indices map to storage, an `InstallSnapshot` RPC with chunked
transfer for followers behind the truncation point, and care that snapshotting
does not race with applying. It is a large, mostly orthogonal feature that would
roughly double the Raft code without changing the ideas worth explaining:
election, replication, and the commit rule.

### Why no dynamic membership

The cluster is the static `--peers` list, and the majority size is derived from
it. Safe membership change (single-server changes or joint consensus, section 6 of
the paper) puts configurations into the log and changes quorum rules during the
transition, because two different majorities must never exist at once. Changing
the list on a live cluster, or by restarting nodes with a different list, can
break that guarantee and lose committed data. Treat the peer list as fixed for the
lifetime of the data directories.

### Other decisions

- **One mutex, per-peer workers.** Easy to reason about and to explain; the
  trade-off is that disk writes happen under the lock, which limits throughput.
- **Reads through the log.** A committed no-op barrier proves the leader is still
  the leader, with no extra protocol. It costs one log entry per read; ReadIndex
  or leader leases are the standard optimizations.
- **Leader no-op on election.** Required to commit entries from earlier terms
  (paper, sections 5.4.2 and 8).
- **Idempotent writes plus client retries** instead of server-side session
  tracking.

## Limitations

- No snapshots or log compaction; no membership changes (above).
- No CheckQuorum or PreVote. A leader cut off from the majority keeps believing
  it leads and its clients see timeouts, and a rejoining node with an inflated
  term can force an unnecessary election.
- Each `Get` appends a log entry.
- Persistence errors are thrown as exceptions and not handled specially. A failed
  write inside an RPC handler could leave in-memory state ahead of disk, whereas a
  production node should abort on any persistence error.
- gRPC runs without TLS or authentication.
- Timing parameters are compile-time defaults.
- Tested on Ubuntu 24.04 only.

## Repository layout

```
raft-kv/
  CMakeLists.txt         core library + raft_server + raft_client
  Dockerfile             Ubuntu 24.04 image containing both binaries
  docker-compose.yml     3-node cluster + on-demand client service
  proto/raft_kv.proto    RaftService and KVService wire protocol
  include/
    storage.h            RocksDB wrapper: term/vote/log + KV state machine
    raft_node.h          RaftNode: roles, election, replication, KV handlers
  src/
    storage.cpp
    raft_node.cpp
    server.cpp           raft_server: flag parsing, gRPC server, signal handling
    client.cpp           raft_client: put/get/delete with leader redirection
```

## References

Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus
Algorithm (Extended Version)*, https://raft.github.io/raft.pdf
