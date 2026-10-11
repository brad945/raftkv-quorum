# raftkv-quorum: design

**Status:** 2026-10-11. The four forks were decided by Bradley (see `raftkv-quorum-DECISIONS.md`); the rest is Claude's design, open to his review. Scope comes from `project-brief-raft-kv-store.md` §4: small, deeply understood, chaos-tested.

## 1. Shape
```
crates (one Cargo workspace)
  raft-core   pure Raft: no I/O, no clock, no threads, no tokio
  kv          the state machine (get/put/delete + client sessions) and durable storage
  sim         deterministic simulator: N nodes, a fake network, simulated time, fault injection
  checker     linearizability checker for KV histories (hand-written)
  server      thin real runtime (R6): the `raftkv` binary, TCP + DiskStorage + a timer around raft-core
```

**Pure core (Bradley's call).** A `RaftNode` changes only through three inputs:
- `tick()`, one unit of simulated time;
- `step(message)`, a message from a peer;
- `propose(command)`, a client write.

Each call returns a `Ready`. Nothing inside the core ever touches a disk, a socket or a clock.

## 2. The contract every driver must keep: `Ready`
```
Ready {
  persist: { term?, voted_for?, entries to append (with a truncate point), snapshot? }
  messages: [to, Message]
  committed: [entries to apply to the state machine]
}
```
**The ordering rule:**
1. Make `persist` durable (write + fsync).
2. Only then send `messages`.
3. Then apply `committed`.

*Why:* a vote or an AppendEntries ack is a promise about durable state. If a node votes, sends the vote, crashes, and restarts having forgotten it, it can vote again in the same term. Two leaders in one term breaks Raft's core safety. The simulator enforces this: a crash drops everything not yet persisted.

## 3. Raft scope (Bradley's call: Figure 2 + snapshots)
**In:**
- leader election with randomized timeouts;
- AppendEntries log replication with the log-matching rule;
- commit by majority, **only for entries of the current term** (paper §5.4.2, Figure 8);
- the persistent state `currentTerm`, `votedFor` and `log`;
- InstallSnapshot plus log compaction;
- a no-op entry at the start of each term, so a new leader can commit earlier entries.

**Flow control (beyond the paper; see POSTMORTEM.md):** the leader keeps each follower in one of two states, as etcd does.
- **Probing:** where the follower's log matches is unknown. At most one append is in flight; the leader resends on the next heartbeat.
- **Replicating:** after the first successful append. `next_index` moves past each append as it is sent, so every entry goes out once.

A rejection carries the `prev_index` it rejected, so the leader ignores stale ones. `match_index` moves only on acks, so the commit rule is exactly Figure 2's.

**Snapshots are sent whole:** InstallSnapshot carries the entire snapshot in one message (the paper chunks it). That's fine for a small KV store, and listed in §9.

**Out, documented as out:**
- cluster membership changes;
- PreVote;
- leader leases and ReadIndex.

All reads go through the log, so they're linearizable without leases.

## 4. The state machine and client sessions (a fork beyond Figure 2; flagged)
Commands are `Put(k, v)`, `Get(k)` and `Delete(k)`. Each carries `(client_id, seq)`.

**Problem:** a client that times out and retries can apply the same write twice, and the second application can land *after* a later write, which breaks linearizability.

**Fix (Raft dissertation §6.3):** the state machine remembers each client's last `seq` and its reply. A duplicate returns the saved reply without re-applying. This is replicated state, so it's included in snapshots.

## 5. Durable storage (`kv` crate)
- **Hard state** (`term`, `voted_for`): written to a temp file, fsynced, renamed over the old file, then the directory is fsynced.
- **Log:** an append-only WAL. Each record is `[len u32][crc32 u32][payload]`.
  - On recovery: read records until the first one that is short or has a bad CRC, then truncate the file there (a torn tail from a crash).
  - A truncation from a Raft conflict is a separate record kind.
- **Snapshot:** written to a temp file, fsynced, then renamed.
- **macOS durability (measured 2026-10-11, this Mac, APFS SSD):** plain `fsync` on macOS doesn't flush the disk's own cache; `fcntl(F_FULLFSYNC)` does. `SyncMode::Full` calls F_FULLFSYNC explicitly.
  - **Measured cost per durable append:** none 0.03 ms, fsync 3.6 ms, full 3.5 ms (`cargo test -p kv measure_sync_cost -- --ignored --nocapture`).
  - **Why fsync and full cost the same:** most likely Rust's `sync_all` already issues F_FULLFSYNC on Apple platforms, so both reach the disk.
  - **What it means:** a durable write costs about 120× a non-durable one. That's why the node driver should batch a whole `Ready` into one sync, not sync per entry.

## 6. The simulator (`sim` crate): determinism is the point
- **Simulated time:** a discrete event queue ordered by `(time, sequence number)`.
- **Randomness:** one seeded RNG (ChaCha8). No `HashMap` iteration anywhere in the core or the simulator (`BTreeMap` only). No threads, no wall clock.
- **Faults:**
  - drop, delay or duplicate a message;
  - **reorder** messages;
  - partition the nodes into sets;
  - crash a node, losing everything not persisted;
  - restart it from its persisted state.
- **Test hooks:** force specific election timeouts (to build a split vote); script fault schedules (named scenarios).
- **The determinism test:** the same seed, run twice, must give byte-identical event traces.

## 7. The linearizability checker (`checker` crate)
**Input:** a history of operations, each with an invoke time, a return time (or "timed out") and a result.
- **Per key:** a KV store's linearizability decomposes by key, which keeps the search tractable.
- **The algorithm:** Wing & Gong / Porcupine-style. A depth-first search for a single sequential order that respects real-time order and the register's semantics, with memoization of `(linearized set, state)`.
- **Timed-out operations:** treated as "may have taken effect at any point after invocation, or never". Getting this wrong produces false violations.

**Testing the checker:**
- hand-written linearizable and non-linearizable histories;
- the chaos runs against a **deliberately broken Raft** (test-only flags: commit previous-term entries by counting replicas; truncate on every AppendEntries). Seeded random runs plus the checker must catch both.

## 8. Named tests (each catches a known way Raft implementations break)
| Test | What it proves | Milestone |
|---|---|---|
| `vote_is_durable_before_sent` | a node that crashes before persisting its vote can't vote twice in a term | R3 |
| `figure8_no_commit_of_old_term_by_count` | a leader commits only entries from its own term | R3 |
| `reordered_append_does_not_truncate` | a delayed or duplicated AppendEntries never deletes entries that match | R3 |
| `match_index_never_decreases_and_stale_response_ignored` | old replies can't move the leader backwards | R3 |
| `higher_term_in_response_steps_down` | the leader steps down on any higher term, including in replies | R3 |
| `split_vote_resolves` | forced equal timeouts split the vote; randomization resolves it | R3/R5 |
| `snapshot_offsets` / `old_snapshot_is_ignored` / `install_snapshot_sets_commit_and_applied_and_restores` / `snapshot_survives_crash` | snapshot indexing and durability | R4 |
| `wal_torn_tail_truncated` / `wal_corrupt_byte_detected` | crash-safe storage | R2 |
| `same_seed_same_trace` | the simulator is deterministic | R3 |
| `kill_mid_write`, `partition_minority_leader`, `split_vote` | the brief's three chaos scenarios | R5 |
| `random_faults_linearizable` (200 seeds; `long_sweep`: 5000) | random fault schedules never break safety or linearizability | R5 |
| `checks_catch_planted_bugs` | the checks can fail: planted bugs are caught | R5 |
| `figure8_correct_raft_stays_safe` / `figure8_planted_bug_breaks_state_machine_safety` | the paper's Figure 8, scripted message by message | R5 |
| `three_processes_survive_a_leader_kill_and_the_restarted_node_catches_up`, `a_whole_cluster_restart_keeps_every_acknowledged_write` | 3 real processes over TCP: kill -9, restart, catch-up via InstallSnapshot | R6 |
| `replication_traffic_is_linear_in_writes`, `a_replicating_peer_gets_each_new_entry_once`, `stale_rejections_are_ignored`, `a_probing_peer_has_one_append_in_flight` | the postmortem's bug stays fixed | R5 |

## 9. What this does NOT handle (for interviews: say it plainly)
- membership changes;
- PreVote, so a partitioned node rejoining can force an election;
- leases and ReadIndex, so reads cost a log round trip;
- sharding;
- Byzantine faults;
- real disk failure beyond torn writes;
- snapshot chunking (a snapshot travels as one message). A frame is capped at 64 MB, so that is also the largest snapshot a follower can catch up from;
- power-loss durability is not tested end to end. A `kill -9` keeps the OS page cache, so the process tests prove the runtime writes before it acks, not that it syncs. That rests on the storage design (fsync / `F_FULLFSYNC`, measured at about 3.6 ms per durable append) and on the storage tests that cut files at every byte;
- the TCP runtime has no authentication or TLS, and membership is static.

## 10. Postmortem
`POSTMORTEM.md` records the real bug: quadratic replication traffic. Every safety and linearizability check passed while it was there; it was found by reading a trace and then measuring. Planted bugs only prove the checker works; they're not the story.
