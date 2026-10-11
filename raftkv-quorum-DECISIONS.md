# Decisions Log — raftkv-quorum

Every non-trivial design choice gets an entry below. Format:

```
## [YYYY-MM-DD] Short title of the decision
**Made by:** [bradley] or [claude]
**Decision:** what was decided
**Why:** the reasoning
**Alternative(s) considered:** what else was on the table, and why it lost
**Status:** settled / revisit later
```

## [2026-10-11] Replication flow control: etcd's probe/replicate states, plus `rejected` in AppendEntriesResp
**Made by:** [claude]
**Decision:** each follower is either *probing* (one append in flight; resend on heartbeat) or *replicating* (`next_index` moves past each append as it's sent). A rejection carries the `prev_index` it rejected, and stale rejections are ignored. `match_index` still moves only on acks, so the commit rule is unchanged.
**Why:** this fixes the postmortem's bug, where traffic grew as O(n²) in writes (52 appends per write per follower at 300 writes; now 1.6). The `rejected` field matters because without it, a burst of rejections after one dropped message would each resend the whole range.
**Alternative(s) considered:**
- **Optimistic `next_index` alone, with no probe state:** under steady writes, "accept a rejection only for `next-1`" never fires, because `next` keeps moving. That's a livelock.
- **Rate-limit sends per tick:** it hides the symptom, and the duplicates are still there.
- **Leave it, since it's "only performance":** no. A Raft that floods the network under load isn't one I'd defend.
**Status:** settled

## [2026-10-11] Figure 8 is a scripted test, not left to random chaos
**Made by:** [claude]
**Decision:** the Figure 8 bug (committing an old-term entry by counting replicas) is tested by a message-by-message script of the paper's sequence (`crates/sim/tests/figure8.rs`). It is not expected from random runs.
**Why:** 500 random seeds with the bug planted caught it 0 times, even with one-entry batches. A new leader's no-op travels with the old entries, so a majority almost never holds an old-term entry without a current-term one. The script proves both directions: correct Raft keeps commit < 2 and every node applies the same entry at index 2; the planted bug commits it and two nodes apply different entries there.
**Alternative(s) considered:**
- **Tune the random fault rates until it shows up:** that's slow, fragile, and a passing sweep would still prove nothing.
- **Unit test only:** `node_tests` already has one, but it doesn't show the end-to-end safety violation.
**Status:** settled

## [2026-10-11] The simulator can crash a node between computing and persisting
**Made by:** [claude]
**Decision:** `crash_mid_step` crashes a node at random points inside one driver step: after the core computed its `Ready` but before the disk write, or after the write but before sending. Named test `kill_mid_write`, and on in every chaos run at 500 ppm.
**Why:** crashes landed only between whole steps at first. In 200 seeds, the planted "send before persist" bug was byte-identical to correct Raft: the window it opens was never hit. With mid-step crashes it fails 1–16 of 300 seeds (more at higher rates), and correct Raft still fails 0.
**Alternative(s) considered:**
- **Crash only between steps:** that was the original model; it was too coarse to test durability ordering.
**Status:** settled

Newest entries at the top. This file is interview prep as much as it is documentation — write entries assuming you'll need to defend them cold, months from now, with no other context.

Reserve a dedicated section (or a separate `POSTMORTEM.md`) for the chaos-testing bug writeup once you hit it — that entry matters more than any other single piece of documentation in this repo.

---

## [2026-10-11] Client sessions: dedup retried commands (client id + sequence number)
**Made by:** [claude], flagged for Bradley
**Decision:** every command carries `(client_id, seq)`. The state machine keeps each client's last `seq` and its reply, and returns the saved reply for a duplicate instead of applying it again. This is replicated state, so it's included in snapshots.
**Why:** Figure 2 doesn't cover client retries. A client that times out and retries can apply a write twice, and the second copy can land *after* a later write. That's a real linearizability violation, and the checker would (correctly) fail it. Without dedup the chaos results would be noise from our own client, not Raft.
**Alternative(s) considered:**
- **Never retry:** that isn't realistic, and timeouts are exactly what chaos produces.
- **Idempotent commands only:** an overwrite is still not safe across reordering.
**Status:** settled unless Bradley objects (Raft dissertation §6.3)

## [2026-10-11] Chaos testing: named scenarios + seeded random runs + a linearizability checker
**Made by:** [bradley] (the recommended option)
**Decision:** the brief's 3 scenarios (kill mid-write, partition, split vote) as named tests, plus thousands of seeded random fault schedules. Every run's client history is checked by a hand-written linearizability checker. A failing seed replays exactly.
**Why:** random runs are what usually surface the real bug the postmortem needs. A checker turns "it seemed fine" into proof that every history could have happened on one correct machine.
**Alternative(s) considered:**
- **Named scenarios only:** they miss what nobody thought to script.
- **Manual runs:** not defensible as "tested".
**Status:** settled

## [2026-10-11] Raft scope: paper Figure 2 + snapshots; durable state; reads through the log
**Made by:** [bradley] (the recommended option)
**Decision:**
- **In:** election, replication, commit rules and safety exactly as in the paper's Figure 2, plus InstallSnapshot and compaction.
- **Durable:** term, vote and log are made durable with fsync before any message that depends on them.
- **Reads:** they go through the log.
- **Out, documented:** membership changes, PreVote, leases and ReadIndex.
**Why:** "kill a node mid-write" only means something if the node restarts with what it promised. The extensions are the scope creep the brief warns against.
**Alternative(s) considered:**
- **Simplified, in-memory Raft:** it makes crash tests meaningless.
- **Extensions:** more to claim, less understood.
**Status:** settled

## [2026-10-11] Architecture: a pure Raft core driven by a deterministic simulator (+ a thin TCP mode)
**Made by:** [bradley] (the recommended option)
**Decision:** Raft is a pure state machine: ticks, messages and proposals go in, and a `Ready` (what to persist, what to send, what to apply) comes out. Tests run the nodes in one process over a simulated network with seeded faults. A small TCP runtime runs 3 real processes for demos.
**Why:**
- **Every failure replays exactly from its seed,** so the hardest-bug story becomes reproducible instead of "it happened once".
- **The pure core keeps I/O out of the logic,** which is where the bugs hide.
- **Precedent:** FoundationDB and TigerBeetle test this way.
**Alternative(s) considered:**
- **Real processes only, or Docker:** both are more "real", but timing-dependent and hard to reproduce.
**Status:** settled

