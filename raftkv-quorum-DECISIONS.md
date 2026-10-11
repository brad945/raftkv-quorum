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

