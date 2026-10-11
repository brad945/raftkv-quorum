# Postmortem: replication traffic grew with the square of the write rate

**Status:** fixed in R5 (flow control, `crates/raft-core/src/node.rs`).
**Severity:** performance and bandwidth. No safety or linearizability violation, ever. Every check passed the whole time, and that is the lesson.

## Summary
The leader re-sent every unacknowledged entry on every send. Each new write, and each ack that left a backlog, started another full copy of the unacked range. Copies already in flight kept generating more copies. In a fault-free run with one write per tick, a follower received **52 AppendEntries per write at 300 writes**, and the number kept growing with the write count: 19 per write at 100 writes, 35 at 200. Total traffic was O(n²).

A second symptom had the same root: the leader re-sent the *whole snapshot* on every heartbeat and every ack until it was acknowledged. That made **342 snapshot transfers in a fault-free run** where about 20 make sense.

## How it was found
It wasn't found by a check. The 5,000-seed chaos sweep was green: 1.09M client operations, 49k crashes, 15k partitions, 0 safety violations, 0 non-linearizable histories.

It was found by reading a trace for an unrelated reason. `partition_minority_leader` failed on seed 4. That turned out to be my test's fault: random heals undid the scripted partition. In that trace, I saw the leader deliver the same `InstallSnapshot` to node 2 **four times in one tick**, to a follower that was healthy and only slightly behind. That's not wrong, but it's not right either, so I measured instead of guessing.

| writes (fault-free, delay 1–6 ticks) | 100 | 200 | 300 |
|---|---|---|---|
| AppendEntries delivered (all 4 followers) | 7,587 | 28,267 | 62,269 |
| per write per follower | 19.0 | 35.3 | 51.9 |

Going from 100 to 200 writes multiplied traffic by 3.7×, and from 200 to 300 by 2.2×; n² predicts 4× and 2.25×. It's quadratic.

One more clue: with 5% message drops, traffic was *lower* (12 per write per follower). Dropping messages killed some of the duplicate streams.

## Root cause
`next_index[peer]` advanced only when an ack arrived. So:

1. A new write called `broadcast_append`. That sent `entries[next_index..]`, which is every entry the peer hadn't acked yet, not just the new one.
2. A successful ack with `next_index <= last_index` sent `entries[next_index..]` again, even though copies of exactly those entries were still in flight.
3. Each in-flight copy came back as an ack, and each ack triggered step 2. The number of in-flight streams per follower grew by one per write and never shrank.

The snapshot path had the same shape. `send_append` sent the snapshot whenever `next_index <= snapshot_index`, and nothing moved `next_index` until the snapshot was acked.

Figure 2 doesn't say when to send. It defines `nextIndex` and the rules for retrying, and a literal reading ("if last log index ≥ nextIndex, send entries starting at nextIndex") produces this. Production Raft implementations add flow control on top; I hadn't.

## The fix
I used etcd's two states per follower:

- **Probing.** The leader doesn't know where the follower's log matches its own. It keeps one append in flight, and further sends are paused until a reply or the next heartbeat. A heartbeat always goes out, so a lost reply can't stall the follower forever. Every follower starts here when a leader is elected, and a follower goes back to probing after a real rejection or a snapshot send.
- **Replicating.** After the first successful append, `next_index` moves past each append *as it is sent*. Each entry goes out once.

Rejections now carry `rejected`, the `prev_index` of the append they reject. Without it, the fix has its own failure mode. After one dropped append, every later in-flight append is rejected, and acting on each rejection would back up and resend the same range once per rejection. A rejection is stale when:
- **replicating:** `rejected <= match_index`, because we already know the follower holds that;
- **probing:** `rejected + 1 != next_index`, because it isn't a reply to the append we'd send now.

**Why this is safe.** `match_index` still moves only on acks, and `maybe_commit` reads only `match_index`. The commit rule is exactly what it was, and Figure 8 still holds. `next_index` was always only a guess about where to start sending; Figure 2 already allows it to be wrong, and a rejection corrects it.

## Verification
| | before | after |
|---|---|---|
| appends per write per follower, 300 writes, no faults | 51.9 | 1.6 (heartbeats included) |
| same, 5% drops | 11.9 | 1.5 |
| snapshot transfers, compaction every 20, no faults | 342 | 21 |
| 5,000-seed chaos sweep | 0 failures | 0 failures (71k snapshots installed, so coverage held) |

- **New tests:**
  - `replication_traffic_is_linear_in_writes` bounds appends and snapshots;
  - `a_replicating_peer_gets_each_new_entry_once`, `a_probing_peer_has_one_append_in_flight` and `stale_rejections_are_ignored` test the core directly.
- **Mutations of the fix, each caught by at least one test:**
  - remove the stale filter;
  - remove the optimistic advance;
  - ignore the pause;
  - never leave probing.
- **Earlier mutations re-run on the new code:**
  - no up-to-date vote check: still caught by the chaos tests;
  - non-random election timeouts: still caught by the split-vote tests.
- **Figure 8:** both scripted tests still pass, and the planted bug is still caught.

## What it cost
The simulated client latency doubled. With no faults, p50 went from 3 to 7 ticks and the mean from 3.6 to 7.2. With the default chaos settings, p99 went from 86 to 117.

**The fault-free part is the honest number.** The simulator gives every message its own random delay of 1–6 ticks. A flood of redundant copies therefore acts like hedged requests: some copy of an entry gets the 1-tick delay, so the old median of 3 ticks was below a realistic round trip. Seven ticks is about one round trip to a majority. Over a real TCP connection, messages to one peer arrive in order, so redundant copies wouldn't win any races.

**The p99 increase is real.** After a drop, a probing follower now waits for a heartbeat (3 ticks) instead of being rescued by a redundant copy that happened to be in flight. That's the usual trade-off: about 32× less traffic, a slightly longer tail on recovery.

## Why the tests missed it, and what changed
Every check I had asks "is the result correct?" Safety (one leader per term, no two different entries applied at one index) and linearizability are both about results. Sending 30× too many messages produces correct results, only more slowly and expensively. My chaos sweep could not have caught this bug, however many seeds it ran.

**What changed:** traffic is now a tested property (`tests/traffic.rs`), and chaos results report snapshot counts. The lesson I'd generalize: a correctness harness proves only correctness. Measure the cost too, and read a trace now and then even when everything is green.

## Other findings from R5 (about the harness, not Raft)
These aren't the bug, but they're real and they changed the tests.

1. **The crash model was too coarse.** At first, nodes crashed only between whole simulator steps. A planted "send before persist" bug was then *byte-identical* to correct Raft across 200 seeds, because the window it opens was never hit. I added crashes in the middle of a driver step (`crash_mid_step`). Correct Raft still fails 0 seeds. The planted bug fails 1–16 of 300, depending on the rate.
2. **Random chaos can't find Figure 8.** With the old-term commit bug planted, 500 seeds caught it 0 times, even with one-entry batches. A new leader's no-op travels with the old entries, so a majority almost never holds an old-term entry without a current-term one. Figure 8 is now a scripted test (`tests/figure8.rs`) that reproduces the paper's exact sequence and shows both outcomes.
3. **My own test bugs while scripting Figure 8:**
   - My message filter dropped the probe that S3 had to reject, so the leader never backed up.
   - With tracing on, the filter ran twice per message. It counted messages, so turning tracing on changed the outcome.

   Both were fixed, and the filter now runs once per message.
