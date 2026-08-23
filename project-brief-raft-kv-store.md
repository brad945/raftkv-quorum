# Project Brief: Replicated Key-Value Store with Raft Consensus

**Owner:** Bradley (GitHub: brad945)
**Purpose of this doc:** Full context for a Claude Code session building this repo from scratch. This captures the reasoning, not just the spec — read it before writing code.

---

## 1. What this is, in one sentence

A small, self-contained, deeply-understood distributed key-value store in Rust, using a real Raft consensus implementation (leader election, log replication, snapshotting), deliberately chaos-tested rather than just implemented.

## 2. Why this project exists (the actual narrative)

This is the second of two portfolio projects built for YC/early-stage startup SWE internship recruiting this cycle (see companion brief: `project-brief-agent-orchestration-platform.md`). The two projects are intentionally kept **separate rather than merged** — bolting a full consensus/replication system onto the agent orchestration platform would be real scope creep (three hard systems problems — scheduling, agent workloads, and distributed consensus — none of them fully understood by the deadline). This project exists specifically to be small, focused, and completely, defensibly understood — which is a stronger signal than a sprawling one.

**Direct match to Dedalus Labs' Infrastructure Engineer Intern JD**, which explicitly lists — nearly verbatim — "a correct implementation of Raft, Paxos, replication, sharding, or another distributed-systems concept, with thoughtful testing and documentation" as one of their "especially strong signals," and separately advises applicants that "a smaller project you deeply understand is more compelling than a popular project where your contribution is unclear." This project is a direct, honest response to that specific ask.

**Secondary relevance:** general systems/database credibility for any of the other companies in this cycle (Sixtyfour, TraceRoot, Ergo) — concurrency, correctness under failure, and testing discipline are broadly valued signals, even where a KV store isn't the literal product.

**Bradley's hard rule for anything he says in interviews:** only claims he can defend cold, with exact numbers, never rounded up or implying scale/correctness that isn't actually there. For this project specifically, that means: don't claim "handles network partitions correctly" without having actually simulated one and shown it.

## 3. Language decision (already made, with reasoning)

**Rust**, not Go. Reasoning: Rust's ownership model forces genuine reasoning about shared mutable state instead of allowing sloppy locking to "work by accident" — which is exactly the discipline a correct Raft implementation is supposed to build. It's also the first language listed in Dedalus's own JD, making it a slightly stronger direct signal for that specific company. (The companion project uses Go instead — see that brief for reasoning — so this project also demonstrates real fluency across both languages rather than picking one and hiding behind it.)

## 4. Scope (deliberately small — do not let this grow)

1. **Single-node KV store** — basic get/set/delete, in-memory or simple on-disk storage. Get this right and simple before adding any distribution.
2. **Leader election** across 3 simulated nodes (can run as separate local processes or in Docker containers — doesn't need real network hosts).
3. **Log replication** — leader replicates writes to followers, handles the standard Raft log-matching logic.
4. **Snapshotting** — so the replicated log doesn't grow unbounded.
5. **Chaos testing (this is the point of the project, not an afterthought):**
   - Kill a node mid-write and verify the cluster recovers correctly
   - Partition the network (simulate it — no need for real network infrastructure) and verify the system either maintains linearizability or the failure mode is precisely understood and documented
   - Force a split-vote scenario during leader election and verify it resolves correctly
6. **What this project explicitly does NOT need:** sharding, a query language, a client SDK beyond the minimum needed to test it, or any kind of UI. Resist scope creep — the value here is depth on the consensus/replication mechanics, not breadth of features.

## 5. Timeline and pacing

This project comes **first** in the sequence (roughly the first 1-2 weeks of the overall 4-6 week window Bradley has set for both projects combined), because the concurrency/RPC/testing discipline built here directly transfers into the Go orchestration platform that follows. As with the companion project: **this is Bradley's own pace, not a hard deadline — prioritize structure and real technical depth over speed.**

Rough internal shape (not a strict schedule, a sanity check on scope):
- Single-node store working correctly, tested
- Leader election + log replication across 3 nodes
- Snapshotting
- Chaos testing — this is where the real, defensible bug story will come from. Budget real time for it; don't treat it as a quick check at the end.

## 6. The deliverable that matters most: the bug postmortem

Chaos-testing a Raft implementation will surface a real bug — most likely somewhere in log matching, split-vote handling, or a race during leader transition. **Write this up explicitly, as its own document in the repo** (or as a clearly separated section of `DECISIONS.md` — see working process below): what broke, how it was diagnosed, what the fix was, and why the fix is actually correct and not just "it stopped crashing." This writeup is worth as much as the implementation itself in an interview — Dedalus, Relling, and Ergo's JDs all explicitly ask candidates to "explain the hardest bug or failure you encountered" and "what you personally contributed."

## 7. Working process (how Bradley wants this session to run)

Same process as the companion project — this is meant to be a learning process, not a hand-off:

- Bradley will code review along the way.
- **Some design choices are Bradley's to make; some are Claude Code's — whichever one makes the call, the reasoning needs to be explained.**
- **When there's a genuine fork in the road** (e.g., how strictly to implement the Raft paper's exact algorithm vs. a simplified variant, what failure modes are in-scope to handle vs. explicitly document as out-of-scope, how much of the chaos-testing harness to automate vs. run manually), **Claude Code should flag it explicitly and ask Bradley rather than silently picking.**
- **Maintain a `DECISIONS.md` in the repo root.** Every non-trivial choice gets a short entry: what was decided, why, what the alternative was and why it lost, and who made the call — tag entries `[bradley]` or `[claude]`. By the end, this should function as genuine interview prep.
- **Commit in small, coherent chunks with real commit messages** — the commit history should be readable as a trail of reasoning, supporting real code review.
- At the end of significant chunks of work, summarize and explain the meaningful choices made — both the logical/technical ones and the more intuitive/judgment-call ones — so Bradley actually retains the reasoning.

## 8. What "good" looks like at the end

Bradley should be able to, cold, in an interview:
- Walk through leader election and log replication without needing notes
- Describe precisely what happens during a network partition or a killed node, because he actually simulated it and watched it happen
- Tell the specific story of the hardest bug: what broke, how he found it, why the fix is correct
- Be honest and specific about what this implementation does NOT handle (e.g., no sharding), rather than overstating scope — consistent with his rule about never implying scale or correctness that isn't actually there

## 9. Repo name

**`raftkv-quorum`** — confirmed name for this repo.

## 10. Relationship to the companion project

Kept intentionally separate (see Section 2). If a natural connection point emerges later (e.g., using this KV store as the durable backing store for the orchestration platform's task queue instead of Postgres), that would be a legitimate, interesting extension — but it should be treated as an optional stretch idea for *after* both projects independently work and are independently understood, never as a requirement that couples the two repos together from the start.
