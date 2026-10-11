# raftkv-quorum

A replicated key-value store on Raft, written in Rust with zero dependencies. It's built to be *checked*, not just run:

- **A pure Raft core.** `raft-core` takes ticks, messages and proposals, and returns a `Ready`. It does no I/O, uses no clock and starts no threads.
- **A deterministic simulator.** One seed controls every crash, partition, drop, duplicate and delay, so any failure replays exactly.
- **A hand-written linearizability checker** that checks every client history the chaos runs produce.
- **Crash-safe storage:** a CRC-framed WAL, atomic snapshot files, and `F_FULLFSYNC` on macOS.
- **A thin TCP runtime:** three real processes, run by the same core.

## Try it
```sh
scripts/demo.sh        # 3 processes: write, kill -9 the leader, write, restart it, watch it catch up
```
Or by hand:
```sh
cargo build --release -p server
C=1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002,3=127.0.0.1:7003/127.0.0.1:8003
target/release/raftkv node --id 1 --cluster $C --dir data/1 &   # likewise for 2 and 3
target/release/raftkv client --cluster $C put greeting hello
target/release/raftkv client --cluster $C get greeting
target/release/raftkv client --cluster $C status
```

## Tests
```sh
cargo test --workspace                                              # 86 tests, about 10 s (incl. two real 3-process clusters)
cargo test -p sim --release --test chaos -- --ignored long_sweep    # 5,000 seeded chaos runs
```
- **Raft rules:** unit tests for each Figure 2 rule.
- **Figure 8:** the paper's sequence, scripted message by message.
- **Chaos scenarios:** kill mid-write, a partitioned minority leader, and split votes.
- **Random chaos:** 200 seeded fault schedules per run, plus a 5,000-seed sweep. That sweep covers about 850k client operations, 30k crashes and 15k partitions, with 0 failures.
- **Traffic bounds and real 3-process cluster tests.**

The checks were themselves tested by mutation: I broke a rule on purpose, confirmed that a test failed, then restored it.

## Read more
- [`docs/design.md`](docs/design.md): the architecture, the `Ready` contract, scope, and what's deliberately out of scope.
- [`POSTMORTEM.md`](POSTMORTEM.md): the real bug. Replication traffic was quadratic, and every correctness check passed anyway.
- [`raftkv-quorum-DECISIONS.md`](raftkv-quorum-DECISIONS.md): every design decision, the alternatives, and who made the call.

## Layout
| crate | what |
|---|---|
| `raft-core` | Raft as a pure state machine; the wire codec |
| `kv` | the state machine (with client sessions) and durable storage |
| `sim` | the deterministic simulator and chaos harness |
| `checker` | the linearizability checker |
| `server` | the `raftkv` binary: TCP, disk and timer around the core |
