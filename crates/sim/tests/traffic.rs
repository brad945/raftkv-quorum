//! Message traffic (POSTMORTEM.md). The safety and linearizability checks
//! can't see a replication protocol that sends far more than it needs to,
//! so these tests count messages directly.

use kv::{Command, Op};
use sim::{Sim, SimConfig};

struct Traffic {
    appends: usize,
    snapshots: usize,
}

/// One write per tick through a steady leader; counts delivered messages.
fn measure(writes: u64, drop: u32, compact_every: u64) -> Traffic {
    let mut c = SimConfig::new(5, 4);
    c.drop = drop;
    c.compact_every = compact_every;
    let mut s = Sim::new(c);
    s.run(50);
    for i in 0..writes {
        let _ = s.propose(Command { client: 1, seq: i + 1, op: Op::Put("k".into(), format!("v{i}")) }.encode());
        s.run(1);
    }
    s.run(100);
    let count = |what: &str| s.trace.iter().filter(|l| l.contains(" deliver ") && l.contains(what)).count();
    Traffic { appends: count("AppendEntries {"), snapshots: count("InstallSnapshot {") }
}

#[test]
#[ignore]
fn print_traffic() {
    for drop in [0, 50_000] {
        for writes in [100, 200, 300] {
            let t = measure(writes, drop, 0);
            let s = measure(writes, drop, 20);
            println!("drop {drop:>6}ppm, {writes} writes: {} appends ({:.1} per write per follower); with compaction every 20: {} snapshots", t.appends, t.appends as f64 / writes as f64 / 4.0, s.snapshots);
        }
    }
}

// POSTMORTEM.md. Before the fix: 52 appends per write per follower at 300
// writes (growing with the write count) and 342 snapshot transfers to
// healthy followers. After: about 1.6 (heartbeats included) and 21.
#[test]
fn replication_traffic_is_linear_in_writes() {
    for drop in [0, 50_000] {
        let t = measure(300, drop, 0);
        let per = t.appends as f64 / 300.0 / 4.0;
        assert!(per < 4.0, "drop {drop}: {per:.1} appends per write per follower");
        let s = measure(300, drop, 20);
        assert!(s.snapshots < 60, "drop {drop}: {} snapshot transfers for 15 compactions", s.snapshots);
    }
}
