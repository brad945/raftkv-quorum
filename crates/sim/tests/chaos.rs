//! Chaos tests (design §8, milestone R5).

use raft_core::Bugs;
use sim::chaos::{run, ChaosConfig, ChaosResult, Fault};

fn summarize(name: &str, rs: &[ChaosResult]) {
    let bad: Vec<u64> = rs.iter().filter(|r| !r.ok()).map(|r| r.seed).collect();
    let ops: usize = rs.iter().map(|r| r.history.len()).sum();
    let done: usize = rs.iter().map(|r| r.completed).sum();
    let crashes: u32 = rs.iter().map(|r| r.crashes).sum();
    let parts: u32 = rs.iter().map(|r| r.partitions).sum();
    let snaps: usize = rs.iter().map(|r| r.snapshots).sum();
    println!("{name}: {} runs, {ops} ops ({done} answered), {crashes} crashes, {parts} partitions, {snaps} snapshots installed, failing seeds: {bad:?}", rs.len());
}

#[test]
#[ignore]
fn explore() {
    let rs: Vec<ChaosResult> = (1..=200).map(|s| run(&ChaosConfig::new(s), &[])).collect();
    summarize("correct raft", &rs);
    if let Some(r) = rs.iter().find(|r| !r.ok()) {
        println!("first failure seed {}: safety {:?}\nlin {:?}", r.seed, r.safety, r.linearizable.as_ref().err().map(|v| &v.key));
    }
    for (name, bugs, sbp) in [
        ("figure8 bug", Bugs { commit_old_term_by_count: true, ..Bugs::default() }, false),
        ("truncate bug", Bugs { truncate_on_every_append: true, ..Bugs::default() }, false),
        ("send before persist", Bugs::default(), true),
    ] {
        let rs: Vec<ChaosResult> = (1..=200).map(|s| { let mut c = ChaosConfig::new(s); c.bugs = bugs; c.send_before_persist = sbp; run(&c, &[]) }).collect();
        summarize(name, &rs);
    }
}

#[test]
#[ignore]
fn explore_mid_step() {
    for rate in [5_000u32, 20_000, 50_000] {
        for (name, sbp) in [("correct raft", false), ("send before persist", true)] {
            let rs: Vec<ChaosResult> = (1..=300).map(|s| { let mut c = ChaosConfig::new(s); c.crash_mid_step = rate; c.send_before_persist = sbp; run(&c, &[]) }).collect();
            summarize(&format!("{name} @ mid-step crash {rate}ppm"), &rs);
            if let Some(r) = rs.iter().find(|r| !r.ok()) {
                println!("   first failure seed {}: safety {:?} linearizable: {}", r.seed, r.safety.first(), r.linearizable.is_ok());
            }
        }
    }
}

#[test]
#[ignore]
fn explore_figure8() {
    for batch in [64usize, 1] {
        for (name, bugs) in [("correct raft", Bugs::default()), ("figure8 bug", Bugs { commit_old_term_by_count: true, ..Bugs::default() })] {
            let rs: Vec<ChaosResult> = (1..=500).map(|s| { let mut c = ChaosConfig::new(s); c.max_batch = batch; c.bugs = bugs; run(&c, &[]) }).collect();
            summarize(&format!("{name} @ max_batch {batch}"), &rs);
            if let Some(r) = rs.iter().find(|r| !r.ok()) {
                println!("   first failure seed {}: safety {:?} linearizable: {}", r.seed, r.safety.first(), r.linearizable.is_ok());
            }
        }
    }
}

/// Random faults off: only the script (and message delays) acts.
fn scripted(seed: u64) -> ChaosConfig {
    let mut c = ChaosConfig::new(seed);
    c.crash = 0;
    c.partition = 0;
    c.heal = 0; // or a random heal undoes the scripted partition
    c.drop = 0;
    c.duplicate = 0;
    c.crash_mid_step = 0;
    c
}

fn assert_ok(r: &ChaosResult) {
    assert!(r.safety.is_empty(), "seed {}: safety violations {:?}", r.seed, r.safety);
    assert!(r.linearizable.is_ok(), "seed {}: not linearizable: {:?}", r.seed, r.linearizable);
}

// design §8: the leader is cut off with one follower (a minority). The
// majority side elects a new leader and keeps serving; the old leader's
// writes from that time never commit. After the heal, everyone agrees.
#[test]
fn partition_minority_leader() {
    for seed in 1..=20 {
        let r = run(&scripted(seed), &[(150, Fault::IsolateLeaderWith(1)), (400, Fault::Heal)]);
        assert_ok(&r);
        assert!(r.terms >= 2, "seed {seed}: the majority side elected a new leader");
        assert!(r.completed > 20, "seed {seed}: clients kept getting answers ({} answered)", r.completed);
    }
}

// design §8: every node times out on the same tick, repeatedly, so votes
// split. Randomized timeouts must still produce a leader, and safety holds.
#[test]
fn split_vote() {
    for seed in 1..=20 {
        let all = vec![1, 2, 3, 4, 5];
        let script: Vec<(u64, Fault)> = (0..5).map(|i| (100 + i * 60, Fault::ForceTimeouts(all.clone(), 1))).collect();
        let r = run(&scripted(seed), &script);
        assert_ok(&r);
        assert!(r.completed > 20, "seed {seed}: a leader emerged and served ({} answered)", r.completed);
    }
}

// design §8: crashes between a node's two driver phases (state computed,
// not yet persisted) and right after persisting, before sending.
#[test]
fn kill_mid_write() {
    for seed in 1..=60 {
        let mut c = ChaosConfig::new(seed);
        c.crash_mid_step = 20_000;
        let r = run(&c, &[]);
        assert_ok(&r);
    }
}

// The main property: under random crashes, restarts, partitions, drops,
// duplicates and delays, correct Raft stays safe and linearizable.
#[test]
fn random_faults_linearizable() {
    let rs: Vec<ChaosResult> = (1..=200).map(|s| run(&ChaosConfig::new(s), &[])).collect();
    for r in &rs {
        assert_ok(r);
    }
    let answered: usize = rs.iter().map(|r| r.completed).sum();
    assert!(answered > 200 * 20, "clients made progress ({answered} answered)");
    let with_snapshots = rs.iter().filter(|r| r.snapshots > 0).count();
    assert!(with_snapshots > 100, "snapshot transfer is exercised ({with_snapshots} of 200 runs)");
}

/// The long sweep: `cargo test -p sim --release --test chaos -- --ignored long_sweep`.
#[test]
#[ignore]
fn long_sweep() {
    let rs: Vec<ChaosResult> = (1..=5_000).map(|s| run(&ChaosConfig::new(s), &[])).collect();
    summarize("correct raft, long sweep", &rs);
    for r in &rs {
        assert_ok(r);
    }
}

// The checks must be able to fail: plant known bugs and require that some
// seed is caught. (Figure 8's bug needs a scripted schedule: tests/figure8.rs.)
#[test]
fn checks_catch_planted_bugs() {
    let caught = |f: &dyn Fn(&mut ChaosConfig), seeds: u64| (1..=seeds).filter(|&s| { let mut c = ChaosConfig::new(s); f(&mut c); !run(&c, &[]).ok() }).count();
    let trunc = caught(&|c| c.bugs = Bugs { truncate_on_every_append: true, ..Bugs::default() }, 100);
    assert!(trunc > 0, "truncating on every append must be caught");
    let sbp = caught(&|c| { c.send_before_persist = true; c.crash_mid_step = 50_000; }, 100);
    assert!(sbp > 0, "sending before persisting must be caught");
}
