//! Chaos runs (design §6–§7, milestone R5): simulated clients, a fault
//! schedule, and a check of every run.
//!
//! Clients behave like real ones. Each has one request outstanding at a
//! time; it sends the request to the current leader, and if no answer comes
//! within `client_timeout` ticks (the leader crashed, was partitioned, or
//! lost its entry to a new leader) it retries with the same sequence number.
//! The state machine's client sessions make the retry safe. A request that
//! never gets an answer is recorded as "timed out": it may or may not have
//! taken effect, and the checker treats it so.
//!
//! Every run is checked two ways:
//!
//! - the simulator's own safety checks (one leader per term; no two nodes
//!   apply different entries at one index);
//! - the clients' history must be linearizable (`checker`).

use crate::{Applied, Rng, Sim, SimConfig};
use checker::{Kind, Op as HistOp, Outcome};
use kv::{Command, Op, Reply};
use raft_core::{Bugs, Index, NodeId, Term};

/// One chaos run's settings.
#[derive(Clone, Debug)]
pub struct ChaosConfig {
    pub seed: u64,
    pub nodes: u64,
    pub clients: u64,
    pub keys: u64,
    /// Ticks with faults, then a quiet period with everything healed.
    pub ticks: u64,
    pub quiet: u64,
    pub client_timeout: u64,
    /// Per million per tick.
    pub crash: u32,
    pub restart: u32,
    pub partition: u32,
    pub heal: u32,
    pub drop: u32,
    pub duplicate: u32,
    pub delay: (u64, u64),
    pub compact_every: u64,
    /// Known bugs to plant in every node (to prove the checks catch them).
    pub bugs: Bugs,
    pub send_before_persist: bool,
    /// Per million per driver step (see `SimConfig::crash_mid_step`).
    pub crash_mid_step: u32,
    pub max_batch: usize,
}

impl ChaosConfig {
    pub fn new(seed: u64) -> Self {
        ChaosConfig {
            seed,
            nodes: 5,
            clients: 4,
            keys: 3,
            ticks: 600,
            quiet: 300,
            client_timeout: 40,
            crash: 8_000,
            restart: 30_000,
            partition: 5_000,
            heal: 20_000,
            drop: 50_000,
            duplicate: 20_000,
            delay: (1, 6),
            compact_every: 20,
            bugs: Bugs::default(),
            send_before_persist: false,
            crash_mid_step: 500,
            max_batch: 64,
        }
    }
}

/// What a run found.
#[derive(Debug)]
pub struct ChaosResult {
    pub seed: u64,
    pub history: Vec<HistOp>,
    /// Simulator safety violations.
    pub safety: Vec<String>,
    /// `Err` if the history is not linearizable.
    pub linearizable: Result<(), checker::Violation>,
    pub completed: usize,
    pub timed_out: usize,
    pub crashes: u32,
    pub partitions: u32,
    pub terms: Term,
    /// InstallSnapshot messages delivered (snapshot coverage).
    pub snapshots: usize,
}

impl ChaosResult {
    pub fn ok(&self) -> bool {
        self.safety.is_empty() && self.linearizable.is_ok()
    }
}

struct Pending {
    /// Position in the history.
    hist: usize,
    cmd: Command,
    /// Where the latest attempt was appended: (node, index).
    at: Option<(NodeId, Index)>,
    attempt_started: u64,
}

struct Client {
    id: u64,
    seq: u64,
    pending: Option<Pending>,
}

fn outcome(r: &Reply) -> Option<Outcome> {
    match r {
        Reply::Value(v) => Some(Outcome::Value(v.clone())),
        Reply::Written => Some(Outcome::Written),
        Reply::Deleted(b) => Some(Outcome::Deleted(*b)),
        Reply::Stale => None,
    }
}

/// A scripted or random fault, applied at a tick.
#[derive(Clone, Debug)]
pub enum Fault {
    Crash(NodeId),
    CrashLeader,
    Restart(NodeId),
    RestartAll,
    Partition(Vec<Vec<NodeId>>),
    /// The current leader plus `n` followers, cut off from the rest.
    IsolateLeaderWith(usize),
    Heal,
    /// Force these nodes' next election timeout to `ticks`.
    ForceTimeouts(Vec<NodeId>, u32),
}

/// Runs one chaos run. `script` adds faults at given ticks on top of the
/// random ones (use rates of 0 for a purely scripted scenario).
pub fn run(cfg: &ChaosConfig, script: &[(u64, Fault)]) -> ChaosResult {
    let mut sc = SimConfig::new(cfg.nodes, cfg.seed);
    sc.delay = cfg.delay;
    sc.drop = cfg.drop;
    sc.duplicate = cfg.duplicate;
    sc.compact_every = cfg.compact_every;
    sc.send_before_persist = cfg.send_before_persist;
    sc.crash_mid_step = cfg.crash_mid_step;
    sc.max_batch = cfg.max_batch;
    let mut sim = Sim::new(sc);
    for id in 1..=cfg.nodes {
        sim.set_bugs(id, cfg.bugs);
    }
    let mut rng = Rng::new(cfg.seed.wrapping_mul(0x9E37_79B9) ^ 0xC4A0_5);
    let mut clients: Vec<Client> = (1..=cfg.clients).map(|id| Client { id, seq: 0, pending: None }).collect();
    let mut history: Vec<HistOp> = Vec::new();
    let mut cursor = 0usize; // into sim.applied_log
    let (mut crashes, mut partitions) = (0, 0);
    let minority = ((cfg.nodes - 1) / 2) as usize;

    for t in 0..cfg.ticks + cfg.quiet {
        let faulty = t < cfg.ticks;
        if t == cfg.ticks {
            sim.heal();
            for id in 1..=cfg.nodes {
                if !sim.nodes[&id].alive() {
                    sim.restart(id);
                }
            }
        }
        for (_, f) in script.iter().filter(|(at, _)| *at == t) {
            apply_fault(&mut sim, f, &mut crashes, &mut partitions);
        }
        if faulty {
            let down: Vec<NodeId> = (1..=cfg.nodes).filter(|i| !sim.nodes[i].alive()).collect();
            if down.len() < minority.max(1) && rng.chance(cfg.crash) {
                let alive: Vec<NodeId> = (1..=cfg.nodes).filter(|i| sim.nodes[i].alive()).collect();
                let v = alive[(rng.next() % alive.len() as u64) as usize];
                sim.crash(v);
                crashes += 1;
            }
            for &d in &down {
                if rng.chance(cfg.restart) {
                    sim.restart(d);
                }
            }
            if rng.chance(cfg.partition) {
                let mut a = Vec::new();
                let mut b = Vec::new();
                for id in 1..=cfg.nodes {
                    if rng.next() % 2 == 0 { a.push(id) } else { b.push(id) }
                }
                sim.partition(vec![a, b]);
                partitions += 1;
            } else if rng.chance(cfg.heal) {
                sim.heal();
            }
        }

        // Clients: start new requests, retry timed-out ones.
        for c in clients.iter_mut() {
            if c.pending.is_none() && faulty && rng.chance(300_000) {
                c.seq += 1;
                let key = format!("k{}", rng.next() % cfg.keys);
                let (op, kind) = match rng.next() % 3 {
                    0 => (Op::Get(key.clone()), Kind::Get),
                    1 => {
                        let v = format!("c{}s{}", c.id, c.seq);
                        (Op::Put(key.clone(), v.clone()), Kind::Put(v))
                    }
                    _ => (Op::Delete(key.clone()), Kind::Delete),
                };
                history.push(HistOp { client: c.id, key, kind, invoked: sim.now, returned: None });
                c.pending = Some(Pending { hist: history.len() - 1, cmd: Command { client: c.id, seq: c.seq, op }, at: None, attempt_started: sim.now });
            }
            if let Some(p) = c.pending.as_mut() {
                let stale = p.at.is_none() || sim.now >= p.attempt_started + cfg.client_timeout;
                if stale {
                    if let Some((node, index, _)) = sim.propose(p.cmd.encode()) {
                        p.at = Some((node, index));
                    } else {
                        p.at = None;
                    }
                    p.attempt_started = sim.now;
                }
            }
        }

        sim.step();

        // Answers: a request is answered when the node it was appended to
        // applies that index. If a different entry landed there, the
        // attempt was lost (overwritten by a new leader): retry.
        while cursor < sim.applied_log.len() {
            let a: Applied = sim.applied_log[cursor].clone();
            cursor += 1;
            for c in clients.iter_mut() {
                let Some(p) = c.pending.as_mut() else { continue };
                if p.at != Some((a.node, a.index)) {
                    continue;
                }
                if a.data == p.cmd.encode() {
                    if let Some(o) = a.reply.as_ref().and_then(outcome) {
                        history[p.hist].returned = Some((sim.now, o));
                        c.pending = None;
                    }
                } else {
                    p.at = None; // lost: resend at the next tick
                }
            }
        }
    }

    let completed = history.iter().filter(|o| o.returned.is_some()).count();
    let linearizable = checker::check(&history);
    ChaosResult {
        seed: cfg.seed,
        timed_out: history.len() - completed,
        completed,
        safety: sim.violations.clone(),
        linearizable,
        crashes: crashes + sim.mid_step_crashes,
        partitions,
        terms: sim.leaders.keys().max().copied().unwrap_or(0),
        snapshots: sim.trace.iter().filter(|l| l.contains(" deliver ") && l.contains("InstallSnapshot {")).count(),
        history,
    }
}

fn apply_fault(sim: &mut Sim, f: &Fault, crashes: &mut u32, partitions: &mut u32) {
    match f {
        Fault::Crash(id) => {
            if sim.nodes[id].alive() {
                sim.crash(*id);
                *crashes += 1;
            }
        }
        Fault::CrashLeader => {
            if let Some(l) = sim.leader() {
                sim.crash(l);
                *crashes += 1;
            }
        }
        Fault::Restart(id) => {
            if !sim.nodes[id].alive() {
                sim.restart(*id);
            }
        }
        Fault::RestartAll => {
            let down: Vec<NodeId> = sim.nodes.values().filter(|n| !n.alive()).map(|n| n.id).collect();
            for d in down {
                sim.restart(d);
            }
        }
        Fault::Partition(groups) => {
            sim.partition(groups.clone());
            *partitions += 1;
        }
        Fault::IsolateLeaderWith(n) => {
            if let Some(l) = sim.leader() {
                let mut small = vec![l];
                let mut rest = Vec::new();
                for id in 1..=sim.cfg.nodes {
                    if id == l {
                        continue;
                    }
                    if small.len() <= *n { small.push(id) } else { rest.push(id) }
                }
                sim.partition(vec![small, rest]);
                *partitions += 1;
            }
        }
        Fault::Heal => sim.heal(),
        Fault::ForceTimeouts(ids, ticks) => {
            for id in ids {
                if let Some(c) = sim.core_mut(*id) {
                    c.force_election_timeout(*ticks);
                }
            }
        }
    }
}
