//! A linearizability checker for key-value histories (design §7).
//!
//! **Linearizable** means: there is one sequential order of all operations
//! that (1) respects real time (if A returned before B was invoked, A comes
//! first) and (2) gives every operation the result a single, correct
//! key-value store would have given it in that order. If no such order
//! exists, some client saw something no correct single machine could show.
//!
//! The search is Wing & Gong's: repeatedly pick an operation that may come
//! next (no other un-placed, completed operation returned before it was
//! invoked), apply it to a model register, check its result, and recurse;
//! back up on a mismatch. Visited (placed set, register state) pairs are
//! remembered, which keeps it fast on real histories.
//!
//! A key-value store is linearizable iff each key's sub-history is, so the
//! search runs per key (much smaller).
//!
//! An operation that never returned (its client timed out) may have taken
//! effect at any point after it was invoked, or never: it can be placed
//! anywhere after its invocation, or left out.

use std::collections::{BTreeMap, HashSet};

/// What an operation did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    Get,
    Put(String),
    Delete,
}

/// What an operation returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Get's value.
    Value(Option<String>),
    /// Put done.
    Written,
    /// Delete: whether the key existed.
    Deleted(bool),
}

/// One client operation in a history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op {
    pub client: u64,
    pub key: String,
    pub kind: Kind,
    pub invoked: u64,
    /// When it returned and with what; `None`: it never returned.
    pub returned: Option<(u64, Outcome)>,
}

/// Why a history is not linearizable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub key: String,
    /// The key's operations, in invocation order, for the report.
    pub ops: Vec<Op>,
}

/// Checks a whole history. Returns the first key with no valid order.
pub fn check(history: &[Op]) -> Result<(), Violation> {
    let mut by_key: BTreeMap<&str, Vec<Op>> = BTreeMap::new();
    for op in history {
        by_key.entry(&op.key).or_default().push(op.clone());
    }
    for (key, mut ops) in by_key {
        ops.sort_by_key(|o| (o.invoked, o.client));
        // A Get that never returned constrains nothing: drop it.
        ops.retain(|o| o.returned.is_some() || o.kind != Kind::Get);
        if !check_key(&ops) {
            return Err(Violation { key: key.to_string(), ops });
        }
    }
    Ok(())
}

/// The register a key is: absent, or a value.
type State = Option<String>;

/// Applies `op` to the register; `None` if its recorded result is
/// impossible from `state` (only completed operations are checked).
fn apply(state: &State, op: &Op) -> Option<State> {
    let result = op.returned.as_ref().map(|(_, r)| r);
    match &op.kind {
        Kind::Get => match result {
            Some(Outcome::Value(v)) if v == state => Some(state.clone()),
            Some(_) => None,
            None => Some(state.clone()),
        },
        Kind::Put(v) => match result {
            Some(Outcome::Written) | None => Some(Some(v.clone())),
            Some(_) => None,
        },
        Kind::Delete => match result {
            Some(Outcome::Deleted(existed)) if *existed == state.is_some() => Some(None),
            None => Some(None),
            Some(_) => None,
        },
    }
}

fn check_key(ops: &[Op]) -> bool {
    let n = ops.len();
    let words = n.div_ceil(64);
    let mut seen: HashSet<(Vec<u64>, State)> = HashSet::new();
    let mut placed = vec![0u64; words];
    search(ops, &mut placed, &None, &mut seen)
}

fn is_placed(placed: &[u64], i: usize) -> bool {
    placed[i / 64] & (1 << (i % 64)) != 0
}

fn flip(placed: &mut [u64], i: usize) {
    placed[i / 64] ^= 1 << (i % 64);
}

fn search(ops: &[Op], placed: &mut Vec<u64>, state: &State, seen: &mut HashSet<(Vec<u64>, State)>) -> bool {
    // Done when every completed operation is placed (pending ones may
    // never have happened).
    let open: Vec<usize> = (0..ops.len()).filter(|&i| !is_placed(placed, i)).collect();
    if open.iter().all(|&i| ops[i].returned.is_none()) {
        return true;
    }
    if !seen.insert((placed.clone(), state.clone())) {
        return false;
    }
    // An operation may go next only if no un-placed, completed operation
    // returned before it was invoked (real-time order).
    let earliest_return = open.iter().filter_map(|&i| ops[i].returned.as_ref().map(|(t, _)| *t)).min().unwrap_or(u64::MAX);
    for &i in &open {
        if ops[i].invoked > earliest_return {
            continue;
        }
        if let Some(next) = apply(state, &ops[i]) {
            flip(placed, i);
            let ok = search(ops, placed, &next, seen);
            flip(placed, i);
            if ok {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests;
