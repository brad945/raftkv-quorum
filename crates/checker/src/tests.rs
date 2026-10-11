//! The checker's own tests: histories with a known answer (design §7,
//! "testing the checker"). Time is in arbitrary units.

use super::*;

fn put(client: u64, key: &str, v: &str, inv: u64, ret: Option<u64>) -> Op {
    Op { client, key: key.into(), kind: Kind::Put(v.into()), invoked: inv, returned: ret.map(|t| (t, Outcome::Written)) }
}
fn get(client: u64, key: &str, saw: Option<&str>, inv: u64, ret: u64) -> Op {
    Op { client, key: key.into(), kind: Kind::Get, invoked: inv, returned: Some((ret, Outcome::Value(saw.map(String::from)))) }
}
fn del(client: u64, key: &str, existed: bool, inv: u64, ret: u64) -> Op {
    Op { client, key: key.into(), kind: Kind::Delete, invoked: inv, returned: Some((ret, Outcome::Deleted(existed))) }
}

#[test]
fn sequential_history_is_linearizable() {
    let h = vec![put(1, "x", "a", 0, Some(1)), get(1, "x", Some("a"), 2, 3), del(1, "x", true, 4, 5), get(1, "x", None, 6, 7)];
    assert!(check(&h).is_ok());
}

#[test]
fn concurrent_operations_may_take_either_order() {
    // Two overlapping writes, then reads that agree on one order.
    let h = vec![put(1, "x", "a", 0, Some(10)), put(2, "x", "b", 1, Some(9)), get(3, "x", Some("a"), 11, 12), get(3, "x", Some("a"), 13, 14)];
    assert!(check(&h).is_ok(), "b then a");
    let h2 = vec![put(1, "x", "a", 0, Some(10)), put(2, "x", "b", 1, Some(9)), get(3, "x", Some("b"), 11, 12)];
    assert!(check(&h2).is_ok(), "a then b");
}

#[test]
fn stale_read_after_a_completed_write_is_caught() {
    // The write finished before the read started, yet the read missed it.
    let h = vec![put(1, "x", "a", 0, Some(1)), put(1, "x", "b", 2, Some(3)), get(2, "x", Some("a"), 4, 5)];
    let v = check(&h).unwrap_err();
    assert_eq!(v.key, "x");
}

#[test]
fn reads_that_disagree_on_the_order_are_caught() {
    // Concurrent writes a and b, but one reader sees b then a and another
    // sees a then b, after both writes finished: no single order fits.
    let h = vec![
        put(1, "x", "a", 0, Some(5)),
        put(2, "x", "b", 0, Some(5)),
        get(3, "x", Some("b"), 6, 7),
        get(3, "x", Some("a"), 8, 9),
        get(4, "x", Some("b"), 10, 11),
    ];
    assert!(check(&h).is_err());
}

#[test]
fn lost_write_is_caught() {
    // An acknowledged write that no later read ever sees.
    let h = vec![put(1, "x", "a", 0, Some(1)), get(2, "x", None, 2, 3)];
    assert!(check(&h).is_err());
}

#[test]
fn timed_out_write_may_or_may_not_have_happened() {
    // Client 1 timed out: its write may have landed (any time after it was
    // sent) or not. Both readings must be accepted.
    let landed = vec![put(1, "x", "a", 0, None), get(2, "x", Some("a"), 5, 6)];
    let never = vec![put(1, "x", "a", 0, None), get(2, "x", None, 5, 6)];
    assert!(check(&landed).is_ok());
    assert!(check(&never).is_ok());
    // But it can't land before it was sent.
    let too_early = vec![get(2, "x", Some("a"), 0, 1), put(1, "x", "a", 2, None)];
    assert!(check(&too_early).is_err());
}

#[test]
fn a_timed_out_write_seen_then_unseen_is_caught() {
    // Once a later read saw it, it happened: an even later read can't miss it.
    let h = vec![put(1, "x", "a", 0, None), get(2, "x", Some("a"), 5, 6), get(2, "x", None, 7, 8)];
    assert!(check(&h).is_err());
}

#[test]
fn delete_results_are_checked() {
    assert!(check(&[del(1, "x", true, 0, 1)]).is_err(), "deleting an absent key can't report it existed");
    assert!(check(&[put(1, "x", "a", 0, Some(1)), del(1, "x", false, 2, 3)]).is_err());
}

#[test]
fn keys_are_independent() {
    let h = vec![put(1, "x", "a", 0, Some(1)), put(1, "y", "b", 0, Some(1)), get(2, "x", Some("a"), 2, 3), get(2, "y", Some("b"), 2, 3)];
    assert!(check(&h).is_ok());
    let bad = vec![put(1, "x", "a", 0, Some(1)), get(2, "y", Some("a"), 2, 3)];
    assert_eq!(check(&bad).unwrap_err().key, "y");
}

#[test]
fn large_concurrent_history_stays_fast() {
    // 8 clients, 300 operations on one key, heavily overlapping: the memo
    // keeps this quick. Writers write their own client id; readers read
    // the value of the last write that returned before them.
    let mut h = Vec::new();
    let mut t = 0;
    let mut last = None;
    for i in 0..300u64 {
        let c = i % 8;
        if i % 3 == 0 {
            h.push(get(c, "k", last.as_deref(), t, t + 2));
        } else {
            let v = format!("v{i}");
            h.push(put(c, "k", &v, t, Some(t + 2)));
            last = Some(v);
        }
        t += 3;
    }
    let start = std::time::Instant::now();
    assert!(check(&h).is_ok());
    assert!(start.elapsed().as_secs() < 5, "took {:?}", start.elapsed());
}
