//! The key-value state machine with client sessions, and one node's
//! crash-safe durable storage (design: docs/design.md §4–§5).

mod crc;
pub mod machine;
pub mod storage;

pub use machine::{Command, Kv, Op, Reply};
pub use storage::{DiskStorage, Recovered, StorageError, SyncMode};

#[cfg(test)]
mod storage_tests;
