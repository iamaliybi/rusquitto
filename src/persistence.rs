//! Disk-backed persistence.
//!
//! Two kinds of durable state are snapshotted to disk and restored on startup:
//!
//! - [`retained`] — the retained-message set. Replicated identically on every
//!   shard, so one shard writes the snapshot and every shard reloads it.
//! - [`session`] — durable (suspended) sessions: subscriptions, in-flight QoS
//!   state, and the offline queue. These are *shard-local* (not replicated), so
//!   each shard persists and restores its own set.
//!
//! Both use [`codec`]'s atomic file I/O (glommio io_uring `BufferedFile`, so the
//! reactor never blocks) and its small length-prefixed value encoding.
//!
//! Sessions additionally have a per-shard [`wal`] (write-ahead log): an
//! append-only, group-committed record of session suspensions and offline-queue
//! growth *between* snapshots, replayed over the snapshot on startup so a crash
//! loses at most one WAL-flush window rather than a whole `snapshot_interval`.

mod codec;
pub mod retained;
pub mod session;
pub mod wal;

pub use retained::{load_retained, save_retained};
pub use session::{load_sessions, save_sessions};
pub use wal::Wal;
