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

mod codec;
pub mod retained;
pub mod session;

pub use retained::{load_retained, save_retained};
pub use session::{load_sessions, save_sessions};
