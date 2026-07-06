//! The message-delivery value types — the broker's *lingua franca* for moving a
//! routed message toward a connection.
//!
//! These are shared by routing ([`shard`](crate::broker::shard)), the connection
//! outbound path ([`Connection`](crate::server::connection::Connection)), and the
//! persistence/migration codecs. They are deliberately separate from durable
//! *session* state ([`session`](crate::broker::session)): a [`Delivery`] is one
//! in-flight message, not per-client state.

use std::rc::Rc;

use glommio::channels::local_channel::LocalSender;
use mqttbytes::{QoS, v5::Publish};

/// Upper bound on QoS > 0 messages buffered for a suspended (offline) session.
/// The oldest are dropped once full, so a client that never returns can't grow an
/// unbounded backlog.
pub const OFFLINE_QUEUE_LIMIT: usize = 1024;

/// Upper bound on deliveries queued in a *connected* session's mailbox, enforced
/// at the routing site (the channel itself is unbounded so an idle connection
/// allocates nothing). This is a hard DoS guard: if a subscriber stops reading
/// its socket, its connection task parks on the blocked write and stops draining
/// the mailbox — without the bound, other clients' publishes would grow it
/// without limit. Further deliveries to a full mailbox are dropped.
pub const MAILBOX_LIMIT: usize = 256;

/// A message routed toward a connection for delivery.
///
/// The `publish` is shared via `Rc` so one message fans out to many local
/// subscribers without re-allocating; `qos` is this subscriber's effective QoS
/// (`min(publish, granted)`). The receiving connection assigns its own packet id
/// when `qos > 0`.
pub struct Delivery {
	pub publish: Rc<Publish>,
	pub qos: QoS,
	/// Retain flag for the delivered PUBLISH: cleared on ordinary live fan-out,
	/// kept for a Retain-As-Published subscriber, set for a retained replay.
	pub retain: bool,
	/// Subscription Identifiers to echo (MQTT 5), gathered from every matching
	/// subscription of this client. Usually empty or one.
	pub sub_ids: Vec<usize>,
}

/// Sender half of a connection's mailbox. `LocalSender` is single-owner (not
/// `Clone`), so each connection's sender is stored exactly once — in its session.
pub type Mailbox = LocalSender<Delivery>;

/// A command from the broker layer to the shard's parking task (the server-layer
/// task that owns the parked-fd readiness ring). The broker knows *that* a session
/// is parked, not *how*; the server injects the sender at startup
/// ([`set_unpark_tx`](crate::broker::shard::ShardState::set_unpark_tx), mirroring
/// how the mesh control outbox is injected) and this enum is the whole vocabulary
/// between the two layers.
#[derive(Debug)]
pub enum UnparkCmd {
	/// A delivery landed for a parked client: resurrect its connection task so it
	/// can drain the queued messages. Sent at most once per park episode
	/// (deduplicated by the session's `wake_pending` flag).
	Wake {
		client_id: String,
	},
	/// The parked fd must be closed without resuming — the session was taken over
	/// by a new connection, discarded by a Clean Start, or claimed by another
	/// shard. `generation` is the parked connection's session generation, so a
	/// racing unpark (entry already gone or re-parked under a newer generation)
	/// makes this a no-op. No Will is published: takeover semantics.
	Close {
		client_id: String,
		generation: u64,
	},
}
