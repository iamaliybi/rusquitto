//! Inter-shard channel-mesh message types and the session-migration protocol.
//!
//! Every shard is a full mesh peer. Most traffic is a forwarded [`Publish`] to be
//! re-routed locally on the receiving shard; a smaller share is [`SessionControl`]
//! for migrating a session between shards when a client reconnects onto a
//! different core.

use std::collections::HashMap;

use mqttbytes::{v5::Publish, QoS};

use crate::broker::session::InflightMessage;

/// A message crossing the inter-shard mesh. The control variant is boxed so the
/// common publish path keeps the enum — and thus the mesh ring buffers — small.
pub enum MeshMsg {
	Publish(Publish),
	Control(Box<SessionControl>),
}

/// Cross-shard session-migration protocol, exchanged over the mesh.
///
/// A reconnecting client can land — via the `SO_REUSEPORT` 4-tuple hash on its new
/// ephemeral port — on a different shard than the one holding its suspended
/// session. Every shard shares one listening address, so there is nothing to
/// redirect the client to; the *session* moves instead. The reached shard
/// broadcasts a [`Claim`](Self::Claim), and whichever peer owns the session
/// replies with a [`Handoff`](Self::Handoff) carrying it.
pub enum SessionControl {
	/// "Client `client_id` just (re)connected to me (`requester`); hand over its
	/// session if you hold it." `resume = false` (Clean Start) instead asks peers
	/// to *discard* any session they hold for this client id.
	Claim {
		client_id: String,
		/// Mesh peer id to send the [`Handoff`](Self::Handoff) reply back to.
		requester: usize,
		resume: bool,
	},
	/// Reply to a [`Claim`](Self::Claim): the owning peer's session, or `None` if
	/// it held none (or the claim was a discard).
	Handoff {
		client_id: String,
		session: Option<MigratedSession>,
	},
}

/// A whole session serialized for migration to another shard.
///
/// Owned data only — the mesh moves values between executors, so the offline
/// queue's `Rc<Publish>` is unwrapped to an owned `Publish` here and re-wrapped on
/// arrival. Subscriptions travel as flat records rather than trie nodes.
pub struct MigratedSession {
	pub subscriptions: Vec<MigratedSub>,
	pub inflight: HashMap<u16, InflightMessage>,
	pub incoming_qos2: HashMap<u16, Publish>,
	pub next_pkid: u16,
	/// QoS > 0 messages buffered while offline, as `(publish, qos, retain, sub_ids)`.
	pub offline: Vec<(Publish, QoS, bool, Vec<usize>)>,
}

/// One migrated subscription (a flattened topic-trie entry).
pub struct MigratedSub {
	pub filter: String,
	pub qos: QoS,
	pub nolocal: bool,
	pub retain_as_published: bool,
	pub share_group: Option<String>,
	pub sub_id: Option<usize>,
}
