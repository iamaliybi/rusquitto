//! Durable per-client session state: the values a
//! [`Connection`](crate::server::connection::Connection) hands to the shard when
//! it disconnects and receives back on resume. The live routing logic that owns
//! them lives in [`shard`](crate::broker::shard); the one-message-in-flight
//! [`Delivery`] type lives in [`delivery`](crate::broker::delivery).

use std::collections::{HashMap, VecDeque};

use mqttbytes::v5::Publish;

use crate::broker::delivery::Delivery;

/// Stage of an outbound QoS 1/2 message awaiting acknowledgement, held per
/// in-flight packet id so the exchange can resume after a reconnect.
#[derive(Clone, Copy)]
pub enum InflightState {
	/// QoS 1 PUBLISH sent, awaiting PUBACK.
	Qos1,
	/// QoS 2 PUBLISH sent, awaiting PUBREC.
	Qos2Pending,
	/// QoS 2 PUBREL sent, awaiting PUBCOMP.
	Qos2Released,
}

/// An outbound QoS 1/2 message in flight: its stage plus the PUBLISH itself, so
/// it can be retransmitted with the DUP flag when a session resumes.
#[derive(Clone)]
pub struct InflightMessage {
	pub publish: Publish,
	pub state: InflightState,
}

/// The durable QoS state a connection hands to its session on disconnect and
/// receives back on resume. While connected this lives in the `Connection` (hot
/// path, no shared borrow); it only rests here between connections.
#[derive(Default, Clone)]
pub struct SessionSnapshot {
	/// Outbound QoS 1/2 messages sent but not fully acknowledged.
	pub inflight: HashMap<u16, InflightMessage>,
	/// Inbound QoS 2 messages received (PUBLISH) but not yet released (PUBREL).
	pub incoming_qos2: HashMap<u16, Publish>,
	/// Where the outbound packet-id allocator left off.
	pub next_pkid: u16,
}

/// A durable session captured for on-disk persistence: the same owned state as a
/// [`MigratedSession`](crate::broker::messages::MigratedSession) plus the identity and
/// remaining expiry needed to restore it standalone at startup.
pub struct PersistedSession {
	pub client_id: String,
	/// Remaining seconds until the session expires; [`u32::MAX`] means it never
	/// expires (Session Expiry Interval `0xFFFFFFFF`).
	pub expiry_secs: u32,
	pub session: crate::broker::messages::MigratedSession,
}

/// Outcome of opening a session at CONNECT, returned so the connection can set
/// CONNACK `session_present`, remember its generation, and restore durable state.
pub struct SessionHandle {
	/// Whether an existing session was resumed (drives CONNACK `session_present`).
	pub resumed: bool,
	/// The generation this connection owns; passed back to `close_session`.
	pub generation: u64,
	/// Durable QoS state to restore (empty when fresh).
	pub snapshot: SessionSnapshot,
	/// Messages buffered while offline, flushed after CONNACK (empty when fresh).
	pub offline_queue: VecDeque<Delivery>,
}
