//! Durable per-client session state and the message-delivery value types.
//!
//! These are the values a [`Connection`](crate::server::connection::Connection)
//! hands to the shard when it disconnects and receives back on resume. The live
//! routing logic that owns them lives in [`shard`](crate::broker::shard).

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use glommio::channels::local_channel::LocalSender;
use mqttbytes::{QoS, v5::Publish};

/// Upper bound on QoS > 0 messages buffered for a suspended (offline) session.
/// The oldest are dropped once full, so a client that never returns can't grow an
/// unbounded backlog.
pub const OFFLINE_QUEUE_LIMIT: usize = 1024;

/// Stage of an outbound QoS 1/2 message awaiting acknowledgement, held per
/// in-flight packet id so the exchange can resume after a reconnect.
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
pub struct InflightMessage {
	pub publish: Publish,
	pub state: InflightState,
}

/// The durable QoS state a connection hands to its session on disconnect and
/// receives back on resume. While connected this lives in the `Connection` (hot
/// path, no shared borrow); it only rests here between connections.
#[derive(Default)]
pub struct SessionSnapshot {
	/// Outbound QoS 1/2 messages sent but not fully acknowledged.
	pub inflight: HashMap<u16, InflightMessage>,
	/// Inbound QoS 2 messages received (PUBLISH) but not yet released (PUBREL).
	pub incoming_qos2: HashMap<u16, Publish>,
	/// Where the outbound packet-id allocator left off.
	pub next_pkid: u16,
}

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
