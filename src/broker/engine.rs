use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalSender;
use mqttbytes::{v5::Publish, QoS};

use crate::broker::topic_trie::{filter_matches, TopicTrie};

/// MQTT 5 Session Expiry Interval sentinel meaning "the session never expires".
const SESSION_NEVER_EXPIRES: u32 = u32::MAX;

/// Upper bound on QoS > 0 messages buffered for a suspended (offline) session.
/// Prevents an unbounded backlog for a client that never returns; the oldest
/// messages are dropped once the queue is full.
const OFFLINE_QUEUE_LIMIT: usize = 1024;

/// Stage of an outbound QoS 1/2 message awaiting acknowledgement. Held per
/// in-flight packet id so the exchange can be resumed and retransmitted after a
/// reconnect.
pub enum InflightState {
	/// QoS 1 PUBLISH sent, awaiting PUBACK.
	Qos1,
	/// QoS 2 PUBLISH sent, awaiting PUBREC.
	Qos2Pending,
	/// QoS 2 PUBREL sent, awaiting PUBCOMP.
	Qos2Released,
}

/// An outbound QoS 1/2 message in flight: its stage plus the PUBLISH itself, so
/// it can be retransmitted (with the DUP flag) when a session resumes.
pub struct InflightMessage {
	pub publish: Publish,
	pub state: InflightState,
}

/// The durable QoS state a connection hands to its session when it disconnects,
/// and receives back when the session is resumed. While the client is connected
/// this state lives in the [`Connection`](crate::server::connection::Connection)
/// (hot path, no shared-state borrow); it only rests here between connections.
#[derive(Default)]
pub struct SessionSnapshot {
	/// Outbound QoS 1/2 messages we sent but the client has not fully acked.
	pub inflight: HashMap<u16, InflightMessage>,
	/// Inbound QoS 2 messages received (PUBLISH) but not yet released (PUBREL).
	pub incoming_qos2: HashMap<u16, Publish>,
	/// Where the outbound packet-id allocator left off.
	pub next_pkid: u16,
}

/// Returns the lower of two QoS levels.
fn min_qos(a: QoS, b: QoS) -> QoS {
	if (a as u8) <= (b as u8) { a } else { b }
}

/// A message routed toward a connection for delivery to its client.
///
/// The `publish` is shared via `Rc` so one message fans out to many subscribers
/// on the same shard without re-allocating; `qos` is the effective QoS for this
/// particular subscriber (`min(publish QoS, granted QoS)`). The receiving
/// connection assigns its own packet id when `qos > 0`.
pub struct Delivery {
	pub publish: Rc<Publish>,
	pub qos: QoS,
}

/// Sender half of a connection's mailbox.
///
/// `LocalSender` is single-owner (not `Clone`), so each connection's sender is
/// stored exactly once — in its [`Session`] — and subscriptions refer to the
/// client by `client_id` rather than holding their own sender.
pub type Mailbox = LocalSender<Delivery>;

/// Durable per-client session state, keyed by `client_id` in [`ShardState`].
///
/// A session outlives the [`Connection`](crate::server::connection::Connection)
/// that created it: on disconnect with a non-zero Session Expiry Interval the
/// session is *suspended* (its live mailbox dropped, subscriptions retained in
/// the trie) rather than destroyed, so a later reconnect with the same client id
/// resumes it. The subscriptions themselves live in the shared [`TopicTrie`]
/// keyed by `client_id`, so they persist across reconnects without being copied
/// here.
struct Session {
	/// Live mailbox while the client is connected; `None` while suspended.
	mailbox: Option<Mailbox>,
	/// Deadline after which a suspended session is discarded. `None` means either
	/// the client is currently connected, or the session never expires
	/// (Session Expiry Interval `0xFFFFFFFF`); the two are told apart by whether
	/// `mailbox` is `Some`.
	expires_at: Option<Instant>,
	/// Bumped every time a connection (re)opens this session. A departing
	/// connection only tears its session down if this still matches the
	/// generation it opened, so a takeover by a newer connection is never
	/// clobbered by the old one's cleanup.
	generation: u64,
	/// Durable QoS state, populated only while the session is suspended (the
	/// connected client holds the live copy). Restored on resume.
	snapshot: SessionSnapshot,
	/// QoS > 0 messages that matched while the client was offline, delivered in
	/// order when it reconnects. Bounded by [`OFFLINE_QUEUE_LIMIT`].
	offline_queue: VecDeque<Delivery>,
}

/// Outcome of opening a session at CONNECT, returned to the connection so it can
/// set CONNACK `session_present`, remember which generation it owns, and restore
/// any durable state carried over from a previous connection.
pub struct SessionHandle {
	/// Whether an existing session was resumed (drives CONNACK `session_present`).
	pub resumed: bool,
	/// The generation this connection now owns; passed back to `close_session`.
	pub generation: u64,
	/// Durable QoS state to restore into the connection (empty when fresh).
	pub snapshot: SessionSnapshot,
	/// Messages buffered while the session was offline, to flush after CONNACK
	/// (empty when fresh).
	pub offline_queue: VecDeque<Delivery>,
}

/// Per-shard broker state.
///
/// Single-threaded and shared between every connection on the shard via
/// `Rc<RefCell<>>`. No locks are needed: in the thread-per-core model no other
/// core ever touches this memory.
#[derive(Default)]
pub struct ShardState {
	/// Sessions on this shard, keyed by client id. Present while a client is
	/// connected and, if it has a non-zero expiry, while suspended after it
	/// disconnects.
	sessions: HashMap<String, Session>,
	/// Monotonic session-generation counter (see [`Session::generation`]).
	next_generation: u64,
	/// Subscription index: wildcard-aware topic trie keyed by filter.
	trie: TopicTrie,
	/// Last retained message per topic. Replicated on every shard (each retained
	/// publish is broadcast to all shards), so a new subscriber finds matches
	/// locally.
	retained: HashMap<String, Publish>,
	/// Senders to every other shard in the full channel mesh. `None` until the
	/// shard joins the mesh in `worker::init`.
	mesh: Option<Senders<Publish>>,
}

impl ShardState {
	/// Creates a fresh, shareable handle to this shard's state.
	pub fn new() -> Rc<RefCell<Self>> {
		Rc::new(RefCell::new(Self::default()))
	}

	/// Stores this shard's mesh senders so publishes can be forwarded to peers.
	pub fn set_mesh(&mut self, senders: Senders<Publish>) {
		self.mesh = Some(senders);
	}

	/// Forwards a publish to every *other* shard in the mesh. Each peer runs its
	/// own local `route`, so a remote subscriber receives it identically.
	///
	/// `try_send_to` is non-blocking (drop-on-full, matching QoS 0 semantics) and
	/// the publisher never stalls on a slow peer. Self is skipped — the local
	/// fan-out is handled directly by `route`.
	pub fn broadcast(&self, publish: &Publish) {
		let Some(senders) = &self.mesh else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			let _ = senders.try_send_to(idx, publish.clone());
		}
	}

	/// Opens (or resumes) a session for a connecting client, installing its live
	/// `mailbox`. Called once per CONNECT.
	///
	/// - `clean_start = true` discards any existing session and its subscriptions,
	///   then starts fresh (`resumed = false`).
	/// - `clean_start = false` resumes an existing session if one is present —
	///   re-attaching the mailbox and clearing its expiry, with the subscriptions
	///   already armed in the trie (`resumed = true`) — otherwise starts fresh.
	///
	/// If a session for this client id was still *online* (a live connection),
	/// installing the new mailbox drops the old sender, which closes the old
	/// connection's channel and ends it: a session takeover. The returned
	/// generation lets the displaced connection detect that it was taken over.
	pub fn open_session(
		&mut self,
		client_id: &str,
		mailbox: Mailbox,
		clean_start: bool,
	) -> SessionHandle {
		self.next_generation += 1;
		let generation = self.next_generation;

		if clean_start {
			if self.sessions.remove(client_id).is_some() {
				self.trie.remove_client(client_id);
			}
		} else if let Some(existing) = self.sessions.get_mut(client_id) {
			existing.mailbox = Some(mailbox);
			existing.expires_at = None;
			existing.generation = generation;
			// Hand the durable state back to the resuming connection.
			return SessionHandle {
				resumed: true,
				generation,
				snapshot: std::mem::take(&mut existing.snapshot),
				offline_queue: std::mem::take(&mut existing.offline_queue),
			};
		}

		self.sessions.insert(
			client_id.to_string(),
			Session {
				mailbox: Some(mailbox),
				expires_at: None,
				generation,
				snapshot: SessionSnapshot::default(),
				offline_queue: VecDeque::new(),
			},
		);
		SessionHandle {
			resumed: false,
			generation,
			snapshot: SessionSnapshot::default(),
			offline_queue: VecDeque::new(),
		}
	}

	/// Subscribes a client to a topic filter with a granted QoS. The filter may
	/// contain the `+` and `#` wildcards. Re-subscribing replaces the prior entry.
	pub fn subscribe(&mut self, filter: &str, client_id: &str, qos: QoS) {
		self.trie.insert(filter, client_id, qos);
	}

	/// Removes a single subscription (used by UNSUBSCRIBE).
	pub fn unsubscribe(&mut self, filter: &str, client_id: &str) {
		self.trie.remove(filter, client_id);
	}

	/// Routes one publish on this shard: updates the retain table if the retain
	/// flag is set, then fans it out to local subscribers. Shared by the local
	/// publish path and the mesh drain task.
	pub fn deliver_local(&mut self, mut publish: Publish) {
		if publish.retain {
			self.update_retain(&publish);
		}
		// The retain flag is only set when a message is delivered because of a new
		// subscription; on live fan-out it is always cleared.
		publish.retain = false;
		self.route(Rc::new(publish));
	}

	/// Inserts or clears a retained message. A retained publish with an empty
	/// payload removes the stored message (MQTT spec).
	fn update_retain(&mut self, publish: &Publish) {
		if publish.payload.is_empty() {
			self.retained.remove(&publish.topic);
		} else {
			self.retained.insert(publish.topic.clone(), publish.clone());
		}
	}

	/// Returns the retained messages whose topic matches a subscription `filter`,
	/// for replay to a newly-subscribed client.
	pub fn retained_matching(&self, filter: &str) -> Vec<Publish> {
		self.retained
			.values()
			.filter(|p| filter_matches(filter, &p.topic))
			.cloned()
			.collect()
	}

	/// Fans a message out to every local subscriber whose filter matches the
	/// publish topic.
	///
	/// Wildcard-aware via the topic trie. A client matching through several
	/// overlapping filters receives a single copy, at the highest QoS it was
	/// granted across those filters (capped by the publish QoS). Uses `try_send`
	/// so a slow or full consumer never blocks the publisher.
	fn route(&mut self, publish: Rc<Publish>) {
		let mut matches = Vec::new();
		self.trie.matching(&publish.topic, &mut matches);

		// Collapse overlapping subscriptions to the best granted QoS per client.
		// Owned keys so the borrow of the trie ends before we touch `sessions`.
		let mut best: HashMap<String, QoS> = HashMap::new();
		for sub in matches {
			let entry = best
				.entry(sub.client_id.clone())
				.or_insert(sub.qos);
			if (sub.qos as u8) > (*entry as u8) {
				*entry = sub.qos;
			}
		}

		for (client_id, granted) in best {
			let Some(session) = self.sessions.get_mut(&client_id) else {
				continue;
			};
			let qos = min_qos(publish.qos, granted);

			match &session.mailbox {
				// Online: hand straight to the live mailbox.
				Some(mailbox) => {
					let _ = mailbox.try_send(Delivery {
						publish: publish.clone(),
						qos,
					});
				}
				// Suspended: buffer QoS > 0 for delivery on resume; drop QoS 0.
				None if qos != QoS::AtMostOnce => {
					if session.offline_queue.len() >= OFFLINE_QUEUE_LIMIT {
						session.offline_queue.pop_front();
					}
					session.offline_queue.push_back(Delivery {
						publish: publish.clone(),
						qos,
					});
				}
				None => {}
			}
		}
	}

	/// Ends a connection's hold on its session when the socket closes (clean
	/// DISCONNECT or EOF/error).
	///
	/// `generation` must be the value returned by the matching [`open_session`];
	/// if a newer connection has since taken over this client id the generations
	/// differ and this is a no-op, leaving the new session untouched.
	///
	/// With `expiry_secs = 0` the session (and its subscriptions) is destroyed
	/// immediately. Otherwise it is *suspended*: the live mailbox is dropped but
	/// the subscriptions stay armed in the trie, and an expiry deadline is set
	/// (unless the interval is `0xFFFFFFFF`, meaning it never expires) so
	/// [`sweep_expired`](Self::sweep_expired) can reclaim it later.
	///
	/// Returns `true` if this connection still owned the session (generations
	/// matched) and it was closed, or `false` if it had already been taken over —
	/// the caller uses this to decide whether to publish the Will Message (a
	/// takeover must not trigger the displaced connection's will).
	pub fn close_session(
		&mut self,
		client_id: &str,
		generation: u64,
		expiry_secs: u32,
		snapshot: SessionSnapshot,
		mut pending: VecDeque<Delivery>,
	) -> bool {
		let Some(session) = self.sessions.get_mut(client_id) else {
			return false;
		};
		if session.generation != generation {
			return false;
		}

		if expiry_secs == 0 {
			self.sessions.remove(client_id);
			self.trie.remove_client(client_id);
		} else {
			session.mailbox = None;
			session.snapshot = snapshot;
			// Messages held back by the outbound window were already dequeued, so
			// they precede anything that arrives while suspended.
			pending.append(&mut session.offline_queue);
			session.offline_queue = pending;
			session.expires_at = (expiry_secs != SESSION_NEVER_EXPIRES)
				.then(|| Instant::now() + Duration::from_secs(u64::from(expiry_secs)));
		}
		true
	}

	/// Drops the live mailbox of every session on this shard. Each connected
	/// client's mailbox channel closes, waking its connection with a closed
	/// receiver (`Outgoing(None)`) so it can disconnect cleanly during shutdown.
	/// The sessions themselves are left intact for the connections' own cleanup.
	pub fn shutdown_connections(&mut self) {
		for session in self.sessions.values_mut() {
			session.mailbox = None;
		}
	}

	/// Discards every suspended session whose expiry deadline has passed, along
	/// with its subscriptions. Driven periodically by a per-shard timer task.
	pub fn sweep_expired(&mut self) {
		let now = Instant::now();
		let expired: Vec<String> = self
			.sessions
			.iter()
			.filter(|(_, s)| s.mailbox.is_none() && s.expires_at.is_some_and(|d| d <= now))
			.map(|(id, _)| id.clone())
			.collect();

		for id in expired {
			self.sessions.remove(&id);
			self.trie.remove_client(&id);
		}
	}
}
