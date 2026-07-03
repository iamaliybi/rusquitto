use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalSender;
use mqttbytes::{v5::Publish, QoS};

use crate::broker::topic_trie::{filter_matches, SubOptions, TopicTrie};

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

/// Returns the lower of two QoS levels (e.g. the granted QoS is
/// `min(requested, server max)`, and delivery is `min(publish, granted)`).
pub(crate) fn min_qos(a: QoS, b: QoS) -> QoS {
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
	/// The retain flag to set on the delivered PUBLISH. Cleared on ordinary live
	/// fan-out, but kept for a Retain-As-Published subscriber, and set for a
	/// retained-message replay.
	pub retain: bool,
	/// Subscription Identifiers to echo on the delivered PUBLISH (MQTT 5), gathered
	/// from every matching subscription of this client. Usually empty or one.
	pub sub_ids: Vec<usize>,
}

/// The chosen subscription for a client during routing: the options of its
/// highest-QoS matching filter, plus the identifiers of *all* its matching
/// subscriptions (MQTT 5 delivers every matching Subscription Identifier).
struct Match {
	qos: QoS,
	nolocal: bool,
	retain_as_published: bool,
	sub_ids: Vec<usize>,
}

/// Sender half of a connection's mailbox.
///
/// `LocalSender` is single-owner (not `Clone`), so each connection's sender is
/// stored exactly once — in its [`Session`] — and subscriptions refer to the
/// client by `client_id` rather than holding their own sender.
pub type Mailbox = LocalSender<Delivery>;

/// A message crossing the inter-shard channel mesh. Most carry a `Publish` to be
/// re-routed on the receiving shard; a smaller number carry a [`SessionControl`]
/// message for cross-shard session migration. The control variant is boxed so the
/// common publish path keeps the enum (and thus the mesh ring buffers) small.
pub enum MeshMsg {
	Publish(Publish),
	Control(Box<SessionControl>),
}

/// Cross-shard session-migration protocol, exchanged over the mesh.
///
/// A reconnecting client can land — via the `SO_REUSEPORT` 4-tuple hash on its new
/// ephemeral port — on a different shard than the one holding its suspended
/// session. Since every shard shares one listening address there is nothing to
/// redirect the client to, so the *session* moves instead: the shard the client
/// reached broadcasts a [`Claim`], and whichever peer owns the session replies
/// with a [`Handoff`] carrying it.
///
/// [`Claim`]: SessionControl::Claim
/// [`Handoff`]: SessionControl::Handoff
pub enum SessionControl {
	/// "Client `client_id` just (re)connected to me (`requester`); if you hold its
	/// session, hand it over." `resume = false` — a Clean Start connect — instead
	/// asks peers to *discard* any session they hold for this client id.
	Claim {
		client_id: String,
		/// Mesh peer id of the shard to send the [`Handoff`](SessionControl::Handoff)
		/// reply back to.
		requester: usize,
		resume: bool,
	},
	/// Reply to a [`Claim`](SessionControl::Claim): the owning peer's session for
	/// `client_id`, or `None` if it held none (or the claim was a discard).
	Handoff {
		client_id: String,
		session: Option<MigratedSession>,
	},
}

/// A whole session serialized for migration to another shard.
///
/// Carries owned data only — the mesh moves values between executors, so the
/// offline queue's `Rc<Publish>` is unwrapped to an owned `Publish` here and
/// re-wrapped on arrival. Subscriptions travel as flat tuples rather than trie
/// nodes.
pub struct MigratedSession {
	/// The client's subscriptions (filter, granted QoS, and options).
	pub subscriptions: Vec<MigratedSub>,
	/// Outbound QoS 1/2 messages awaiting acknowledgement.
	pub inflight: HashMap<u16, InflightMessage>,
	/// Inbound QoS 2 messages received but not yet released.
	pub incoming_qos2: HashMap<u16, Publish>,
	/// Where the outbound packet-id allocator left off.
	pub next_pkid: u16,
	/// QoS > 0 messages buffered while offline, as owned
	/// `(publish, qos, retain, sub_ids)`.
	pub offline: Vec<(Publish, QoS, bool, Vec<usize>)>,
}

/// One migrated subscription (a flattened [`TopicTrie`] entry).
pub struct MigratedSub {
	pub filter: String,
	pub qos: QoS,
	pub nolocal: bool,
	pub retain_as_published: bool,
	pub share_group: Option<String>,
	pub sub_id: Option<usize>,
}

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
	/// A Will Message armed with a non-zero Will Delay Interval: `(will, deadline)`.
	/// Published by [`sweep_expired`](ShardState::sweep_expired) once the deadline
	/// passes (or the session ends first), and cancelled if the client reconnects.
	pending_will: Option<(Publish, Instant)>,
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
	/// shard joins the mesh in `worker::init`. Held in an `Rc` so a connection can
	/// clone the handle and `await` a cross-shard send without keeping this
	/// `ShardState` borrowed across the await.
	mesh: Option<Rc<Senders<MeshMsg>>>,
	/// In-flight cross-shard session claims this shard is awaiting, keyed by client
	/// id. When a `Handoff` reply arrives on the mesh it is delivered through the
	/// matching sender, waking the CONNECT handler blocked on the claim.
	pending_claims: HashMap<String, LocalSender<Option<MigratedSession>>>,
	/// Round-robin cursor per shared-subscription group (keyed by `group` + the
	/// matched filter), advanced each time a message is load-balanced to a member so
	/// deliveries rotate across the group.
	shared_cursor: HashMap<String, usize>,
}

impl ShardState {
	/// Creates a fresh, shareable handle to this shard's state.
	pub fn new() -> Rc<RefCell<Self>> {
		Rc::new(RefCell::new(Self::default()))
	}

	/// Stores this shard's mesh senders so publishes can be forwarded to peers.
	pub fn set_mesh(&mut self, senders: Senders<MeshMsg>) {
		self.mesh = Some(Rc::new(senders));
	}

	/// A cloneable handle to this shard's mesh senders. Lets the publish path
	/// `await` a cross-shard `send_to` (backpressure for QoS > 0) after dropping the
	/// `ShardState` borrow, rather than dropping the message with `try_send_to`.
	pub fn mesh_senders(&self) -> Option<Rc<Senders<MeshMsg>>> {
		self.mesh.clone()
	}

	/// Forwards a publish to every *other* shard in the mesh, best-effort. Each peer
	/// runs its own local `route`, so a remote subscriber receives it identically.
	///
	/// `try_send_to` is non-blocking (drop-on-full), so the caller never stalls on a
	/// slow peer — used for QoS 0 and broker-internal (`$SYS`) publishes where a drop
	/// is acceptable. The QoS > 0 publish path instead awaits [`mesh_senders`]'s
	/// `send_to` for backpressure. Self is skipped — local fan-out is done by `route`.
	///
	/// [`mesh_senders`]: Self::mesh_senders
	pub fn broadcast(&self, publish: &Publish) {
		let Some(senders) = &self.mesh else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			let _ = senders.try_send_to(idx, MeshMsg::Publish(publish.clone()));
		}
	}

	/// The number of *other* shards in the mesh (peers this shard can talk to).
	/// Zero for a single-shard broker, which short-circuits cross-shard migration.
	pub fn mesh_peers(&self) -> usize {
		self.mesh
			.as_ref()
			.map_or(0, |s| s.nr_consumers().saturating_sub(1))
	}

	/// Sends a single control message to one peer shard (best effort, drop-on-full).
	fn send_control_to(&self, peer: usize, control: SessionControl) {
		if let Some(senders) = &self.mesh {
			let _ = senders.try_send_to(peer, MeshMsg::Control(Box::new(control)));
		}
	}

	/// Broadcasts a session [`Claim`](SessionControl::Claim) to every peer shard.
	/// With `resume = true` peers holding a suspended session hand it back; with
	/// `resume = false` (Clean Start) they discard it instead. A no-op when there
	/// are no peers.
	pub fn broadcast_claim(&self, client_id: &str, resume: bool) {
		let Some(senders) = &self.mesh else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			let _ = senders.try_send_to(
				idx,
				MeshMsg::Control(Box::new(SessionControl::Claim {
					client_id: client_id.to_string(),
					requester: me,
					resume,
				})),
			);
		}
	}

	/// Registers a pending claim: the CONNECT handler awaits `tx`'s receiver while
	/// this sender is delivered any [`Handoff`](SessionControl::Handoff) replies.
	pub fn register_claim(
		&mut self,
		client_id: String,
		tx: LocalSender<Option<MigratedSession>>,
	) {
		self.pending_claims.insert(client_id, tx);
	}

	/// Removes a pending claim once the CONNECT handler is done waiting.
	pub fn unregister_claim(&mut self, client_id: &str) {
		self.pending_claims.remove(client_id);
	}

	/// Dispatches a control message received from a peer over the mesh.
	pub fn on_control(&mut self, control: SessionControl) {
		match control {
			SessionControl::Claim {
				client_id,
				requester,
				resume,
			} => self.handle_claim(client_id, requester, resume),
			SessionControl::Handoff { client_id, session } => {
				// Route the reply to whichever CONNECT handler is awaiting it. If none
				// is (timed out, or a stray/duplicate reply), it is simply dropped.
				if let Some(tx) = self.pending_claims.get(&client_id) {
					let _ = tx.try_send(session);
				}
			}
		}
	}

	/// Handles a peer's session [`Claim`](SessionControl::Claim): reply with the
	/// session if we own one and this is a resume, otherwise discard/none.
	fn handle_claim(&mut self, client_id: String, requester: usize, resume: bool) {
		// Decide with an immutable peek first so the borrow ends before we mutate.
		let session = match self.sessions.get(&client_id).map(|s| s.mailbox.is_none()) {
			// Suspended session and the client wants to resume: migrate it wholesale.
			Some(true) if resume => Some(self.extract_session(&client_id)),
			// A still-live session (cross-shard takeover) or a Clean Start discard:
			// drop it here — dropping the mailbox also disconnects the live client —
			// without migrating any durable state.
			Some(_) => {
				self.sessions.remove(&client_id);
				self.trie.remove_client(&client_id);
				None
			}
			// Nothing for this client id.
			None => None,
		};
		self.send_control_to(requester, SessionControl::Handoff { client_id, session });
	}

	/// Removes a suspended session from this shard and packages it for migration:
	/// its subscriptions (pulled from the trie), durable QoS state, and offline
	/// queue (unwrapped from `Rc` to owned publishes).
	fn extract_session(&mut self, client_id: &str) -> MigratedSession {
		let subscriptions = self
			.trie
			.take_client(client_id)
			.into_iter()
			.map(|f| MigratedSub {
				filter: f.filter,
				qos: f.qos,
				nolocal: f.nolocal,
				retain_as_published: f.retain_as_published,
				share_group: f.share_group,
				sub_id: f.sub_id,
			})
			.collect();

		let session = self
			.sessions
			.remove(client_id)
			.expect("extract_session called for a client without a session");
		let offline = session
			.offline_queue
			.into_iter()
			.map(|d| ((*d.publish).clone(), d.qos, d.retain, d.sub_ids))
			.collect();

		MigratedSession {
			subscriptions,
			inflight: session.snapshot.inflight,
			incoming_qos2: session.snapshot.incoming_qos2,
			next_pkid: session.snapshot.next_pkid,
			offline,
		}
	}

	/// Installs a session migrated from another shard onto the freshly-opened
	/// local session for `client_id`: re-arms its subscriptions in the trie and
	/// returns the durable QoS state and offline queue for the connection to load.
	///
	/// The local session must already exist (just created by `open_session`); its
	/// expiry is governed by the current CONNECT, so the migrated deadline is not
	/// carried over.
	pub fn install_migrated(
		&mut self,
		client_id: &str,
		migrated: MigratedSession,
	) -> (SessionSnapshot, VecDeque<Delivery>) {
		for sub in migrated.subscriptions {
			self.trie.insert(
				&sub.filter,
				client_id,
				SubOptions {
					qos: sub.qos,
					nolocal: sub.nolocal,
					retain_as_published: sub.retain_as_published,
					share_group: sub.share_group.as_deref(),
					sub_id: sub.sub_id,
				},
			);
		}

		let offline = migrated
			.offline
			.into_iter()
			.map(|(publish, qos, retain, sub_ids)| Delivery {
				publish: Rc::new(publish),
				qos,
				retain,
				sub_ids,
			})
			.collect();

		let snapshot = SessionSnapshot {
			inflight: migrated.inflight,
			incoming_qos2: migrated.incoming_qos2,
			next_pkid: migrated.next_pkid,
		};
		(snapshot, offline)
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
			// Reconnecting cancels any armed (delayed) Will Message.
			existing.pending_will = None;
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
				pending_will: None,
			},
		);
		SessionHandle {
			resumed: false,
			generation,
			snapshot: SessionSnapshot::default(),
			offline_queue: VecDeque::new(),
		}
	}

	/// Subscribes a client to a topic filter with a granted QoS and its options.
	/// The filter may contain the `+` and `#` wildcards. Re-subscribing replaces
	/// the prior entry. Returns `true` if the subscription is new (for Retain
	/// Handling).
	pub fn subscribe(&mut self, filter: &str, client_id: &str, opts: SubOptions) -> bool {
		self.trie.insert(filter, client_id, opts)
	}

	/// Removes a single subscription (used by UNSUBSCRIBE). `share_group` selects
	/// the ordinary (`None`) or shared entry.
	pub fn unsubscribe(&mut self, filter: &str, client_id: &str, share_group: Option<&str>) {
		self.trie.remove(filter, client_id, share_group);
	}

	/// Routes one publish on this shard: updates the retain table if the retain
	/// flag is set, then fans it out to local subscribers. Shared by the local
	/// publish path and the mesh drain task.
	///
	/// `publisher` is the client id that produced this message, when it is local
	/// to this shard (`None` for mesh-forwarded publishes and broker-internal
	/// ones); it drives the No Local subscription option.
	pub fn deliver_local(&mut self, mut publish: Publish, publisher: Option<&str>) {
		let was_retained = publish.retain;
		if was_retained {
			self.update_retain(&publish);
		}
		// Clear the wire retain flag; each delivery's flag is decided per subscriber
		// in `route` (kept only for Retain-As-Published subscribers).
		publish.retain = false;
		self.route(Rc::new(publish), publisher, was_retained);
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

	/// Fans a message out to the local subscribers whose filter matches the publish
	/// topic.
	///
	/// Wildcard-aware via the topic trie. An *ordinary* subscriber matching through
	/// several overlapping filters receives a single copy, using the options of its
	/// highest-QoS matching subscription (capped by the publish QoS). A *shared*
	/// subscription group (`$share/{group}/…`) instead delivers to exactly one of its
	/// members, chosen round-robin, so the group load-balances. Honours the No Local
	/// and Retain As Published options. Uses `try_send` so a slow or full consumer
	/// never blocks the publisher.
	fn route(&mut self, publish: Rc<Publish>, publisher: Option<&str>, was_retained: bool) {
		let mut matches = Vec::new();
		self.trie.matching(&publish.topic, &mut matches);

		// Collapse overlapping subscriptions to one Match per client, keeping the
		// options of the highest-QoS match. Ordinary subscribers go in `best` (each
		// gets a copy); shared subscribers are bucketed by group name in `groups`
		// (one member of each is picked below). Owned keys so the trie borrow ends
		// before we touch `sessions`.
		let mut best: HashMap<String, Match> = HashMap::new();
		let mut groups: HashMap<String, HashMap<String, Match>> = HashMap::new();
		for sub in matches {
			let bucket = match &sub.share_group {
				None => &mut best,
				Some(group) => {
					// No Local: the publisher is never a load-balance candidate for
					// its own shared subscription, so it is dropped from the group here.
					if sub.nolocal && publisher == Some(sub.client_id.as_str()) {
						continue;
					}
					groups.entry(group.clone()).or_default()
				}
			};
			let entry = bucket.entry(sub.client_id.clone()).or_insert(Match {
				qos: sub.qos,
				nolocal: sub.nolocal,
				retain_as_published: sub.retain_as_published,
				sub_ids: Vec::new(),
			});
			if (sub.qos as u8) > (entry.qos as u8) {
				entry.qos = sub.qos;
				entry.nolocal = sub.nolocal;
				entry.retain_as_published = sub.retain_as_published;
			}
			// Every matching subscription's identifier is delivered (MQTT 5),
			// regardless of which one won the QoS tie-break.
			if let Some(id) = sub.sub_id
				&& !entry.sub_ids.contains(&id)
			{
				entry.sub_ids.push(id);
			}
		}

		// Ordinary subscribers: one copy each.
		for (client_id, m) in best {
			// No Local: never echo a message back to the client that published it.
			if m.nolocal && publisher == Some(client_id.as_str()) {
				continue;
			}
			let qos = min_qos(publish.qos, m.qos);
			let retain = was_retained && m.retain_as_published;
			self.deliver_to(&client_id, &publish, qos, retain, m.sub_ids);
		}

		// Shared groups: one member each, round-robin (preferring connected members
		// so a message isn't parked in an offline queue while a peer is live).
		for (group, members) in groups {
			let mut ids: Vec<String> = members.keys().cloned().collect();
			if ids.is_empty() {
				continue;
			}
			ids.sort();
			let online: Vec<String> = ids
				.iter()
				.filter(|id| {
					self.sessions
						.get(*id)
						.is_some_and(|s| s.mailbox.is_some())
				})
				.cloned()
				.collect();
			let pool = if online.is_empty() { ids } else { online };

			let cursor = self.shared_cursor.entry(group).or_insert(0);
			let client_id = pool[*cursor % pool.len()].clone();
			*cursor = cursor.wrapping_add(1);

			let m = &members[&client_id];
			let qos = min_qos(publish.qos, m.qos);
			let retain = was_retained && m.retain_as_published;
			self.deliver_to(&client_id, &publish, qos, retain, m.sub_ids.clone());
		}
	}

	/// Delivers one message to a single client's session: straight to its live
	/// mailbox if connected, otherwise buffered in its offline queue (QoS > 0 only;
	/// QoS 0 is dropped for a suspended session). `sub_ids` are the Subscription
	/// Identifiers to echo on the delivered PUBLISH.
	fn deliver_to(
		&mut self,
		client_id: &str,
		publish: &Rc<Publish>,
		qos: QoS,
		retain: bool,
		sub_ids: Vec<usize>,
	) {
		let Some(session) = self.sessions.get_mut(client_id) else {
			return;
		};
		match &session.mailbox {
			Some(mailbox) => {
				let _ = mailbox.try_send(Delivery {
					publish: publish.clone(),
					qos,
					retain,
					sub_ids,
				});
			}
			None if qos != QoS::AtMostOnce => {
				if session.offline_queue.len() >= OFFLINE_QUEUE_LIMIT {
					session.offline_queue.pop_front();
				}
				session.offline_queue.push_back(Delivery {
					publish: publish.clone(),
					qos,
					retain,
					sub_ids,
				});
			}
			None => {}
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

	/// Arms a delayed Will Message on a suspended session: it fires from
	/// [`sweep_expired`](Self::sweep_expired) once `delay_secs` elapses, unless the
	/// client reconnects first (which clears it in [`open_session`](Self::open_session)).
	/// A no-op if the session was taken over (generation mismatch) or already gone.
	pub fn arm_will(&mut self, client_id: &str, generation: u64, will: Publish, delay_secs: u32) {
		if let Some(session) = self.sessions.get_mut(client_id)
			&& session.generation == generation
		{
			let deadline = Instant::now() + Duration::from_secs(u64::from(delay_secs));
			session.pending_will = Some((will, deadline));
		}
	}

	/// Discards every suspended session whose expiry deadline has passed (along with
	/// its subscriptions) and collects any Will Messages that are now due — either
	/// because their delay elapsed or because their session ended first. Driven
	/// periodically by a per-shard timer task, which publishes the returned wills.
	pub fn sweep_expired(&mut self) -> Vec<Publish> {
		let now = Instant::now();
		let mut wills = Vec::new();

		// Fire any delayed wills whose deadline has passed (the session may live on,
		// e.g. a will delay shorter than the session expiry).
		for session in self.sessions.values_mut() {
			if session
				.pending_will
				.as_ref()
				.is_some_and(|(_, deadline)| *deadline <= now)
			{
				wills.push(session.pending_will.take().unwrap().0);
			}
		}

		let expired: Vec<String> = self
			.sessions
			.iter()
			.filter(|(_, s)| s.mailbox.is_none() && s.expires_at.is_some_and(|d| d <= now))
			.map(|(id, _)| id.clone())
			.collect();

		for id in expired {
			// A session ending publishes any still-pending will (the deadline is
			// `min(will_delay, session_expiry)`, so this is the delay==expiry case).
			if let Some(session) = self.sessions.remove(&id) {
				if let Some((will, _)) = session.pending_will {
					wills.push(will);
				}
				self.trie.remove_client(&id);
			}
		}
		wills
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use glommio::channels::local_channel;

	fn pubm(topic: &str, qos: QoS, payload: &[u8], retain: bool) -> Publish {
		let mut p = Publish::new(topic, qos, payload.to_vec());
		p.retain = retain;
		p
	}

	fn opts(
		qos: QoS,
		nolocal: bool,
		retain_as_published: bool,
		share_group: Option<&str>,
		sub_id: Option<usize>,
	) -> SubOptions<'_> {
		SubOptions {
			qos,
			nolocal,
			retain_as_published,
			share_group,
			sub_id,
		}
	}

	/// Installs a *suspended* session (no live mailbox) for `client`, so any message
	/// routed to it lands in its offline queue where the test can inspect it.
	fn arm(state: &mut ShardState, client: &str) {
		state.sessions.insert(
			client.to_string(),
			Session {
				mailbox: None,
				expires_at: None,
				generation: 1,
				snapshot: SessionSnapshot::default(),
				offline_queue: VecDeque::new(),
				pending_will: None,
			},
		);
	}

	/// The offline queue of a client's session.
	fn offline<'a>(state: &'a ShardState, client: &str) -> &'a VecDeque<Delivery> {
		&state.sessions[client].offline_queue
	}

	#[test]
	fn route_fans_out_and_downgrades_qos_to_granted() {
		let mut s = ShardState::default();
		arm(&mut s, "c1");
		s.subscribe("home/+/temp", "c1", opts(QoS::AtLeastOnce, false, false, None, None));

		// A QoS 2 publish to a QoS 1 subscription is delivered at QoS 1.
		s.deliver_local(pubm("home/kitchen/temp", QoS::ExactlyOnce, b"21", false), None);

		let q = offline(&s, "c1");
		assert_eq!(q.len(), 1);
		assert_eq!(q[0].qos, QoS::AtLeastOnce);
	}

	#[test]
	fn route_delivers_one_copy_with_all_matching_sub_ids() {
		let mut s = ShardState::default();
		arm(&mut s, "c1");
		// Two overlapping subscriptions from the same client, different sub ids.
		s.subscribe("a/+", "c1", opts(QoS::AtLeastOnce, false, false, None, Some(1)));
		s.subscribe("a/b", "c1", opts(QoS::AtLeastOnce, false, false, None, Some(2)));

		s.deliver_local(pubm("a/b", QoS::AtLeastOnce, b"x", false), None);

		let q = offline(&s, "c1");
		assert_eq!(q.len(), 1, "one copy, not one per matching filter");
		let mut ids = q[0].sub_ids.clone();
		ids.sort();
		assert_eq!(ids, vec![1, 2]);
	}

	#[test]
	fn route_honours_no_local() {
		let mut s = ShardState::default();
		arm(&mut s, "c1");
		s.subscribe("t", "c1", opts(QoS::AtLeastOnce, true, false, None, None));

		// Publisher is the subscriber -> skipped.
		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"x", false), Some("c1"));
		assert_eq!(offline(&s, "c1").len(), 0);

		// A different publisher -> delivered.
		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"y", false), Some("other"));
		assert_eq!(offline(&s, "c1").len(), 1);
	}

	#[test]
	fn route_retain_as_published_kept_only_for_rap_subscribers() {
		let mut s = ShardState::default();
		arm(&mut s, "keep");
		arm(&mut s, "clear");
		s.subscribe("t", "keep", opts(QoS::AtLeastOnce, false, true, None, None));
		s.subscribe("t", "clear", opts(QoS::AtLeastOnce, false, false, None, None));

		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"x", true), None);

		assert!(offline(&s, "keep")[0].retain, "RAP subscriber keeps retain");
		assert!(!offline(&s, "clear")[0].retain, "ordinary subscriber clears it");
	}

	#[test]
	fn route_shared_group_load_balances_round_robin() {
		let mut s = ShardState::default();
		arm(&mut s, "c1");
		arm(&mut s, "c2");
		s.subscribe("t", "c1", opts(QoS::AtLeastOnce, false, false, Some("g"), None));
		s.subscribe("t", "c2", opts(QoS::AtLeastOnce, false, false, Some("g"), None));

		// Two messages to a two-member group -> one each (members sorted: c1, c2).
		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"1", false), None);
		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"2", false), None);

		assert_eq!(offline(&s, "c1").len(), 1);
		assert_eq!(offline(&s, "c2").len(), 1);
	}

	#[test]
	fn retained_is_stored_matched_and_cleared() {
		let mut s = ShardState::default();
		s.deliver_local(pubm("sensors/temp", QoS::AtMostOnce, b"21", true), None);
		assert_eq!(s.retained_matching("sensors/#").len(), 1);

		// An empty retained payload clears it.
		s.deliver_local(pubm("sensors/temp", QoS::AtMostOnce, b"", true), None);
		assert!(s.retained_matching("sensors/#").is_empty());
	}

	#[test]
	fn open_session_fresh_then_resumes_after_suspend() {
		let mut s = ShardState::default();
		let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
		let h = s.open_session("c1", tx, false);
		assert!(!h.resumed);

		// Suspend (non-zero expiry), then reconnect resumes.
		assert!(s.close_session("c1", h.generation, 60, SessionSnapshot::default(), VecDeque::new()));
		let (tx2, _rx2) = local_channel::new_unbounded::<Delivery>();
		let h2 = s.open_session("c1", tx2, false);
		assert!(h2.resumed);
		assert_ne!(h2.generation, h.generation);
	}

	#[test]
	fn close_session_expiry_zero_destroys_session_and_subs() {
		let mut s = ShardState::default();
		let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
		let h = s.open_session("c1", tx, false);
		s.subscribe("t", "c1", opts(QoS::AtLeastOnce, false, false, None, None));

		assert!(s.close_session("c1", h.generation, 0, SessionSnapshot::default(), VecDeque::new()));
		assert!(!s.sessions.contains_key("c1"));
		let mut m = Vec::new();
		s.trie.matching("t", &mut m);
		assert!(m.is_empty(), "subscriptions removed with the session");
	}

	#[test]
	fn close_session_generation_mismatch_is_noop() {
		let mut s = ShardState::default();
		arm(&mut s, "c1");
		// Wrong generation (a stale connection) must not tear down the session.
		assert!(!s.close_session("c1", 999, 0, SessionSnapshot::default(), VecDeque::new()));
		assert!(s.sessions.contains_key("c1"));
	}

	#[test]
	fn sweep_fires_due_delayed_will_and_reaps_expired_session() {
		let mut s = ShardState::default();
		arm(&mut s, "willed");
		s.sessions.get_mut("willed").unwrap().pending_will =
			Some((pubm("will/topic", QoS::AtLeastOnce, b"bye", false), Instant::now()));

		arm(&mut s, "gone");
		s.subscribe("t", "gone", opts(QoS::AtLeastOnce, false, false, None, None));
		s.sessions.get_mut("gone").unwrap().expires_at = Some(Instant::now());

		let wills = s.sweep_expired();
		assert_eq!(wills.len(), 1);
		assert_eq!(wills[0].topic, "will/topic");
		assert!(!s.sessions.contains_key("gone"), "expired session reclaimed");
	}
}
