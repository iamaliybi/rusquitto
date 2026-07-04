//! Per-shard broker state.
//!
//! [`ShardState`] is single-threaded and shared between every connection on the
//! shard via `Rc<RefCell<>>`. No locks are needed: in the thread-per-core model
//! no other core ever touches this memory. Its behaviour is split by concern:
//!
//! - this module — session lifecycle (open / close / suspend / expire) and the
//!   shard's data (sessions, retained table, subscription trie).
//! - [`routing`] — turning one publish into per-subscriber deliveries.
//! - [`mesh`] — cross-shard forwarding and session migration.

mod mesh;
mod routing;

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalSender;
use mqttbytes::v5::Publish;

use crate::broker::mesh::{MeshMsg, MigratedSession};
use crate::broker::session::{Delivery, Mailbox, SessionHandle, SessionSnapshot};
use crate::broker::topics::{SubOptions, TopicTrie};

/// MQTT 5 Session Expiry Interval sentinel meaning "the session never expires".
const SESSION_NEVER_EXPIRES: u32 = u32::MAX;

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
	///
	/// [`OFFLINE_QUEUE_LIMIT`]: crate::broker::session::OFFLINE_QUEUE_LIMIT
	offline_queue: VecDeque<Delivery>,
	/// A Will Message armed with a non-zero Will Delay Interval: `(will, deadline)`.
	/// Published by [`sweep_expired`](ShardState::sweep_expired) once the deadline
	/// passes (or the session ends first), and cancelled if the client reconnects.
	pending_will: Option<(Publish, Instant)>,
}

/// Per-shard broker state. See the [module docs](self) for the split of concerns.
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
	/// Cap on distinct retained topics stored on this shard (`0` = unlimited). Bounds
	/// the memory a flood of retained publishes to unique topics can consume.
	retained_limit: usize,
}

impl ShardState {
	/// Creates a fresh, shareable handle to this shard's state.
	pub fn new() -> Rc<RefCell<Self>> {
		Rc::new(RefCell::new(Self::default()))
	}

	/// Sets the cap on distinct retained topics (`0` = unlimited).
	pub fn set_retained_limit(&mut self, limit: usize) {
		self.retained_limit = limit;
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
	pub fn open_session(&mut self, client_id: &str, mailbox: Mailbox, clean_start: bool) -> SessionHandle {
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
	/// the ordinary (`None`) or shared entry. Returns whether one was removed.
	pub fn unsubscribe(&mut self, filter: &str, client_id: &str, share_group: Option<&str>) -> bool {
		self.trie.remove(filter, client_id, share_group)
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
	///
	/// [`open_session`]: Self::open_session
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

	/// Sheds up to `max` currently-connected sessions to relieve an overloaded
	/// shard, returning how many were shed.
	///
	/// Dropping a session's live mailbox closes the sender, so the connection's
	/// event loop wakes with a closed receiver and ends (its normal cleanup then
	/// suspends or discards the session per its expiry, exactly as any disconnect).
	/// The client reconnects from a *new* source port, so `SO_REUSEPORT` rehashes
	/// it — usually onto a less-loaded shard. Already-suspended sessions (no live
	/// mailbox) are skipped. This is how the thread-per-core model rebalances: it
	/// moves the *connection*, since the compute can't move between cores.
	pub fn shed_connections(&mut self, max: usize) -> usize {
		if max == 0 {
			return 0;
		}
		let mut shed = 0;
		for session in self.sessions.values_mut() {
			if shed >= max {
				break;
			}
			if session.mailbox.is_some() {
				session.mailbox = None;
				shed += 1;
			}
		}
		shed
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
