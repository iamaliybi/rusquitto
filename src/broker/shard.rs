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
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalSender;
use mqttbytes::v5::Publish;

use crate::broker::delivery::{Delivery, Mailbox, UnparkCmd};
use crate::broker::messages::{MeshMsg, MigratedSession, MigratedSub};
use crate::broker::session::{PersistedSession, SessionHandle, SessionSnapshot};
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
	/// Boxed and absent while connected (the live connection holds the real
	/// state), so a connected session costs its table slot nothing here.
	snapshot: Option<Box<SessionSnapshot>>,
	/// QoS > 0 messages that matched while the client was offline, delivered in
	/// order when it reconnects. Bounded by [`OFFLINE_QUEUE_LIMIT`].
	///
	/// [`OFFLINE_QUEUE_LIMIT`]: crate::broker::delivery::OFFLINE_QUEUE_LIMIT
	offline_queue: VecDeque<Delivery>,
	/// A Will Message armed with a non-zero Will Delay Interval: `(will, deadline)`.
	/// Published by [`sweep_expired`](ShardState::sweep_expired) once the deadline
	/// passes (or the session ends first), and cancelled if the client reconnects.
	/// Boxed: armed wills are rare, and the tuple inline (~232 B) would bloat
	/// every slot of the sessions table, which scales with connection count.
	pending_will: Option<Box<(Publish, Instant)>>,
	/// Whether this session's client is *parked*: still connected, but its
	/// connection task is torn down and only its fd (on the shard's readiness
	/// ring) plus a resume record remain. Invariant while parked:
	/// `mailbox.is_none() && snapshot.is_some() && expires_at.is_none()` — from the
	/// broker's view a parked session looks exactly like a suspended one (so
	/// persistence-skip and migration need no special cases) except deliveries
	/// queue *and wake it* instead of waiting for a reconnect.
	parked: bool,
	/// Deduplicates egress wakes: set when the first delivery lands for a parked
	/// session (one [`UnparkCmd::Wake`] is sent), cleared on reattach. Further
	/// deliveries just queue — the one wake resurrects the connection, which
	/// drains everything.
	wake_pending: bool,
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
	mesh_tx: Option<Rc<Senders<MeshMsg>>>,
	/// In-flight cross-shard session claims this shard is awaiting, keyed by client
	/// id. When a `Handoff` reply arrives on the mesh it is delivered through the
	/// matching sender, waking the CONNECT handler blocked on the claim.
	pending_claims: HashMap<String, LocalSender<Option<MigratedSession>>>,
	/// Reliable outbound queue for **control-plane** mesh messages (session
	/// `Claim`/`Handoff` and shared-subscription `Join`/`Leave`). Enqueuing is
	/// synchronous and never drops; a drain task sends each with the awaiting
	/// `send_to` (mesh backpressure), so control messages survive an overloaded
	/// link instead of being silently dropped like the best-effort data plane
	/// (`$SYS`, QoS 0 publishes). `None` on a single-shard broker (no peers).
	/// Control volume is low (membership + migration events), so the queue stays
	/// small even under sustained data-plane saturation.
	control_tx: Option<LocalSender<(usize, MeshMsg)>>,
	/// Round-robin cursor per shared-subscription group, keyed by group name,
	/// advanced each time a purely-local group load-balances a message so its
	/// deliveries rotate across the members.
	shared_cursor: HashMap<String, usize>,
	/// Replicated view of *remote* shards' connected shared-group members, keyed by
	/// group name, maintained by [`SharedEvent`](crate::broker::messages::SharedEvent)
	/// broadcasts. Sorted (`BTreeSet`) so every shard indexes an identical member
	/// order when it computes the global delivery pick. Local members are not in
	/// here — they come from the trie at match time.
	shared_remote: HashMap<String, BTreeSet<String>>,
	/// Cap on distinct retained topics stored on this shard (`0` = unlimited). Bounds
	/// the memory a flood of retained publishes to unique topics can consume.
	retained_limit: usize,
	/// Per-shard counter for server-assigned client ids (MQTT 5 allows an empty
	/// CONNECT client id). Shard-local on purpose: the shard id is baked into the
	/// generated id string, so per-shard counters stay broker-unique without any
	/// cross-core atomic on the CONNECT path.
	next_assigned_id: u64,
	/// Session mutations awaiting the next write-ahead-log flush. `None` when the
	/// WAL is disabled, so a broker without persistence tracks nothing.
	wal: Option<WalPending>,
	/// Sender to the shard's parking task ([`UnparkCmd`] wake/close commands).
	/// `None` when parking is disabled. Injected by the server layer at startup
	/// (like `control_tx`), keeping the broker layer free of server dependencies.
	unpark_tx: Option<LocalSender<UnparkCmd>>,
}

/// Pending durable-session mutations for the write-ahead log: client ids whose
/// suspended state changed (`dirty`, re-logged as Upserts) and client ids whose
/// session ended (`removed`, logged as tombstones). Drained each flush by
/// [`ShardState::take_wal_batch`]. A client id is in at most one set at a time.
#[derive(Default)]
struct WalPending {
	dirty: HashSet<String>,
	removed: HashSet<String>,
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

	/// Hands out the next per-shard counter value for a server-assigned client id
	/// (see the field docs for why this is shard-local rather than a global).
	pub fn next_assigned_id(&mut self) -> u64 {
		let n = self.next_assigned_id;
		self.next_assigned_id += 1;
		n
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
			// A parked predecessor is discarded like any other; its dormant fd must
			// be closed too (takeover semantics — no Will), so signal the parking
			// task before the session record disappears.
			self.signal_close_parked(client_id);
			if self.sessions.remove(client_id).is_some() {
				self.trie.remove_client(client_id);
				// A prior durable session is discarded: tombstone it in the WAL.
				self.wal_removed(client_id);
			}
		} else if self.sessions.contains_key(client_id) {
			// Resuming over a *parked* predecessor is a session takeover: the old
			// connection still holds a dormant fd, which must be closed (signalled
			// before the generation is overwritten, since the Close carries the old
			// generation for the parking task's race check).
			self.signal_close_parked(client_id);
			let existing = self.sessions.get_mut(client_id).expect("checked above");
			existing.mailbox = Some(mailbox);
			existing.expires_at = None;
			existing.generation = generation;
			existing.parked = false;
			existing.wake_pending = false;
			// Reconnecting cancels any armed (delayed) Will Message.
			existing.pending_will = None;
			// Hand the durable state back to the resuming connection.
			let handle = SessionHandle {
				resumed: true,
				generation,
				snapshot: existing.snapshot.take().map(|b| *b).unwrap_or_default(),
				offline_queue: std::mem::take(&mut existing.offline_queue),
			};
			// The resumed client's shared subscriptions (kept armed in the trie
			// across the suspension) are connected group members again.
			for group in self.shared_groups_of(client_id) {
				self.broadcast_shared(&group, client_id, true);
			}
			// The session is online again; its durable copy is now held by the
			// connection, so tombstone the suspended record (a fresh one is written
			// when it next disconnects).
			self.wal_removed(client_id);
			return handle;
		}

		self.sessions.insert(
			client_id.to_string(),
			Session {
				mailbox: Some(mailbox),
				expires_at: None,
				generation,
				snapshot: None,
				offline_queue: VecDeque::new(),
				pending_will: None,
				parked: false,
				wake_pending: false,
			},
		);
		SessionHandle {
			resumed: false,
			generation,
			snapshot: SessionSnapshot::default(),
			offline_queue: VecDeque::new(),
		}
	}

	// --- parking --------------------------------------------------------------

	/// Installs the sender half of the parking task's command channel (see
	/// [`UnparkCmd`]). Called at startup when `[parking]` is enabled, mirroring
	/// [`set_control_tx`](Self::set_control_tx).
	pub fn set_unpark_tx(&mut self, tx: LocalSender<UnparkCmd>) {
		self.unpark_tx = Some(tx);
	}

	/// Flips a connected session to *parked* — the synchronous half of the park
	/// transition, called by the connection's own shard (no `.await` between the
	/// caller's idle check and this flip, so no delivery can slip past unseen).
	///
	/// The mailbox is dropped (deliveries now take the parked arm of `deliver_to`:
	/// queue + wake) and the connection's durable QoS state is stored exactly as a
	/// suspension would, so migration and takeover paths need no parked special
	/// cases. The generation is *not* bumped: the same logical connection resumes.
	///
	/// Returns `false` — the caller must not park, and should close instead — if
	/// the session is missing, was taken over (generation mismatch), or holds no
	/// live mailbox.
	pub fn park_session(&mut self, client_id: &str, generation: u64, snapshot: SessionSnapshot) -> bool {
		let Some(session) = self.sessions.get_mut(client_id) else {
			return false;
		};
		if session.generation != generation || session.mailbox.is_none() {
			return false;
		}
		session.mailbox = None;
		session.parked = true;
		session.wake_pending = false;
		session.snapshot = Some(Box::new(snapshot));
		// A parked client is connected: it has no expiry deadline (keep-alive is
		// enforced by the parking task's deadline sweep instead).
		session.expires_at = None;
		true
	}

	/// Re-attaches a resurrected connection to its parked session: installs the
	/// new mailbox and returns the stored QoS state plus everything queued while
	/// parked, in order. The unpark counterpart of [`park_session`](Self::park_session).
	///
	/// Returns `None` if the session is gone, was taken over (generation
	/// mismatch), or is no longer parked — the resumed task must then close its
	/// socket quietly (displaced-connection semantics: no Will).
	pub fn reattach_parked(
		&mut self,
		client_id: &str,
		generation: u64,
		mailbox: Mailbox,
	) -> Option<(SessionSnapshot, VecDeque<Delivery>)> {
		let session = self.sessions.get_mut(client_id)?;
		if session.generation != generation || !session.parked {
			return None;
		}
		session.mailbox = Some(mailbox);
		session.parked = false;
		session.wake_pending = false;
		let snapshot = session.snapshot.take().map(|b| *b).unwrap_or_default();
		let queued = std::mem::take(&mut session.offline_queue);
		Some((snapshot, queued))
	}

	/// Converts a parked session into a properly *suspended* (or destroyed) one —
	/// the parked analogue of [`close_session`](Self::close_session), used when the
	/// dormant fd is closed without resuming: parked keep-alive expiry, or graceful
	/// shutdown. The stored snapshot is already in place from the park, so only the
	/// expiry deadline, WAL record, and shared-group Leave announcements are new.
	///
	/// Returns `true` if the session was parked under this generation and is now
	/// suspended/destroyed (the caller then owns Will publication, exactly like a
	/// disconnect); `false` on any mismatch (taken over meanwhile — publish no Will).
	pub fn suspend_parked(&mut self, client_id: &str, generation: u64, expiry_secs: u32) -> bool {
		let shared_groups = self.shared_groups_of(client_id);

		let Some(session) = self.sessions.get_mut(client_id) else {
			return false;
		};
		if session.generation != generation || !session.parked {
			return false;
		}
		session.parked = false;
		session.wake_pending = false;

		if expiry_secs == 0 {
			self.sessions.remove(client_id);
			self.trie.remove_client(client_id);
			self.wal_removed(client_id);
		} else {
			session.expires_at = (expiry_secs != SESSION_NEVER_EXPIRES)
				.then(|| Instant::now() + Duration::from_secs(u64::from(expiry_secs)));
			// Now genuinely suspended: its durable record (snapshot + queue stored at
			// park time) enters the WAL.
			self.wal_dirty(client_id);
		}
		// Parked counted as a connected shared-group member; suspended does not.
		for group in &shared_groups {
			self.broadcast_shared(group, client_id, false);
		}
		true
	}

	/// Tells the parking task to close a parked session's dormant fd without
	/// resuming it (session takeover, Clean Start discard, or a mesh claim). A
	/// no-op unless the session is currently parked. The command carries the
	/// parked generation so a racing unpark makes it harmless.
	pub(super) fn signal_close_parked(&mut self, client_id: &str) {
		let Some(session) = self.sessions.get_mut(client_id) else {
			return;
		};
		if !session.parked {
			return;
		}
		session.parked = false;
		session.wake_pending = false;
		let generation = session.generation;
		if let Some(tx) = &self.unpark_tx {
			// Unbounded local channel: only errors if the parking task is gone
			// (shard teardown), where the fd is reclaimed by the drain instead.
			let _ = tx.try_send(UnparkCmd::Close { client_id: client_id.to_string(), generation });
		}
	}

	/// Subscribes a client to a topic filter with a granted QoS and its options.
	/// The filter may contain the `+` and `#` wildcards. Re-subscribing replaces
	/// the prior entry. Returns `true` if the subscription is new (for Retain
	/// Handling). Callers are live connections, so a new shared subscription is a
	/// membership Join announced to every peer shard.
	pub fn subscribe(&mut self, filter: &str, client_id: &str, opts: SubOptions) -> bool {
		let group = opts.share_group.map(str::to_string);
		let is_new = self.trie.insert(filter, client_id, opts);
		if is_new && let Some(group) = group {
			self.broadcast_shared(&group, client_id, true);
		}
		is_new
	}

	/// Periodic garbage collection of shard-local index memory that the hot paths
	/// deliberately don't reclaim inline: interned topic segments no longer used by
	/// any trie node, and round-robin cursors for shared groups whose last local
	/// member has unsubscribed. Both grow only under topic/group *churn* and are
	/// bounded to the live set by this sweep. Off the message path — called from the
	/// per-shard maintenance timer.
	pub fn gc_indexes(&mut self) {
		self.trie.gc_interner();
		if !self.shared_cursor.is_empty() {
			let mut live = std::collections::HashSet::new();
			self.trie.collect_shared_groups(&mut live);
			self.shared_cursor.retain(|group, _| live.contains(group));
		}
	}

	/// Removes a single subscription (used by UNSUBSCRIBE). `share_group` selects
	/// the ordinary (`None`) or shared entry. Returns whether one was removed.
	pub fn unsubscribe(&mut self, filter: &str, client_id: &str, share_group: Option<&str>) -> bool {
		let removed = self.trie.remove(filter, client_id, share_group);
		if removed && let Some(group) = share_group {
			// The client may still hold the same group via another filter; only
			// announce a Leave once its last subscription in the group is gone.
			if !self.holds_shared_group(client_id, group) {
				self.broadcast_shared(group, client_id, false);
			}
		}
		removed
	}

	/// Whether `client_id` still holds any subscription in shared group `group`.
	fn holds_shared_group(&self, client_id: &str, group: &str) -> bool {
		self.trie
			.client_subscriptions(client_id)
			.iter()
			.any(|s| s.share_group.as_deref() == Some(group))
	}

	/// The distinct shared groups `client_id` currently subscribes to.
	fn shared_groups_of(&self, client_id: &str) -> Vec<String> {
		let mut groups: Vec<String> = self
			.trie
			.client_subscriptions(client_id)
			.into_iter()
			.filter_map(|s| s.share_group)
			.collect();
		groups.sort_unstable();
		groups.dedup();
		groups
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
		// Gathered before the session borrow (and before a destroy clears the
		// trie); only announced below once we know this connection owned it.
		let shared_groups = self.shared_groups_of(client_id);

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
			session.snapshot = Some(Box::new(snapshot));
			// Messages held back by the outbound window were already dequeued, so
			// they precede anything that arrives while suspended.
			pending.append(&mut session.offline_queue);
			session.offline_queue = pending;
			session.expires_at = (expiry_secs != SESSION_NEVER_EXPIRES)
				.then(|| Instant::now() + Duration::from_secs(u64::from(expiry_secs)));
		}
		// Record the durable outcome in the WAL: a tombstone when destroyed, or an
		// Upsert of the freshly-suspended state (its offline queue included).
		if expiry_secs == 0 {
			self.wal_removed(client_id);
		} else {
			self.wal_dirty(client_id);
		}
		// Destroyed or suspended, either way the client is no longer a *connected*
		// member of its shared groups — tell the peers.
		for group in &shared_groups {
			self.broadcast_shared(group, client_id, false);
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

	/// Snapshots the durable *suspended* (offline) sessions on this shard for
	/// persistence, non-destructively. Connected sessions are skipped: their live
	/// QoS state lives in the connection, not here, and their expiry interval isn't
	/// known to the shard. A session already at its expiry deadline is skipped too.
	pub fn persist_sessions(&self, now: Instant) -> Vec<PersistedSession> {
		self.sessions
			.keys()
			.filter_map(|id| self.persist_one(id, now))
			.collect()
	}

	/// Builds the durable snapshot of a single *suspended* session, or `None` if it
	/// is connected, missing, or already at its expiry deadline. Backs both
	/// [`persist_sessions`](Self::persist_sessions) and the WAL batch.
	fn persist_one(&self, client_id: &str, now: Instant) -> Option<PersistedSession> {
		let session = self.sessions.get(client_id)?;
		if session.mailbox.is_some() || session.parked {
			// Connected (or parked, which *is* connected — just task-less): not
			// durable here. Persisting a parked session would write it with "never
			// expires", resurrecting every parked client as an immortal suspended
			// session after a crash. Graceful shutdown converts parked → suspended
			// before the final snapshot, so nothing is lost on a clean stop.
			return None;
		}
		let expiry_secs = match session.expires_at {
			None => u32::MAX, // never expires
			Some(deadline) => {
				let remaining = deadline.saturating_duration_since(now).as_secs();
				if remaining == 0 {
					return None; // about to expire; not worth persisting
				}
				u32::try_from(remaining).unwrap_or(u32::MAX)
			}
		};
		let subscriptions = self
			.trie
			.client_subscriptions(client_id)
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
		let snapshot = session.snapshot.as_deref().cloned().unwrap_or_default();
		let offline = session
			.offline_queue
			.iter()
			.map(|d| ((*d.publish).clone(), d.qos, d.retain, d.sub_ids.clone()))
			.collect();
		Some(PersistedSession {
			client_id: client_id.to_string(),
			expiry_secs,
			session: MigratedSession {
				subscriptions,
				inflight: snapshot.inflight,
				incoming_qos2: snapshot.incoming_qos2,
				next_pkid: snapshot.next_pkid,
				offline,
			},
		})
	}

	// --- write-ahead log ----------------------------------------------------

	/// Enables WAL tracking on this shard (called at startup when persistence and a
	/// non-zero flush interval are configured). Idempotent.
	pub fn enable_wal(&mut self) {
		self.wal.get_or_insert_with(WalPending::default);
	}

	/// Marks a suspended session's durable state as changed, to be re-logged as an
	/// Upsert at the next WAL flush. No-op when the WAL is disabled.
	fn wal_dirty(&mut self, client_id: &str) {
		if let Some(w) = self.wal.as_mut() {
			w.removed.remove(client_id);
			if !w.dirty.contains(client_id) {
				w.dirty.insert(client_id.to_string());
			}
		}
	}

	/// Marks a session as ended, to be logged as a tombstone at the next WAL flush
	/// (supersedes any pending dirty mark). No-op when the WAL is disabled.
	fn wal_removed(&mut self, client_id: &str) {
		if let Some(w) = self.wal.as_mut() {
			w.dirty.remove(client_id);
			w.removed.insert(client_id.to_string());
		}
	}

	/// Drains the pending WAL mutations into one group-committed batch: tombstones
	/// for removed sessions, then Upserts for the current state of dirty (still
	/// suspended) ones. `None` when the WAL is disabled or nothing changed.
	pub fn take_wal_batch(&mut self, now: Instant) -> Option<Vec<u8>> {
		let (dirty, removed) = {
			let w = self.wal.as_mut()?;
			if w.dirty.is_empty() && w.removed.is_empty() {
				return None;
			}
			(
				w.dirty.drain().collect::<Vec<_>>(),
				w.removed.drain().collect::<Vec<_>>(),
			)
		};
		let mut buf = Vec::new();
		for id in &removed {
			crate::persistence::wal::encode_remove(&mut buf, id);
		}
		for id in &dirty {
			// A dirty session that is no longer suspended (reconnected, expired) has a
			// matching tombstone queued elsewhere or already flushed; just skip it.
			if let Some(ps) = self.persist_one(id, now) {
				let _ = crate::persistence::wal::encode_upsert(&mut buf, &ps);
			}
		}
		(!buf.is_empty()).then_some(buf)
	}

	/// Installs persisted sessions as *suspended* sessions at startup: re-arms their
	/// subscriptions in the trie, restores durable QoS state and the offline queue,
	/// and recomputes each expiry deadline from `now`. A reconnecting client then
	/// resumes it locally, or the cross-shard `Claim`/`Handoff` migrates it here.
	pub fn load_sessions(&mut self, sessions: Vec<PersistedSession>, now: Instant) {
		for ps in sessions {
			for sub in &ps.session.subscriptions {
				self.trie.insert(
					&sub.filter,
					&ps.client_id,
					SubOptions {
						qos: sub.qos,
						// No Local on a shared subscription is a protocol error
						// (rejected at SUBSCRIBE); strip it from snapshots that
						// predate the rule so the global delivery pick stays
						// consistent across shards.
						nolocal: sub.nolocal && sub.share_group.is_none(),
						retain_as_published: sub.retain_as_published,
						share_group: sub.share_group.as_deref(),
						sub_id: sub.sub_id,
					},
				);
			}
			let offline_queue = ps
				.session
				.offline
				.into_iter()
				.map(|(publish, qos, retain, sub_ids)| Delivery { publish: Rc::new(publish), qos, retain, sub_ids })
				.collect();
			let snapshot = SessionSnapshot {
				inflight: ps.session.inflight,
				incoming_qos2: ps.session.incoming_qos2,
				next_pkid: ps.session.next_pkid,
			};
			let expires_at = (ps.expiry_secs != u32::MAX).then(|| now + Duration::from_secs(u64::from(ps.expiry_secs)));
			self.next_generation += 1;
			let generation = self.next_generation;
			self.sessions.insert(
				ps.client_id,
				Session {
					mailbox: None,
					expires_at,
					generation,
					snapshot: Some(Box::new(snapshot)),
					offline_queue,
					pending_will: None,
					parked: false,
					wake_pending: false,
				},
			);
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
	///
	/// *Parked* sessions are deliberately skipped too (they also hold no mailbox):
	/// shedding relieves reactor saturation — a CPU signal — and a parked
	/// connection contributes ~zero CPU, so closing one relieves nothing while
	/// disconnecting the best-behaved clients (whose reconnects would then *add*
	/// CONNECT load).
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
			session.pending_will = Some(Box::new((will, deadline)));
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
				.is_some_and(|armed| armed.1 <= now)
			{
				// Box permits moving the will out through its deref.
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
				if let Some(armed) = session.pending_will {
					let (will, _) = *armed;
					wills.push(will);
				}
				self.trie.remove_client(&id);
				// Its durable record is gone: tombstone it in the WAL.
				self.wal_removed(&id);
			}
		}
		wills
	}
}
