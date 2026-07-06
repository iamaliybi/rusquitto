//! Per-client MQTT connection: the protocol state machine.
//!
//! [`Connection`] owns one client socket (any [`ByteStream`], so the same logic
//! serves plain TCP and WebSocket) and drives it from CONNECT to close. The
//! implementation is split by responsibility across sibling modules:
//!
//! - [`connect`] — the CONNECT handshake, authentication, and session resume.
//! - [`publish`] — inbound PUBLISH handling and the receiver-side QoS flows.
//! - [`subscribe`] — SUBSCRIBE / UNSUBSCRIBE and retained replay.
//! - [`control`] — PING, DISCONNECT, and the sender-side QoS acknowledgements.
//! - [`delivery`] — the outbound path: window control, fan-out, retransmit.

mod connect;
mod control;
mod delivery;
mod publish;
mod ratelimit;
mod subscribe;

#[cfg(test)]
mod tests;

use ratelimit::TokenBucket;

use bytes::BytesMut;
use futures_lite::FutureExt;
use glommio::channels::local_channel::{self, LocalReceiver};
use mqttbytes::{
	Error as MqttError,
	v5::{self as mqtt_v5, Packet},
};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::{Error, ErrorKind, Result};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::auth::Authenticator;
use crate::broker::delivery::{Delivery, Mailbox};
use crate::broker::session::InflightMessage;
use crate::broker::shard::ShardState;
use crate::config::LimitsConfig;
use crate::telemetry::metrics::Metrics;
use crate::transport::ByteStream;
use crate::transport::tls::TlsIdentity;

/// How long a CONNECT handler waits for peers to answer a cross-shard session
/// [`Claim`](crate::broker::messages::SessionControl::Claim) before giving up and
/// treating the session as fresh. Mesh replies normally arrive in microseconds;
/// this only bounds the wait if a reply is dropped (drop-on-full mesh) or a peer
/// is wedged, so it can be generous without slowing the common case (which
/// resolves as soon as every peer has answered).
const SESSION_CLAIM_TIMEOUT: Duration = Duration::from_millis(250);

/// Smallest per-read reservation in the assembly buffer. Each socket read lands
/// directly in the buffer's tail (no intermediate copy); the reservation adapts
/// between these bounds — an idle connection reserves only [`READ_CHUNK_MIN`],
/// a connection streaming large packets grows toward [`READ_CHUNK_MAX`] so bulk
/// transfers need fewer reads.
const READ_CHUNK_MIN: usize = 512;

/// Largest per-read reservation in the assembly buffer.
const READ_CHUNK_MAX: usize = 8192;

/// Flush the coalesced output buffer once it grows past this many bytes, even
/// mid-drain. This is also the elastic-memory ceiling for a consumer whose
/// socket has stalled (its task parks on the blocked write with the buffer
/// full), so keep it modest: 16 KiB is still far past the point of diminishing
/// batching returns, while a thousand stuck consumers pin ≤ 16 MiB.
const FLUSH_THRESHOLD: usize = 16 * 1024;

/// Read/output buffers whose capacity exceeds this are released once empty, so
/// a burst (one large packet, one deep fan-out) doesn't pin its high-water
/// allocation on an idle connection forever.
const BUFFER_RETAIN_MAX: usize = 16 * 1024;

/// Longest client identifier the broker accepts (the spec only mandates support
/// for 23; we allow generously more but bound it to reject abuse).
const MAX_CLIENT_ID_LEN: usize = 256;

/// Upper bound on QoS > 0 messages held for a connected client whose in-flight
/// window is full. Beyond this the oldest held message is dropped, so a client
/// that stops acknowledging can't force unbounded broker memory growth.
const PENDING_OUTBOUND_LIMIT: usize = 4096;

// NOTE: the outbound mailbox is deliberately an *unbounded* channel: glommio's
// bounded variant pre-allocates its whole ring per connection (`VecDeque::
// with_capacity`), while the unbounded one allocates nothing until a delivery
// is actually queued — the right trade for tens of thousands of mostly-idle
// connections. The drop-on-full DoS bound a bounded channel would provide is
// enforced instead at the routing site via [`MAILBOX_LIMIT`]
// (crate::broker::session::MAILBOX_LIMIT).

/// How a connection's event loop ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Flow {
	/// The connection is over: EOF, protocol error, timeout, takeover, or a clean
	/// DISCONNECT. `run()` has already torn the session down.
	Closed,
	/// The connection went fully idle past the parking grace and should be
	/// *parked*: the caller must destructure it ([`Connection::into_parts`]),
	/// flip the session ([`ShardState::park_session`]) and hand the fd + resume
	/// state to the shard's parking registry — synchronously, with no `.await`
	/// in between (the single-threaded shard then guarantees no delivery can
	/// slip past the transition). `run()` performed **no** cleanup: the session,
	/// the connected-clients gauge, and the Will all stay live.
	Park,
}

/// One blocking turn of the connection event loop resolves to exactly one of
/// these: bytes from the client socket, a routed delivery, or the idle deadline.
/// Packets are parsed *outside* the race (synchronously, from the assembly
/// buffer), so this enum stays small.
enum Event {
	/// The socket read appended this many bytes to the assembly buffer (0 = EOF).
	Bytes(usize),
	/// The socket read failed.
	ReadErr(Error),
	/// A message was routed into this connection's mailbox for delivery.
	/// `None` means the channel closed (all senders dropped).
	Outgoing(Option<Delivery>),
	/// The idle deadline (handshake or keep-alive) lapsed.
	Timeout,
}

/// Lazily-allocated inbound and outbound topic-alias tables (MQTT 5). Kept behind
/// an `Option<Box<…>>` on the connection so the idle/non-aliasing common case
/// holds no `HashMap`s — see the `aliases` field.
#[derive(Default)]
struct AliasTables {
	/// Alias → topic, for aliases the client registered on its inbound PUBLISHes.
	inbound: HashMap<u16, String>,
	/// Topic → alias, for aliases we assigned on the PUBLISHes we send this client.
	outbound: HashMap<String, u16>,
}

pub struct Connection<S: ByteStream> {
	stream: S,
	inbound: BytesMut,
	/// Coalesced output: every outbound packet is encoded here and the whole
	/// batch is written with one `write_all` per event-loop wakeup (one io_uring
	/// op — and one TLS record / WebSocket frame — instead of one per packet).
	outbound: BytesMut,
	/// Adaptive per-read reservation in `buffer`, between [`READ_CHUNK_MIN`] and
	/// [`READ_CHUNK_MAX`]: doubles when reads come back full, halves when they
	/// come back nearly empty, so idle connections hold small buffers.
	read_chunk: usize,
	shard_id: usize,
	client_id: String,
	/// Shard-local broker state, shared with every other connection on this core.
	shard: Rc<RefCell<ShardState>>,
	/// Sender half of this connection's mailbox, held until CONNECT hands it to
	/// the registry. `None` thereafter — the registry owns it (it is not `Clone`).
	mailbox_tx: Option<Mailbox>,
	/// Receiver half, drained by the event loop and written to the socket.
	mailbox_rx: LocalReceiver<Delivery>,
	/// Inbound QoS 2 messages received (PUBLISH) but not yet committed (PUBREL),
	/// keyed by the publisher's packet id. Delivered exactly once on PUBREL.
	incoming_qos2: HashMap<u16, mqtt_v5::Publish>,
	/// Outbound QoS 1/2 messages we sent to this client, keyed by the packet id
	/// we assigned, awaiting their acknowledgement. Retained so they can be
	/// retransmitted (with DUP) if the session is resumed.
	inflight: HashMap<u16, InflightMessage>,
	/// Rolling packet-id allocator for outbound QoS 1/2 messages.
	next_pkid: u16,
	/// Broker resource limits (max payload, granted QoS, keep-alive, …).
	limits: LimitsConfig,
	/// Session Expiry Interval (seconds) negotiated at CONNECT: `0` discards the
	/// session on disconnect, `0xFFFFFFFF` keeps it forever, anything between
	/// suspends it for that many seconds. See [`ShardState::close_session`].
	session_expiry: u32,
	/// Generation token for this connection's session, returned by
	/// `open_session` and handed back to `close_session` so a takeover by a
	/// newer connection isn't torn down by this one's cleanup.
	session_generation: u64,
	/// The client's Will Message (from CONNECT), pre-built as a `Publish` ready
	/// to fan out. Published when the connection ends abnormally; cleared by a
	/// normal DISCONNECT so it is suppressed. `None` when no will was set.
	/// Boxed: a will is rare, and inline it would put a `Publish`-sized
	/// (~208 B) field in every connection.
	will: Option<Box<mqtt_v5::Publish>>,
	/// The client's Receive Maximum (CONNECT): the most unacknowledged QoS 1/2
	/// PUBLISHes we may have in flight to it at once. Defaults to 65535.
	peer_receive_max: u16,
	/// The client's Maximum Packet Size (CONNECT), if any: we must not send it a
	/// packet larger than this. `None` means no limit.
	peer_max_packet_size: Option<u32>,
	/// Outbound QoS 1/2 messages held back because the in-flight window (bounded
	/// by [`peer_receive_max`](Self::peer_receive_max)) is full; drained as
	/// acknowledgements free slots.
	pending_outbound: VecDeque<Delivery>,
	/// Shard-local credential store, shared by every connection on this shard.
	auth: Rc<Authenticator>,
	/// Authenticated username, used for per-topic ACL checks. `None` for an
	/// anonymous client (which is unrestricted).
	username: Option<String>,
	/// Cross-shard broker metrics (published to `$SYS`).
	metrics: Arc<Metrics>,
	/// Whether this connection has been counted as connected, so the matching
	/// decrement happens exactly once (only after a successful CONNECT).
	counted: bool,
	/// Broker-wide shutdown flag; when set, a closed mailbox means the server is
	/// stopping (rather than a session takeover) so we send DISCONNECT first.
	shutdown: Arc<AtomicBool>,
	/// Will Delay Interval (seconds) from CONNECT: the will is published this many
	/// seconds after an abnormal disconnect (capped by the session expiry), or
	/// immediately when 0. See [`ShardState::arm_will`].
	will_delay: u32,
	/// The client's Topic Alias Maximum (CONNECT): how many aliases we may assign
	/// on the publishes we send it. `0` (the default when absent) disables
	/// outbound aliasing entirely.
	peer_topic_alias_max: u16,
	/// Inbound + outbound topic-alias tables (MQTT 5), boxed and **absent until the
	/// connection actually uses aliasing**: a client that never registers an inbound
	/// alias and is never assigned an outbound one pays 8 bytes here, not two
	/// `HashMap`s. Topic aliasing is a wire-size optimization, so this lazy path
	/// costs the idle/non-aliasing common case nothing. See [`AliasTables`].
	aliases: Option<Box<AliasTables>>,
	/// Set once a valid CONNECT has been accepted. Every other packet type is a
	/// protocol violation before this, and a second CONNECT is a violation after.
	connected: bool,
	/// The mutual-TLS authentication outcome for this connection, consulted by the
	/// CONNECT handler to decide the client's MQTT identity: a verified certificate
	/// authenticates a client that supplies no username, and its CN can stand in as
	/// that username (see [`handle_connect`](Self::handle_connect)).
	tls_identity: TlsIdentity,
	/// Idle deadline: the CONNECT handshake deadline before connecting, then the
	/// keep-alive deadline (1.5× the negotiated keep-alive) afterwards. `None`
	/// disables the check. Reset on every inbound packet.
	deadline: Option<Instant>,
	/// The keep-alive window (1.5× the negotiated interval), used to refresh
	/// `deadline` after each inbound packet. `None` when keep-alive is disabled.
	keepalive: Option<Duration>,
	/// When the bytes of the current *incomplete* frame first appeared. A frame
	/// that then stalls is a slow-loris (the truncated-header adversarial case);
	/// [`framing_deadline`](Self::framing_deadline) reaps it within the handshake
	/// window even when keep-alive is disabled (`deadline` is then `None`). Reset
	/// to `None` whenever the assembly buffer holds no partial frame.
	partial_since: Option<Instant>,
	/// Count of active subscriptions, enforced against `limits.max_subscriptions_per_client`.
	subscription_count: usize,
	/// Per-connection inbound PUBLISH throttle. `Some` when `limits.max_message_rate`
	/// is set: bounds how much CPU one noisy publisher can draw on its pinned core.
	rate_limiter: Option<TokenBucket>,
	/// How long the connection must stay fully idle before the event loop offers
	/// it for parking (`[parking] idle_grace_secs`). `None` disables parking for
	/// this connection — the default; only the plain-TCP path opts in (TLS and
	/// WebSocket streams carry mid-stream codec state that can't be parked).
	park_grace: Option<Duration>,
	/// Last moment this connection did real work (an inbound packet processed or
	/// an outbound delivery queued). The parking deadline is `last_activity +
	/// park_grace`, evaluated only while [`park_ready`](Self::park_ready) holds.
	last_activity: Instant,
	/// Set by [`resume`](Self::resume): the next `run()` first re-attaches the
	/// parked session and replays what queued while parked, before entering the
	/// normal event loop.
	resume_pending: bool,
}

/// Everything a parked connection needs to be rebuilt, beyond its fd: the
/// negotiated MQTT session parameters that live in the connection while online.
/// Produced by [`Connection::into_parts`] on park and consumed by
/// [`Connection::resume`] on wake; boxed in the parking registry (~200 B — the
/// entire per-connection cost of a parked client besides the fd itself).
///
/// Durable QoS state (`inflight`/`incoming_qos2`/`next_pkid`) is deliberately
/// *not* here: the park predicate guarantees the maps are empty, and `next_pkid`
/// travels through the session snapshot (`park_session` → `reattach_parked`) so
/// cross-shard migration of a parked session carries it automatically. The
/// mutual-TLS identity isn't here either: only plain TCP parks, which is always
/// [`TlsIdentity::None`].
///
/// Fields are private; the parking registry reads what its sweep needs through
/// the accessors below and treats the rest as an opaque payload.
pub(crate) struct ResumeState {
	client_id: String,
	session_generation: u64,
	next_pkid: u16,
	session_expiry: u32,
	keepalive: Option<Duration>,
	deadline: Option<Instant>,
	will: Option<Box<mqtt_v5::Publish>>,
	will_delay: u32,
	username: Option<String>,
	peer_receive_max: u16,
	peer_max_packet_size: Option<u32>,
	peer_topic_alias_max: u16,
	aliases: Option<Box<AliasTables>>,
	subscription_count: usize,
	rate_limiter: Option<TokenBucket>,
}

impl ResumeState {
	/// The parked client's id (registry key).
	pub(crate) fn client_id(&self) -> &str {
		&self.client_id
	}

	/// The session generation the connection parked under, for the takeover /
	/// stale-wake race checks.
	pub(crate) fn generation(&self) -> u64 {
		self.session_generation
	}

	/// The keep-alive deadline frozen at park time (`None` = keep-alive
	/// disabled). The parking task's sweep reaps the fd past this — the client's
	/// transmission clock keeps running while it is parked.
	pub(crate) fn deadline(&self) -> Option<Instant> {
		self.deadline
	}

	/// The negotiated Session Expiry Interval, for suspending the session when
	/// the parked fd is reaped.
	pub(crate) fn session_expiry(&self) -> u32 {
		self.session_expiry
	}

	/// Takes the Will Message and its delay, if any — published when the parked
	/// connection ends abnormally (keep-alive expiry), exactly as a live one's
	/// `run()` cleanup would.
	pub(crate) fn take_will(&mut self) -> Option<(mqtt_v5::Publish, u32)> {
		self.will.take().map(|w| (*w, self.will_delay))
	}

	/// The durable snapshot to store on the session at park time. The QoS maps
	/// are empty by the park predicate; only the packet-id allocator carries
	/// over (and thus migrates with the session if another shard claims it).
	pub(crate) fn session_snapshot(&self) -> crate::broker::session::SessionSnapshot {
		crate::broker::session::SessionSnapshot { next_pkid: self.next_pkid, ..Default::default() }
	}
}

/// The earlier of two optional deadlines; `None` means "no bound", so a present
/// deadline always wins over an absent one.
fn earlier(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
	match (a, b) {
		(Some(x), Some(y)) => Some(x.min(y)),
		(x, None) => x,
		(None, y) => y,
	}
}

impl<S: ByteStream> Connection<S> {
	/// Largest inbound topic alias the broker accepts, advertised to clients as the
	/// CONNACK Topic Alias Maximum.
	const INBOUND_TOPIC_ALIAS_MAX: u16 = 16;

	/// Ceiling on the outbound aliases we assign per connection, regardless of how
	/// large a Topic Alias Maximum the client advertises — each assigned alias
	/// stores its topic string, so this bounds that per-connection memory.
	const OUTBOUND_TOPIC_ALIAS_MAX: u16 = 32;

	// A constructor wiring the per-connection handles; each argument is a distinct
	// required dependency, so bundling them would only add an indirection.
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		stream: S,
		shard_id: usize,
		shard: Rc<RefCell<ShardState>>,
		limits: LimitsConfig,
		auth: Rc<Authenticator>,
		metrics: Arc<Metrics>,
		shutdown: Arc<AtomicBool>,
		tls_identity: TlsIdentity,
	) -> Self {
		let (mailbox_tx, mailbox_rx) = local_channel::new_unbounded();
		Self {
			stream,
			// `with_capacity(0)` (the default) allocates nothing; the buffer grows
			// on demand from the first read and is trimmed when it empties.
			inbound: BytesMut::with_capacity(limits.initial_read_buffer),
			outbound: BytesMut::new(),
			read_chunk: READ_CHUNK_MIN,
			shard_id,
			client_id: String::new(),
			shard,
			mailbox_tx: Some(mailbox_tx),
			mailbox_rx,
			incoming_qos2: HashMap::new(),
			inflight: HashMap::new(),
			next_pkid: 0,
			limits,
			session_expiry: 0,
			session_generation: 0,
			will: None,
			peer_receive_max: u16::MAX,
			peer_max_packet_size: None,
			pending_outbound: VecDeque::new(),
			auth,
			username: None,
			metrics,
			counted: false,
			shutdown,
			will_delay: 0,
			peer_topic_alias_max: 0,
			aliases: None,
			connected: false,
			tls_identity,
			// Bound the pre-CONNECT handshake so an idle socket can't hold a slot.
			deadline: (limits.connect_timeout > 0)
				.then(|| Instant::now() + Duration::from_secs(u64::from(limits.connect_timeout))),
			keepalive: None,
			partial_since: None,
			subscription_count: 0,
			rate_limiter: (limits.max_message_rate > 0)
				.then(|| TokenBucket::per_second(limits.max_message_rate, Instant::now())),
			park_grace: None,
			last_activity: Instant::now(),
			resume_pending: false,
		}
	}

	/// Opts this connection into parking: once fully idle for `grace`, its event
	/// loop returns [`Flow::Park`] instead of blocking. Only the plain-TCP serve
	/// path calls this — the fd of a TLS/WebSocket stream can't be parked.
	pub(crate) fn set_parkable(&mut self, grace: Duration) {
		self.park_grace = Some(grace);
	}

	/// Whether this connection is *fully* idle and thus parkable: past the
	/// handshake, with no in-flight QoS state in either direction, nothing held
	/// back by the outbound window, no buffered inbound bytes (a partial frame is
	/// buffered bytes), and every outbound byte flushed. The mailbox is not
	/// checked here — the event loop only consults this after its drain phase,
	/// when the mailbox is provably empty.
	fn park_ready(&self) -> bool {
		self.connected
			&& self.inflight.is_empty()
			&& self.incoming_qos2.is_empty()
			&& self.pending_outbound.is_empty()
			&& self.inbound.is_empty()
			&& self.outbound.is_empty()
			&& self.partial_since.is_none()
	}

	/// Destructures a parking connection into its transport stream and the
	/// [`ResumeState`] needed to rebuild it. Only meaningful after `run()`
	/// returned [`Flow::Park`].
	pub(crate) fn into_parts(self) -> (S, Box<ResumeState>) {
		let state = Box::new(ResumeState {
			client_id: self.client_id,
			session_generation: self.session_generation,
			next_pkid: self.next_pkid,
			session_expiry: self.session_expiry,
			keepalive: self.keepalive,
			deadline: self.deadline,
			will: self.will,
			will_delay: self.will_delay,
			username: self.username,
			peer_receive_max: self.peer_receive_max,
			peer_max_packet_size: self.peer_max_packet_size,
			peer_topic_alias_max: self.peer_topic_alias_max,
			aliases: self.aliases,
			subscription_count: self.subscription_count,
			rate_limiter: self.rate_limiter,
		});
		(self.stream, state)
	}

	/// Rebuilds a parked connection around its (re-materialized) stream: the
	/// unpark counterpart of [`into_parts`](Self::into_parts). The connection
	/// comes back `connected` and `counted` (the gauge was never decremented — the
	/// client never disconnected) with a fresh mailbox pair; the next `run()`
	/// re-attaches the session and replays whatever queued while parked.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn resume(
		stream: S,
		state: ResumeState,
		shard_id: usize,
		shard: Rc<RefCell<ShardState>>,
		limits: LimitsConfig,
		auth: Rc<Authenticator>,
		metrics: Arc<Metrics>,
		shutdown: Arc<AtomicBool>,
		park_grace: Duration,
	) -> Self {
		let (mailbox_tx, mailbox_rx) = local_channel::new_unbounded();
		Self {
			stream,
			inbound: BytesMut::with_capacity(limits.initial_read_buffer),
			outbound: BytesMut::new(),
			read_chunk: READ_CHUNK_MIN,
			shard_id,
			client_id: state.client_id,
			shard,
			mailbox_tx: Some(mailbox_tx),
			mailbox_rx,
			incoming_qos2: HashMap::new(),
			inflight: HashMap::new(),
			next_pkid: state.next_pkid,
			limits,
			session_expiry: state.session_expiry,
			session_generation: state.session_generation,
			will: state.will,
			peer_receive_max: state.peer_receive_max,
			peer_max_packet_size: state.peer_max_packet_size,
			pending_outbound: VecDeque::new(),
			auth,
			username: state.username,
			metrics,
			counted: true,
			shutdown,
			will_delay: state.will_delay,
			peer_topic_alias_max: state.peer_topic_alias_max,
			aliases: state.aliases,
			connected: true,
			tls_identity: TlsIdentity::None, // only plain TCP parks
			// The keep-alive deadline continues from where it froze at park time:
			// the client's transmission clock never stopped. Refreshed by its next
			// inbound packet as usual.
			deadline: state.deadline,
			keepalive: state.keepalive,
			partial_since: None,
			subscription_count: state.subscription_count,
			rate_limiter: state.rate_limiter,
			park_grace: Some(park_grace),
			last_activity: Instant::now(),
			resume_pending: true,
		}
	}

	/// Boxes the unpark prelude — session reattach plus replay of everything
	/// queued while parked — on a plain stack frame (see [`process_one`] for the
	/// pattern). Returns `Ok(false)` when the session was taken over while the
	/// wake was in flight: displaced-connection semantics — no Will, no session
	/// teardown (a newer connection owns it), only the gauge decrement in
	/// `run()`'s tail still applies.
	fn boxed_resume_prelude(&mut self) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<bool>> + '_>> {
		Box::pin(async move {
			match self.reattach() {
				None => {
					debug!("parked session gone on resume (taken over), closing");
					self.will = None;
					self.client_id.clear();
					Ok(false)
				}
				Some(queued) => {
					self.resume_delivery(queued).await?;
					debug!("connection resumed from parked");
					Ok(true)
				}
			}
		})
	}

	/// Re-attaches a resumed connection to its parked session, restoring the
	/// snapshot and returning what queued while parked. `None` means the session
	/// was taken over (or claimed away) while the wake was in flight — the caller
	/// closes quietly with displaced-connection semantics.
	fn reattach(&mut self) -> Option<VecDeque<Delivery>> {
		let mailbox = self
			.mailbox_tx
			.take()
			.expect("a resumed connection holds its fresh mailbox sender");
		let (snapshot, queued) =
			self.shard
				.borrow_mut()
				.reattach_parked(&self.client_id, self.session_generation, mailbox)?;
		self.inflight = snapshot.inflight;
		self.incoming_qos2 = snapshot.incoming_qos2;
		self.next_pkid = snapshot.next_pkid;
		Some(queued)
	}

	/// Encodes a single MQTT packet into the coalesced output buffer, mapping any
	/// serialization failure to an I/O error. Nothing touches the socket here:
	/// the buffered batch goes out in one write at the next [`flush`](Self::flush)
	/// (the event loop flushes before every blocking wait, so ordering and
	/// promptness are preserved while syscalls, TLS records, and WebSocket frames
	/// are amortized across the whole wakeup).
	fn send<F>(&mut self, encode: F) -> Result<()>
	where
		F: FnOnce(&mut BytesMut) -> std::result::Result<usize, MqttError>,
	{
		encode(&mut self.outbound)
			.map(|_| ())
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
	}

	/// Writes the coalesced output buffer to the socket in one `write_all`.
	async fn flush(&mut self) -> Result<()> {
		if self.outbound.is_empty() {
			return Ok(());
		}
		self.stream.write_all(&self.outbound).await?;
		self.outbound.clear();
		Ok(())
	}

	/// Queues a server-initiated DISCONNECT with the given reason; it reaches the
	/// wire at the next flush (every exit path flushes best-effort).
	fn send_disconnect(&mut self, reason: mqtt_v5::DisconnectReasonCode) -> Result<()> {
		let mut disconnect = mqtt_v5::Disconnect::new();
		disconnect.reason_code = reason;
		self.send(|buf| disconnect.write(buf))
	}

	/// Mutable access to the topic-alias tables, allocating the box on first use
	/// (the first alias this connection registers or is assigned).
	fn aliases_mut(&mut self) -> &mut AliasTables {
		self.aliases.get_or_insert_with(Box::default)
	}

	/// Releases oversized buffer allocations once they empty, so one burst (a
	/// large packet in, a deep fan-out out) doesn't pin its high-water memory on
	/// an idle connection. The next use re-grows on demand.
	fn shrink_buffers(&mut self) {
		if self.inbound.is_empty() && self.inbound.capacity() > BUFFER_RETAIN_MAX {
			self.inbound = BytesMut::new();
		}
		if self.outbound.is_empty() && self.outbound.capacity() > BUFFER_RETAIN_MAX {
			self.outbound = BytesMut::new();
		}
	}

	pub(crate) async fn run(&mut self) -> Result<Flow> {
		debug!("connection opened");

		let result = if self.resume_pending {
			// Unparked: re-attach the session, replay what queued while parked,
			// then fall into the normal event loop. The prelude is boxed through
			// a plain-fn seam (like the cold handler arms) so its slots don't
			// live in every connection's long-lived `run()` machine.
			self.resume_pending = false;
			match self.boxed_resume_prelude().await {
				Ok(true) => self.event_loop().await,
				Ok(false) => Ok(Flow::Closed),
				Err(e) => Err(e),
			}
		} else {
			self.event_loop().await
		};

		// Parked: no cleanup at all — the client is still connected. The session,
		// the connected-clients gauge, and the Will stay live; the caller now owns
		// the fd + resume state (via `into_parts`) and completes the transition.
		if matches!(result, Ok(Flow::Park)) {
			debug!("connection parking");
			return result;
		}

		// Best-effort: put any still-buffered output (a reject CONNACK, a final
		// DISCONNECT) on the wire before tearing the session down. The connection
		// is closing either way, so a write failure here is not an error.
		let _ = self.flush().await;

		// Release our hold on the session, whichever way the loop exited. Depending
		// on the negotiated expiry this either destroys the session (and its
		// subscriptions) or suspends it for a later reconnect, handing over our
		// durable QoS state so it survives the gap. The generation guard makes this
		// a no-op if a newer connection already took over our client id.
		//
		// If we still owned the session and a Will Message survives (i.e. the loop
		// ended abnormally, not via a normal DISCONNECT), publish it. A takeover
		// returns `owned == false`, so a displaced connection never fires its will.
		if !self.client_id.is_empty() {
			let snapshot = crate::broker::session::SessionSnapshot {
				inflight: std::mem::take(&mut self.inflight),
				incoming_qos2: std::mem::take(&mut self.incoming_qos2),
				next_pkid: self.next_pkid,
			};
			let pending = std::mem::take(&mut self.pending_outbound);
			let will = self.will.take();
			let owned = self.shard.borrow_mut().close_session(
				&self.client_id,
				self.session_generation,
				self.session_expiry,
				snapshot,
				pending,
			);
			if owned && let Some(will) = will {
				// Will Delay Interval: fire after min(will delay, session expiry). A
				// zero delay (or a session that didn't outlive us) publishes now; a
				// non-zero delay arms the will on the suspended session for the sweep
				// timer to publish later, cancelled if the client reconnects first.
				let delay = self.will_delay.min(self.session_expiry);
				if delay == 0 {
					debug!(topic = %will.topic, "publishing will message");
					// Broker-originated, so no publisher for No Local. Reuses the
					// reliable forward path (QoS > 0 wills apply mesh backpressure).
					self.fan_out(*will, None).await;
				} else {
					debug!(topic = %will.topic, delay, "arming delayed will message");
					self.shard
						.borrow_mut()
						.arm_will(&self.client_id, self.session_generation, *will, delay);
				}
			}
		}

		// Balance the connected-client gauge if this connection was ever counted.
		if self.counted {
			self.metrics.client_disconnected();
			self.counted = false;
		}

		debug!("connection closed");
		result
	}

	/// The stall deadline for an in-progress (incomplete) frame: a client that has
	/// sent part of a packet must finish it within the handshake window
	/// (`connect_timeout`). This bounds a slow-loris that dribbles a frame header
	/// and stalls — including *after* CONNECT with keep-alive disabled, where the
	/// idle deadline is otherwise `None`. A zero `connect_timeout` disables it.
	fn framing_deadline(&self) -> Option<Instant> {
		let secs = u64::from(self.limits.connect_timeout);
		match self.partial_since {
			Some(started) if secs > 0 => Some(started + Duration::from_secs(secs)),
			_ => None,
		}
	}

	/// Bidirectional event loop, structured as *drain → flush → block*:
	///
	/// 1. Process every complete packet already in the assembly buffer and every
	///    delivery already queued in the mailbox (both synchronous — responses
	///    accumulate in the coalesced output buffer).
	/// 2. Flush the whole batch in one write, then trim oversized idle buffers.
	/// 3. Block: race a socket read against a mailbox delivery and the idle
	///    deadline, then loop.
	///
	/// Everything one wakeup produces — acks for a burst of PUBLISHes, a fan-out
	/// of deliveries — thus leaves in a single io_uring op instead of one per
	/// packet.
	async fn event_loop(&mut self) -> Result<Flow> {
		let max_packet = self.limits.max_payload_size;
		loop {
			// Drain: every complete packet already buffered. Parsing and dispatch
			// share one function (and thus one `Packet`-sized slot, ~384 bytes) —
			// a separate parse-here-dispatch-there split would hold two.
			loop {
				match self.process_one(max_packet).await {
					Ok(true) => {}
					Ok(false) => break, // need more bytes
					Err(e) => {
						warn!(error = %e, "protocol/io error, closing connection");
						return Ok(Flow::Closed);
					}
				}
				// Any inbound packet refreshes the keep-alive deadline and marks
				// activity for the parking grace clock.
				self.last_activity = Instant::now();
				if let Some(window) = self.keepalive {
					self.deadline = Some(Instant::now() + window);
				}
				// A complete frame was consumed: clear the stall clock so the next
				// frame (if bytes for one are already buffered) starts a fresh one.
				self.partial_since = None;
				// A large ack burst shouldn't balloon the output buffer.
				if self.outbound.len() >= FLUSH_THRESHOLD {
					self.flush().await?;
				}
			}

			// Drain: every delivery already queued in the mailbox (without
			// blocking — `poll_once` returns `None` the moment it would park).
			loop {
				match futures_lite::future::poll_once(self.mailbox_rx.recv()).await {
					Some(Some(delivery)) => {
						if let Err(e) = self.deliver(delivery) {
							warn!(error = %e, "delivery error, closing connection");
							return Err(e);
						}
						if self.outbound.len() >= FLUSH_THRESHOLD {
							self.flush().await?;
						}
					}
					Some(None) => return self.mailbox_closed(),
					None => break,
				}
			}

			// Flush the coalesced batch, then trim what the burst grew.
			self.flush().await?;
			self.shrink_buffers();

			// Block until bytes arrive, a delivery lands, or the deadline lapses.
			// The read reserves `read_chunk` bytes in the assembly buffer and reads
			// directly into it (no intermediate copy). `valid` marks the real data
			// length so a cancelled read's zeroed reservation is always dropped.
			// Track an in-progress (incomplete) frame: leftover bytes that don't yet
			// form a complete packet start the stall clock. A frame that then stalls
			// — a slow-loris, or the truncated-CONNECT-header adversarial case — is
			// reaped by `framing_deadline`, even when the idle `deadline` is `None`
			// (keep-alive disabled). No partial frame ⇒ no framing bound.
			if self.inbound.is_empty() {
				self.partial_since = None;
			} else if self.partial_since.is_none() {
				self.partial_since = Some(Instant::now());
			}

			// The parking deadline: armed only when this connection opted in, is
			// fully idle right now (the drain phases above left nothing pending),
			// and the broker isn't shutting down. If the race below resolves as a
			// timeout on this deadline, nothing arrived in between — the connection
			// is provably still idle and parks.
			let park_at = match self.park_grace {
				Some(grace) if self.park_ready() && !self.shutdown.load(Ordering::Relaxed) => {
					Some(self.last_activity + grace)
				}
				_ => None,
			};
			let valid = self.inbound.len();
			let close_deadline = earlier(self.deadline, self.framing_deadline());
			let deadline = earlier(close_deadline, park_at);
			let chunk = self.read_chunk;
			let event = {
				let stream = &mut self.stream;
				let buffer = &mut self.inbound;
				let read = async {
					buffer.resize(valid + chunk, 0);
					match stream.read(&mut buffer[valid..]).await {
						Ok(n) => {
							buffer.truncate(valid + n);
							Event::Bytes(n)
						}
						Err(e) => Event::ReadErr(e),
					}
				};
				let recv = async { Event::Outgoing(self.mailbox_rx.recv().await) };
				let idle = async {
					match deadline {
						Some(dl) => {
							let now = Instant::now();
							glommio::timer::sleep(dl.saturating_duration_since(now)).await;
							Event::Timeout
						}
						None => std::future::pending().await,
					}
				};
				read.or(recv).or(idle).await
			};

			match event {
				Event::Bytes(0) => break, // Client closed (EOF)
				Event::Bytes(n) => {
					// Adapt the next reservation: a full read suggests more is
					// coming (grow); a nearly-empty one suggests idling (shrink).
					if n == chunk {
						self.read_chunk = (chunk * 2).min(READ_CHUNK_MAX);
					} else if n < chunk / 4 {
						self.read_chunk = (chunk / 2).max(READ_CHUNK_MIN);
					}
				}
				Event::ReadErr(e) => {
					self.inbound.truncate(valid);
					warn!(error = %e, "network error, closing connection");
					return Err(e);
				}
				Event::Outgoing(delivery) => {
					// The read lost the race: drop its zeroed reservation.
					self.inbound.truncate(valid);
					match delivery {
						Some(delivery) => {
							if let Err(e) = self.deliver(delivery) {
								warn!(error = %e, "delivery error, closing connection");
								return Err(e);
							}
						}
						None => return self.mailbox_closed(),
					}
				}
				// A deadline lapsed. Which one decides the outcome: the close deadline
				// (handshake, keep-alive, or framing stall) drops the connection; the
				// parking deadline — reachable only when it was armed, i.e. the
				// connection was fully idle going into this block — parks it.
				Event::Timeout => {
					self.inbound.truncate(valid);
					let now = Instant::now();
					if park_at.is_none() || close_deadline.is_some_and(|dl| dl <= now) {
						// A partial frame still buffered means the deadline that fired
						// was the framing (stall) bound, not an idle keep-alive lapse.
						let stalled_mid_frame = !self.inbound.is_empty();
						if self.connected {
							warn!(
								stalled_mid_frame,
								"keep-alive or framing timeout, closing connection"
							);
							let _ = self.send_disconnect(mqtt_v5::DisconnectReasonCode::KeepAliveTimeout);
						} else {
							warn!("CONNECT not received or completed within handshake timeout, closing");
						}
						return Ok(Flow::Closed);
					}
					// The parking grace elapsed while fully idle. The timeout winning
					// the race proves no bytes and no delivery arrived meanwhile; one
					// final non-yielding mailbox check (`poll_once` resolves on its
					// first poll) is belt-and-braces before handing the connection to
					// the caller for the synchronous park transition.
					debug_assert!(self.park_ready());
					match futures_lite::future::poll_once(self.mailbox_rx.recv()).await {
						None => return Ok(Flow::Park),
						Some(Some(delivery)) => {
							// A delivery slipped in after all: don't park, serve it.
							if let Err(e) = self.deliver(delivery) {
								warn!(error = %e, "delivery error, closing connection");
								return Err(e);
							}
						}
						Some(None) => return self.mailbox_closed(),
					}
				}
			}
		}

		Ok(Flow::Closed)
	}

	/// The mailbox sender was dropped — either the server is shutting down (tell
	/// the client and suppress the will) or a new connection took over our client
	/// id (just close). `run()` flushes the DISCONNECT on the way out.
	fn mailbox_closed(&mut self) -> Result<Flow> {
		if self.shutdown.load(Ordering::Relaxed) {
			self.will = None;
			let _ = self.send_disconnect(mqtt_v5::DisconnectReasonCode::ServerShuttingDown);
		}
		Ok(Flow::Closed)
	}

	/// Frames one complete MQTT packet out of `buffer`, or `None` if more bytes
	/// are needed. Synchronous: never touches the socket.
	fn parse_packet(buffer: &mut BytesMut, max_packet: usize) -> Result<Option<Packet>> {
		if buffer.is_empty() {
			return Ok(None);
		}
		// First byte (fixed header) before `read` consumes the frame; used to
		// recognise the zero-length DISCONNECT that mqttbytes can't parse.
		let first_byte = buffer.first().copied();
		match mqtt_v5::read(buffer, max_packet) {
			Ok(packet) => Ok(Some(packet)),
			Err(MqttError::InsufficientBytes(_)) => Ok(None),
			// mqttbytes rejects any zero-length packet other than PING as
			// `PayloadRequired`, but a bare `E0 00` DISCONNECT is a valid MQTT 5
			// normal disconnect. Synthesize one so it flows through
			// `handle_disconnect` (which suppresses the will) instead of being
			// mistaken for an abrupt EOF, which would wrongly fire the will.
			// Nothing after a DISCONNECT matters, so drop the remaining bytes.
			Err(MqttError::PayloadRequired) if first_byte.map(|b| b >> 4) == Some(14) => {
				buffer.clear();
				Ok(Some(Packet::Disconnect(mqtt_v5::Disconnect::new())))
			}
			Err(e) => Err(Error::new(
				ErrorKind::InvalidData,
				format!("MQTT Parse Error: {:?}", e),
			)),
		}
	}

	/// Parses one complete packet from the assembly buffer and processes it.
	/// `Ok(false)` means the buffer needs more bytes for a complete packet.
	///
	/// Parse and dispatch live in one function so a packet occupies one
	/// `Packet`-sized slot in the connection's state machine, and the handler
	/// futures large enough to matter — CONNECT (auth, claim/migration, resume),
	/// PUBLISH (fan-out), and PUBREL (the deferred QoS 2 fan-out) — are boxed
	/// through plain-fn seams: one small allocation per such packet buys those
	/// kilobytes out of every connection's *resident* memory. The remaining
	/// inline arms are tiny (acks and pings are effectively synchronous).
	async fn process_one(&mut self, max_packet: usize) -> Result<bool> {
		let Some(packet) = Self::parse_packet(&mut self.inbound, max_packet)? else {
			return Ok(false);
		};

		// Enforce the CONNECT handshake ordering: the first packet must be a CONNECT,
		// and exactly one CONNECT is allowed. This closes the pre-auth bypass where a
		// client could PUBLISH/SUBSCRIBE before (or without) authenticating.
		let is_connect = matches!(packet, Packet::Connect(_));
		if self.connected && is_connect {
			warn!("second CONNECT received, closing connection");
			self.send_disconnect(mqtt_v5::DisconnectReasonCode::ProtocolError)?;
			return Err(Error::new(ErrorKind::InvalidData, "duplicate CONNECT"));
		}
		if !self.connected && !is_connect {
			warn!("first packet was not CONNECT, closing connection");
			return Err(Error::new(ErrorKind::InvalidData, "expected CONNECT"));
		}

		match packet {
			// Client -> Server Requests
			Packet::Connect(connect) => self.boxed_handle_connect(connect).await,
			Packet::Publish(publish) => self.boxed_handle_publish(publish).await,
			Packet::Subscribe(subscribe) => self.handle_subscribe(subscribe).await,
			Packet::Unsubscribe(unsubscribe) => self.handle_unsubscribe(unsubscribe).await,
			Packet::PingReq => self.handle_ping().await,
			Packet::Disconnect(disconnect) => self.handle_disconnect(disconnect).await,

			// QoS 1 & 2 Flows (Client Responses)
			Packet::PubAck(puback) => self.handle_puback(puback).await,
			Packet::PubRec(pubrec) => self.handle_pubrec(pubrec).await,
			Packet::PubRel(pubrel) => self.boxed_handle_pubrel(pubrel).await,
			Packet::PubComp(pubcomp) => self.handle_pubcomp(pubcomp).await,

			// Server-only packets — a client must never send these.
			Packet::ConnAck(_) | Packet::SubAck(_) | Packet::UnsubAck(_) | Packet::PingResp => {
				warn!("protocol violation: received server-only packet from client");
				Ok(())
			}
		}
		.map(|()| true)
	}

	/// Boxes the CONNECT handler on a plain stack frame (see [`process_one`]).
	fn boxed_handle_connect(
		&mut self,
		connect: mqtt_v5::Connect,
	) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<()>> + '_>> {
		Box::pin(self.handle_connect(connect))
	}

	/// Boxes the PUBLISH handler on a plain stack frame (see [`process_one`]).
	fn boxed_handle_publish(
		&mut self,
		publish: mqtt_v5::Publish,
	) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<()>> + '_>> {
		Box::pin(self.handle_publish(publish))
	}

	/// Boxes the PUBREL handler on a plain stack frame (see [`process_one`]).
	fn boxed_handle_pubrel(
		&mut self,
		pubrel: mqtt_v5::PubRel,
	) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<()>> + '_>> {
		Box::pin(self.handle_pubrel(pubrel))
	}
}
