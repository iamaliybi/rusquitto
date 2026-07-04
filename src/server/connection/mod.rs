//! Per-client MQTT connection: the protocol state machine.
//!
//! [`Connection`] owns one client socket (any [`ByteStream`], so the same logic
//! serves plain TCP and WebSocket) and drives it from CONNECT to close. The
//! implementation is split by responsibility across sibling modules:
//!
//! - [`connect`] — the CONNECT handshake, authentication, and session resume.
//! - [`publish`] — inbound PUBLISH handling and the receiver-side QoS flows.
//! - [`subscribe`] — SUBSCRIBE / UNSUBSCRIBE and retained replay.
//! - [`ack`] — PING, DISCONNECT, and the sender-side QoS acknowledgements.
//! - [`delivery`] — the outbound path: window control, fan-out, retransmit.

mod ack;
mod connect;
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::auth::Authenticator;
use crate::broker::session::{Delivery, InflightMessage, Mailbox};
use crate::broker::shard::ShardState;
use crate::config::LimitsConfig;
use crate::telemetry::metrics::Metrics;
use crate::transport::ByteStream;

/// Monotonic counter for assigning identifiers to clients that connect without
/// one (MQTT 5 allows an empty client id, leaving the server to assign it).
/// Combined with the shard id it is unique across the whole broker.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

/// How long a CONNECT handler waits for peers to answer a cross-shard session
/// [`Claim`](crate::broker::mesh::SessionControl::Claim) before giving up and
/// treating the session as fresh. Mesh replies normally arrive in microseconds;
/// this only bounds the wait if a reply is dropped (drop-on-full mesh) or a peer
/// is wedged, so it can be generous without slowing the common case (which
/// resolves as soon as every peer has answered).
const SESSION_CLAIM_TIMEOUT: Duration = Duration::from_millis(250);

/// Stack scratch buffer for each socket read. The growable assembly buffer is
/// `buffer` (sized from config); a fixed size keeps this one on the stack.
const READ_BUFFER_SIZE: usize = 2048;

/// Longest client identifier the broker accepts (the spec only mandates support
/// for 23; we allow generously more but bound it to reject abuse).
const MAX_CLIENT_ID_LEN: usize = 256;

/// Upper bound on QoS > 0 messages held for a connected client whose in-flight
/// window is full. Beyond this the oldest held message is dropped, so a client
/// that stops acknowledging can't force unbounded broker memory growth.
const PENDING_OUTBOUND_LIMIT: usize = 4096;

/// Capacity of a connection's outbound mailbox. Bounding it is a hard DoS guard:
/// if a subscriber stops reading its socket, its connection task parks on the
/// blocked write and stops draining the mailbox — an *unbounded* mailbox would
/// then grow without limit as other clients keep publishing to it. A full mailbox
/// drops further deliveries for that stuck consumer instead of exhausting memory.
const MAILBOX_CAPACITY: usize = 8192;

/// One iteration of the connection event loop resolves to exactly one of these:
/// either the client sent us bytes, or the broker routed a message to us.
enum Event {
	/// A parsed packet (or EOF) arrived from the client socket. Boxed because a
	/// `Packet` (its `Connect` variant especially) is much larger than a `Delivery`.
	Incoming(Result<Option<Box<Packet>>>),
	/// A message was routed into this connection's mailbox for delivery.
	/// `None` means the channel closed (all senders dropped).
	Outgoing(Option<Delivery>),
	/// The idle deadline (handshake or keep-alive) lapsed.
	Timeout,
}

pub struct Connection<S: ByteStream> {
	stream: S,
	buffer: BytesMut,
	shard_id: usize,
	client_id: String,
	/// Shard-local broker state, shared with every other connection on this core.
	state: Rc<RefCell<ShardState>>,
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
	will: Option<mqtt_v5::Publish>,
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
	/// Inbound topic-alias table (MQTT 5): maps an alias the client has registered
	/// (by sending a PUBLISH with both a topic and an alias) to its topic, so later
	/// PUBLISHes may carry the alias with an empty topic.
	inbound_aliases: HashMap<u16, String>,
	/// Set once a valid CONNECT has been accepted. Every other packet type is a
	/// protocol violation before this, and a second CONNECT is a violation after.
	connected: bool,
	/// Idle deadline: the CONNECT handshake deadline before connecting, then the
	/// keep-alive deadline (1.5× the negotiated keep-alive) afterwards. `None`
	/// disables the check. Reset on every inbound packet.
	deadline: Option<Instant>,
	/// The keep-alive window (1.5× the negotiated interval), used to refresh
	/// `deadline` after each inbound packet. `None` when keep-alive is disabled.
	keepalive: Option<Duration>,
	/// Count of active subscriptions, enforced against `limits.max_subscriptions_per_client`.
	subscription_count: usize,
	/// Per-connection inbound PUBLISH throttle. `Some` when `limits.max_message_rate`
	/// is set: bounds how much CPU one noisy publisher can draw on its pinned core.
	rate_limiter: Option<TokenBucket>,
}

impl<S: ByteStream> Connection<S> {
	/// Largest inbound topic alias the broker accepts, advertised to clients as the
	/// CONNACK Topic Alias Maximum.
	const INBOUND_TOPIC_ALIAS_MAX: u16 = 16;

	pub fn new(
		stream: S,
		shard_id: usize,
		state: Rc<RefCell<ShardState>>,
		limits: LimitsConfig,
		auth: Rc<Authenticator>,
		metrics: Arc<Metrics>,
		shutdown: Arc<AtomicBool>,
	) -> Self {
		let (mailbox_tx, mailbox_rx) = local_channel::new_bounded(MAILBOX_CAPACITY);
		Self {
			stream,
			buffer: BytesMut::with_capacity(limits.initial_read_buffer),
			shard_id,
			client_id: String::new(),
			state,
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
			inbound_aliases: HashMap::new(),
			connected: false,
			// Bound the pre-CONNECT handshake so an idle socket can't hold a slot.
			deadline: (limits.connect_timeout > 0)
				.then(|| Instant::now() + Duration::from_secs(u64::from(limits.connect_timeout))),
			keepalive: None,
			subscription_count: 0,
			rate_limiter: (limits.max_message_rate > 0)
				.then(|| TokenBucket::per_second(limits.max_message_rate, Instant::now())),
		}
	}

	/// Encodes a single MQTT packet and writes it to the socket, mapping any
	/// serialization failure to an I/O error. Centralizes the encode-then-write
	/// boilerplate shared by every acknowledgement path.
	async fn send<F>(&mut self, encode: F) -> Result<()>
	where
		F: FnOnce(&mut BytesMut) -> std::result::Result<usize, MqttError>,
	{
		let mut buf = BytesMut::new();
		encode(&mut buf).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	/// Sends a server-initiated DISCONNECT with the given reason (best effort).
	async fn send_disconnect(&mut self, reason: mqtt_v5::DisconnectReasonCode) -> Result<()> {
		let mut disconnect = mqtt_v5::Disconnect::new();
		disconnect.reason_code = reason;
		self.send(|buf| disconnect.write(buf)).await
	}

	pub async fn run(&mut self) -> Result<()> {
		debug!("connection opened");

		let result = self.event_loop().await;

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
			let owned = self.state.borrow_mut().close_session(
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
					self.fan_out(will, None).await;
				} else {
					debug!(topic = %will.topic, delay, "arming delayed will message");
					self.state
						.borrow_mut()
						.arm_will(&self.client_id, self.session_generation, will, delay);
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

	/// Bidirectional event loop: race an inbound socket read against an outbound
	/// mailbox delivery and an idle-deadline timer, handling whichever fires first.
	async fn event_loop(&mut self) -> Result<()> {
		let max_packet = self.limits.max_payload_size;
		loop {
			// Borrow disjoint fields so the futures don't all need `&mut self`.
			let deadline = self.deadline;
			let event = {
				let read = async {
					Event::Incoming(
						Self::read_packet(&mut self.stream, &mut self.buffer, max_packet)
							.await
							.map(|opt| opt.map(Box::new)),
					)
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
				Event::Incoming(Ok(Some(packet))) => {
					if let Err(e) = self.process_packet(*packet).await {
						warn!(error = %e, "protocol/io error, closing connection");
						return Ok(());
					}
					// Any inbound packet refreshes the keep-alive deadline.
					if let Some(window) = self.keepalive {
						self.deadline = Some(Instant::now() + window);
					}
				}
				Event::Incoming(Ok(None)) => break, // Client closed (EOF)
				Event::Incoming(Err(e)) => {
					warn!(error = %e, "network error, closing connection");
					return Err(e);
				}
				Event::Outgoing(Some(delivery)) => {
					if let Err(e) = self.deliver(delivery).await {
						warn!(error = %e, "delivery error, closing connection");
						return Err(e);
					}
				}
				// The mailbox sender was dropped — either the server is shutting down
				// (tell the client and suppress the will) or a new connection took
				// over our client id (just close).
				Event::Outgoing(None) => {
					if self.shutdown.load(Ordering::Relaxed) {
						self.will = None;
						let _ = self
							.send_disconnect(mqtt_v5::DisconnectReasonCode::ServerShuttingDown)
							.await;
					}
					break;
				}
				// The idle deadline lapsed: no valid CONNECT in time, or no traffic
				// within the keep-alive window. Either way, drop the connection (an
				// abnormal close, so a keep-alive timeout still fires the will).
				Event::Timeout => {
					if self.connected {
						warn!("keep-alive timeout, closing connection");
						let _ = self
							.send_disconnect(mqtt_v5::DisconnectReasonCode::KeepAliveTimeout)
							.await;
					} else {
						warn!("CONNECT not received within handshake timeout, closing");
					}
					return Ok(());
				}
			}
		}

		Ok(())
	}

	/// Reads from `stream` into `buffer` until a complete MQTT packet can be
	/// framed. Takes the fields directly (not `&mut self`) so it can race against
	/// the mailbox receiver, which borrows a different field.
	async fn read_packet(stream: &mut S, buffer: &mut BytesMut, max_packet: usize) -> Result<Option<Packet>> {
		let mut temp_buf = [0u8; READ_BUFFER_SIZE];

		// One read may carry several MQTT packets; frame as many as are complete.
		loop {
			// First byte (fixed header) before `read` consumes the frame; used to
			// recognise the zero-length DISCONNECT that mqttbytes can't parse.
			let first_byte = buffer.first().copied();
			match mqtt_v5::read(buffer, max_packet) {
				Ok(packet) => return Ok(Some(packet)),
				Err(MqttError::InsufficientBytes(_)) => {
					// Need more bytes.
				}
				// mqttbytes rejects any zero-length packet other than PING as
				// `PayloadRequired`, but a bare `E0 00` DISCONNECT is a valid MQTT 5
				// normal disconnect. Synthesize one so it flows through
				// `handle_disconnect` (which suppresses the will) instead of being
				// mistaken for an abrupt EOF, which would wrongly fire the will.
				Err(MqttError::PayloadRequired) if first_byte.map(|b| b >> 4) == Some(14) => {
					return Ok(Some(Packet::Disconnect(mqtt_v5::Disconnect::new())));
				}
				Err(e) => {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!("MQTT Parse Error: {:?}", e),
					));
				}
			}

			let n = stream.read(&mut temp_buf).await?;
			if n == 0 {
				return Ok(None);
			}

			buffer.extend_from_slice(&temp_buf[..n]);
		}
	}

	async fn process_packet(&mut self, packet: Packet) -> Result<()> {
		// Enforce the CONNECT handshake ordering: the first packet must be a CONNECT,
		// and exactly one CONNECT is allowed. This closes the pre-auth bypass where a
		// client could PUBLISH/SUBSCRIBE before (or without) authenticating.
		let is_connect = matches!(packet, Packet::Connect(_));
		if self.connected && is_connect {
			warn!("second CONNECT received, closing connection");
			self.send_disconnect(mqtt_v5::DisconnectReasonCode::ProtocolError)
				.await?;
			return Err(Error::new(ErrorKind::InvalidData, "duplicate CONNECT"));
		}
		if !self.connected && !is_connect {
			warn!("first packet was not CONNECT, closing connection");
			return Err(Error::new(ErrorKind::InvalidData, "expected CONNECT"));
		}

		match packet {
			// Client -> Server Requests
			Packet::Connect(connect) => self.handle_connect(connect).await,
			Packet::Publish(publish) => self.handle_publish(publish).await,
			Packet::Subscribe(subscribe) => self.handle_subscribe(subscribe).await,
			Packet::Unsubscribe(unsubscribe) => self.handle_unsubscribe(unsubscribe).await,
			Packet::PingReq => self.handle_ping().await,
			Packet::Disconnect(disconnect) => self.handle_disconnect(disconnect).await,

			// QoS 1 & 2 Flows (Client Responses)
			Packet::PubAck(puback) => self.handle_puback(puback).await,
			Packet::PubRec(pubrec) => self.handle_pubrec(pubrec).await,
			Packet::PubRel(pubrel) => self.handle_pubrel(pubrel).await,
			Packet::PubComp(pubcomp) => self.handle_pubcomp(pubcomp).await,

			// Server-only packets — a client must never send these.
			Packet::ConnAck(_) | Packet::SubAck(_) | Packet::UnsubAck(_) | Packet::PingResp => {
				warn!("protocol violation: received server-only packet from client");
				Ok(())
			}
		}
	}
}
