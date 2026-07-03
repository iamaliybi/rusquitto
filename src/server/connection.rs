use bytes::BytesMut;
use futures_lite::{AsyncReadExt, AsyncWriteExt, FutureExt};
use glommio::channels::local_channel::{self, LocalReceiver};
use glommio::net::TcpStream;
use mqttbytes::{
	v5::{self as mqtt_v5, Packet},
	Error as MqttError, QoS,
};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::{Error, ErrorKind, Result};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::{debug, info, warn};

use std::sync::Arc;

use crate::auth::{AuthResult, Authenticator};
use crate::broker::engine::{
	Delivery, InflightMessage, InflightState, Mailbox, MeshMsg, MigratedSession, SessionSnapshot,
	ShardState,
};
use crate::config::LimitsConfig;
use crate::logger::redact;
use crate::metrics::Metrics;

use std::time::Duration;

/// Monotonic counter for assigning identifiers to clients that connect without
/// one (MQTT 5 allows an empty client id, leaving the server to assign it).
/// Combined with the shard id it is unique across the whole broker.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

/// How long a CONNECT handler waits for peers to answer a cross-shard session
/// [`Claim`](crate::broker::engine::SessionControl::Claim) before giving up and
/// treating the session as fresh. Mesh replies normally arrive in microseconds;
/// this only bounds the wait if a reply is dropped (drop-on-full mesh) or a peer
/// is wedged, so it can be generous without slowing the common case (which
/// resolves as soon as every peer has answered).
const SESSION_CLAIM_TIMEOUT: Duration = Duration::from_millis(250);

/// One iteration of the connection event loop resolves to exactly one of these:
/// either the client sent us bytes, or the broker routed a message to us.
enum Event {
	/// A parsed packet (or EOF) arrived from the client socket.
	Incoming(Result<Option<Packet>>),
	/// A message was routed into this connection's mailbox for delivery.
	/// `None` means the channel closed (all senders dropped).
	Outgoing(Option<Delivery>),
}

pub struct Connection {
	stream: TcpStream,
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
}

impl Connection {
	/// Stack scratch buffer for each socket read. The growable assembly buffer is
	/// `buffer` (sized from config); a fixed size keeps this one on the stack.
	const READ_BUFFER_SIZE: usize = 2048;

	pub fn new(
		stream: TcpStream,
		shard_id: usize,
		state: Rc<RefCell<ShardState>>,
		limits: LimitsConfig,
		auth: Rc<Authenticator>,
		metrics: Arc<Metrics>,
		shutdown: Arc<AtomicBool>,
	) -> Self {
		let (mailbox_tx, mailbox_rx) = local_channel::new_unbounded();
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
		}
	}

	/// Largest inbound topic alias the broker accepts, advertised to clients as the
	/// CONNACK Topic Alias Maximum.
	const INBOUND_TOPIC_ALIAS_MAX: u16 = 16;

	/// The outbound in-flight ceiling: the smaller of the client's Receive Maximum
	/// and our own configured `max_inflight`, and always at least 1.
	fn outbound_window(&self) -> usize {
		usize::from(self.peer_receive_max.min(self.limits.max_inflight)).max(1)
	}

	/// Sends a delivery now if the in-flight window has room (QoS 0 always sends),
	/// otherwise holds it in the pending queue for later draining.
	async fn deliver(&mut self, delivery: Delivery) -> Result<()> {
		if delivery.qos == QoS::AtMostOnce || self.inflight.len() < self.outbound_window() {
			self.send_publish(&delivery.publish, delivery.qos, delivery.retain, &delivery.sub_ids)
				.await
		} else {
			self.pending_outbound.push_back(delivery);
			Ok(())
		}
	}

	/// Releases held-back messages up to the in-flight window; called after an
	/// acknowledgement frees a slot.
	async fn drain_pending(&mut self) -> Result<()> {
		while self.inflight.len() < self.outbound_window() {
			let Some(delivery) = self.pending_outbound.pop_front() else {
				break;
			};
			self.send_publish(&delivery.publish, delivery.qos, delivery.retain, &delivery.sub_ids)
				.await?;
		}
		Ok(())
	}

	/// Forwards a publish to peer shards, then fans it out to local subscribers.
	///
	/// The cross-shard forward is where at-least/exactly-once could previously be
	/// lost: a full mesh link dropped the message. Now a **QoS > 0** publish is sent
	/// with the awaiting `send_to`, so the publisher applies backpressure (its own
	/// PUBACK/PUBREC is only written after this returns) rather than dropping —
	/// making the guarantee hold across shards, not just within one. A **QoS 0**
	/// publish keeps the non-blocking `try_send_to` (fire-and-forget). The mesh
	/// senders are cloned out of `ShardState` so its borrow isn't held across the
	/// await. `publisher` is this connection's client id for a client publish (No
	/// Local), or `None` for a broker-originated one such as a Will Message.
	async fn fan_out(&self, message: mqtt_v5::Publish, publisher: Option<&str>) {
		let senders = self.state.borrow().mesh_senders();
		if let Some(senders) = senders {
			let me = senders.peer_id();
			for idx in 0..senders.nr_consumers() {
				if idx == me {
					continue;
				}
				if message.qos == QoS::AtMostOnce {
					let _ = senders.try_send_to(idx, MeshMsg::Publish(message.clone()));
				} else {
					// Backpressure: wait for room so a QoS > 0 message is never dropped
					// on a full mesh link. Err means the peer is gone — nothing to do.
					let _ = senders.send_to(idx, MeshMsg::Publish(message.clone())).await;
				}
			}
		}
		self.state.borrow_mut().deliver_local(message, publisher);
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
			let snapshot = SessionSnapshot {
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
					self.state.borrow_mut().arm_will(
						&self.client_id,
						self.session_generation,
						will,
						delay,
					);
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

	/// Bidirectional event loop: race an inbound socket read against an
	/// outbound mailbox delivery, handling whichever resolves first.
	async fn event_loop(&mut self) -> Result<()> {
		let max_packet = self.limits.max_payload_size;
		loop {
			// Borrow disjoint fields so the two futures don't both need `&mut self`.
			let event = {
				let read = async {
					Event::Incoming(
						Self::read_packet(&mut self.stream, &mut self.buffer, max_packet).await,
					)
				};
				let recv = async { Event::Outgoing(self.mailbox_rx.recv().await) };
				read.or(recv).await
			};

			match event {
				Event::Incoming(Ok(Some(packet))) => {
					if let Err(e) = self.process_packet(packet).await {
						warn!(error = %e, "protocol/io error, closing connection");
						return Ok(());
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
			}
		}

		Ok(())
	}

	/// Sends a server-initiated DISCONNECT with the given reason (best effort).
	async fn send_disconnect(&mut self, reason: mqtt_v5::DisconnectReasonCode) -> Result<()> {
		let mut disconnect = mqtt_v5::Disconnect::new();
		disconnect.reason_code = reason;
		let mut buf = BytesMut::new();
		disconnect
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	/// Reads from `stream` into `buffer` until a complete MQTT packet can be
	/// framed. Takes the fields directly (not `&mut self`) so it can race against
	/// the mailbox receiver, which borrows a different field.
	async fn read_packet(
		stream: &mut TcpStream,
		buffer: &mut BytesMut,
		max_packet: usize,
	) -> Result<Option<Packet>> {
		let mut temp_buf = [0u8; Self::READ_BUFFER_SIZE];

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

	/// Delivers a routed message to this client at the given effective QoS and
	/// retain flag.
	///
	/// QoS 0 is fire-and-forget. QoS 1/2 are assigned a fresh packet id, recorded
	/// in the in-flight window, and delivered with their QoS set; the rest of the
	/// handshake (PUBACK / PUBREC+PUBREL+PUBCOMP) is driven by the ack handlers.
	/// `retain` is decided by the caller (set for a retained replay or a
	/// Retain-As-Published subscriber, cleared for ordinary live fan-out). `sub_ids`
	/// are the Subscription Identifiers to echo to the client.
	async fn send_publish(
		&mut self,
		publish: &mqtt_v5::Publish,
		qos: QoS,
		retain: bool,
		sub_ids: &[usize],
	) -> Result<()> {
		let mut message = publish.clone();
		message.qos = qos;
		message.dup = false;
		message.retain = retain;

		// Property hygiene for delivery: attach this subscriber's Subscription
		// Identifiers, and never forward the publisher's Topic Alias (it is scoped to
		// the publisher's connection; we don't assign outbound aliases). Other v5
		// properties (message expiry, content type, user properties, …) pass through.
		if !sub_ids.is_empty() || message.properties.is_some() {
			let props = message
				.properties
				.get_or_insert_with(|| mqtt_v5::PublishProperties {
					payload_format_indicator: None,
					message_expiry_interval: None,
					topic_alias: None,
					response_topic: None,
					correlation_data: None,
					user_properties: Vec::new(),
					subscription_identifiers: Vec::new(),
					content_type: None,
				});
			props.topic_alias = None;
			props.subscription_identifiers = sub_ids.to_vec();
		}

		let pkid = match qos {
			QoS::AtMostOnce => {
				message.pkid = 0;
				None
			}
			QoS::AtLeastOnce => {
				let pkid = self.alloc_pkid();
				message.pkid = pkid;
				self.track_inflight(pkid, &message, InflightState::Qos1);
				Some(pkid)
			}
			QoS::ExactlyOnce => {
				let pkid = self.alloc_pkid();
				message.pkid = pkid;
				self.track_inflight(pkid, &message, InflightState::Qos2Pending);
				Some(pkid)
			}
		};

		let mut buf = BytesMut::new();
		message
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

		// The client's Maximum Packet Size is a hard ceiling: we must not send a
		// larger packet. Drop it (rolling back the in-flight slot so it doesn't
		// wedge the window) — it can never be delivered to this client.
		if let Some(max) = self.peer_max_packet_size
			&& buf.len() as u64 > u64::from(max)
		{
			warn!(
				size = buf.len(),
				max, "outbound publish exceeds client max packet size, dropping"
			);
			if let Some(pkid) = pkid {
				self.inflight.remove(&pkid);
			}
			return Ok(());
		}

		self.metrics.message_sent(message.payload.len());
		self.stream.write_all(&buf).await
	}

	/// Records an outbound QoS 1/2 message in the in-flight window, keeping a copy
	/// of the PUBLISH so it can be retransmitted with the DUP flag on resume.
	fn track_inflight(&mut self, pkid: u16, message: &mqtt_v5::Publish, state: InflightState) {
		self.inflight.insert(
			pkid,
			InflightMessage {
				publish: message.clone(),
				state,
			},
		);
	}

	/// Allocates the next unused packet id (1..=65535) for an outbound message.
	fn alloc_pkid(&mut self) -> u16 {
		loop {
			self.next_pkid = self.next_pkid.wrapping_add(1);
			if self.next_pkid == 0 {
				self.next_pkid = 1;
			}
			// In practice the in-flight window is tiny, so this resolves at once.
			if !self.inflight.contains_key(&self.next_pkid) {
				return self.next_pkid;
			}
		}
	}

	async fn process_packet(&mut self, packet: Packet) -> Result<()> {
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

impl Connection {
	/// Replies to a rejected CONNECT with a failure CONNACK (session present is
	/// always false) and returns an error to unwind and close the connection.
	async fn reject_connect(&mut self, code: mqtt_v5::ConnectReturnCode) -> Result<()> {
		let mut conn_ack = mqtt_v5::ConnAck::new(code, false);
		// Attach empty properties so mqttbytes emits the mandatory v5 length byte.
		conn_ack.properties = Some(mqtt_v5::ConnAckProperties::new());
		let mut buf = BytesMut::new();
		conn_ack
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await?;
		Err(Error::new(
			ErrorKind::PermissionDenied,
			"authentication failed",
		))
	}

	async fn handle_connect(&mut self, connect: mqtt_v5::Connect) -> Result<()> {
		// Clean Start decides whether an existing session is resumed; the Session
		// Expiry Interval decides how long the session outlives a disconnect.
		let clean_start = connect.clean_session;
		let props = connect.properties.as_ref();
		self.session_expiry = props.and_then(|p| p.session_expiry_interval).unwrap_or(0);

		// Client flow-control limits we must honour on the outbound path. Receive
		// Maximum bounds our unacked QoS 1/2 window (0 is invalid, so clamp to 1);
		// Maximum Packet Size caps the size of any packet we send it.
		self.peer_receive_max = props
			.and_then(|p| p.receive_maximum)
			.unwrap_or(u16::MAX)
			.max(1);
		self.peer_max_packet_size = props.and_then(|p| p.max_packet_size);

		// An empty client id has the server assign one, which must then be echoed
		// back in CONNACK so the client can reconnect to the same session.
		let assigned = connect.client_id.is_empty();
		self.client_id = if assigned {
			let n = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
			format!("auto-{}-{}", self.shard_id, n)
		} else {
			connect.client_id
		};

		// Stash the Will Message (if any) as a ready-to-route Publish, and capture its
		// Will Delay Interval — the will is published this many seconds after an
		// abnormal disconnect (capped by the session expiry), armed on the session in
		// `run()` cleanup.
		if let Some(w) = connect.last_will {
			self.will_delay = w
				.properties
				.as_ref()
				.and_then(|p| p.delay_interval)
				.unwrap_or(0);
			let mut publish = mqtt_v5::Publish::new(w.topic, w.qos, w.message.to_vec());
			publish.retain = w.retain;
			self.will = Some(publish);
		}

		// Backfill the connection span's `client_id` so every subsequent log line
		// for this connection carries it automatically.
		tracing::Span::current().record("client_id", self.client_id.as_str());

		// Authenticate before logging a successful connection or opening any session
		// state. On failure, reply with the matching CONNACK reason code and close.
		let login = connect.login.as_ref();
		let auth = self
			.auth
			.check(login.map(|l| l.username.as_str()), login.map(|l| l.password.as_str()));
		if auth != AuthResult::Granted {
			let code = match auth {
				AuthResult::BadUserNamePassword => mqtt_v5::ConnectReturnCode::BadUserNamePassword,
				_ => mqtt_v5::ConnectReturnCode::NotAuthorized,
			};
			warn!(
				credentials = %redact::credentials(
					login.map(|l| l.username.as_str()),
					login.is_some_and(|l| !l.password.is_empty()),
				),
				reason = ?code,
				"authentication failed, rejecting connection"
			);
			return self.reject_connect(code).await;
		}

		// Remember the authenticated identity for per-topic ACL checks.
		self.username = login.map(|l| l.username.clone());

		// Drop a Will Message whose topic the client isn't authorized to publish.
		let will_authorized = match &self.will {
			Some(will) => self
				.auth
				.authorize_publish(self.username.as_deref(), &will.topic),
			None => true,
		};
		if !will_authorized {
			debug!("will topic not authorized, dropping will");
			self.will = None;
		}

		// Credentials are redacted: the username is logged, the password is never
		// passed to the logger — only its presence is noted.
		info!(
			credentials = %redact::credentials(
				connect.login.as_ref().map(|l| l.username.as_str()),
				connect.login.as_ref().is_some_and(|l| !l.password.is_empty()),
			),
			clean_session = clean_start,
			session_expiry = self.session_expiry,
			keep_alive = connect.keep_alive,
			"client connected"
		);

		// Open or resume the session, handing over our mailbox sender. The shard
		// now owns it (the sender is not Clone); on takeover this displaces any
		// prior live connection for the same client id. Resuming restores the
		// durable QoS state and any messages buffered while we were offline.
		let mut session_present = false;
		let mut offline_queue = VecDeque::new();
		if let Some(mailbox) = self.mailbox_tx.take() {
			let handle =
				self.state
					.borrow_mut()
					.open_session(&self.client_id, mailbox, clean_start);
			self.session_generation = handle.generation;
			session_present = handle.resumed;
			self.inflight = handle.snapshot.inflight;
			self.incoming_qos2 = handle.snapshot.incoming_qos2;
			self.next_pkid = handle.snapshot.next_pkid;
			offline_queue = handle.offline_queue;
		}

		// Cross-shard session resume. `SO_REUSEPORT` may have landed this reconnect
		// on a different shard than the one holding the client's session, so if we
		// opened a *fresh* session on a non-clean connect, ask our peers whether one
		// of them owns it and migrate it here. A Clean Start instead tells peers to
		// discard any session they may still hold from an earlier rehash.
		if clean_start {
			self.state.borrow().broadcast_claim(&self.client_id, false);
		} else if !session_present
			&& let Some(migrated) = self.claim_remote_session().await
		{
			info!("resumed session migrated from another shard");
			let (snapshot, offline) = self
				.state
				.borrow_mut()
				.install_migrated(&self.client_id, migrated);
			self.inflight = snapshot.inflight;
			self.incoming_qos2 = snapshot.incoming_qos2;
			self.next_pkid = snapshot.next_pkid;
			offline_queue = offline;
			session_present = true;
		}

		// The CONNECT succeeded; count this client until it disconnects.
		self.metrics.client_connected();
		self.counted = true;

		debug!(session_present, "session established");

		// NOTE: mqttbytes' `ConnAck::write` omits the mandatory MQTT v5 property-
		// length byte when `properties` is `None`, producing a malformed packet
		// that clients reject. Attach an empty property set so the 0-length is
		// emitted. (SubAck/Publish/PubAck handle the None case correctly.)
		let mut conn_ack =
			mqtt_v5::ConnAck::new(mqtt_v5::ConnectReturnCode::Success, session_present);
		let mut ack_props = mqtt_v5::ConnAckProperties::new();
		// Advertise the server keep-alive so clients adopt our ceiling.
		if self.limits.keep_alive > 0 {
			ack_props.server_keep_alive = Some(self.limits.keep_alive);
		}
		// Tell the client the id we assigned so it can resume this session later.
		if assigned {
			ack_props.assigned_client_identifier = Some(self.client_id.clone());
		}

		// Advertise our capabilities so the client shapes its traffic accordingly.
		// Receive Maximum: how many unacked QoS 1/2 PUBLISHes we accept concurrently.
		ack_props.receive_max = Some(self.limits.max_inflight);
		// Maximum Packet Size we will accept.
		ack_props.max_packet_size = Some(self.limits.max_payload_size as u32);
		// Maximum QoS — only sent when below 2 (absence means QoS 2 supported).
		if self.limits.max_qos < 2 {
			ack_props.max_qos = Some(self.limits.max_qos);
		}
		// Retain Available — 0 signals retained messages are unsupported.
		if !self.limits.retain_available {
			ack_props.retain_available = Some(0);
		}
		// We support wildcard and shared subscriptions, subscription identifiers, and
		// inbound topic aliases.
		ack_props.wildcard_subscription_available = Some(1);
		ack_props.subscription_identifiers_available = Some(1);
		ack_props.shared_subscription_available = Some(1);
		// We accept inbound topic aliases up to this maximum (we send none outbound).
		ack_props.topic_alias_max = Some(Self::INBOUND_TOPIC_ALIAS_MAX);

		conn_ack.properties = Some(ack_props);
		let mut buf = BytesMut::new();
		conn_ack
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

		self.stream.write_all(&buf).await?;

		// After CONNACK, resurrect message flow for a resumed session.
		if session_present {
			self.resume_delivery(offline_queue).await?;
		}

		Ok(())
	}

	/// Broadcasts a session Claim to peer shards and waits (bounded) for their
	/// replies, returning a session if a peer handed one over.
	///
	/// Every peer answers a claim — with its session or a negative reply — so this
	/// normally resolves the instant the last peer responds (or the first session
	/// arrives). The timeout only guards against a reply lost to the drop-on-full
	/// mesh or a wedged peer, so treating that as "no session" is safe (the stranded
	/// session simply expires on its old shard).
	async fn claim_remote_session(&mut self) -> Option<MigratedSession> {
		let nr_peers = self.state.borrow().mesh_peers();
		if nr_peers == 0 {
			return None; // Single-shard broker: no peers to claim from.
		}

		// Register a mailbox for the Handoff replies, then broadcast the claim.
		let (tx, rx) = local_channel::new_unbounded::<Option<MigratedSession>>();
		{
			let mut state = self.state.borrow_mut();
			state.register_claim(self.client_id.clone(), tx);
			state.broadcast_claim(&self.client_id, true);
		}
		debug!(peers = nr_peers, "claiming session from peer shards");

		// Resolve as soon as a session arrives or every peer has answered.
		let collect = async {
			let mut remaining = nr_peers;
			let mut found = None;
			while remaining > 0 {
				match rx.recv().await {
					Some(reply) => {
						remaining -= 1;
						if let Some(session) = reply {
							found = Some(session);
							break;
						}
					}
					None => break,
				}
			}
			found
		};
		let timeout = async {
			glommio::timer::sleep(SESSION_CLAIM_TIMEOUT).await;
			None
		};
		let found = collect.or(timeout).await;

		self.state.borrow_mut().unregister_claim(&self.client_id);
		found
	}

	/// Restores message flow on a resumed session: first retransmit the unacked
	/// in-flight QoS 1/2 messages (with the DUP flag, reusing their packet ids),
	/// then deliver everything buffered while the client was offline.
	async fn resume_delivery(&mut self, offline_queue: VecDeque<Delivery>) -> Result<()> {
		// Encode the retransmissions before writing, so we don't hold a borrow of
		// `self.inflight` across the await points.
		let mut packets: Vec<BytesMut> = Vec::with_capacity(self.inflight.len());
		for (pkid, entry) in &self.inflight {
			let mut buf = BytesMut::new();
			match entry.state {
				// Message not yet acknowledged: resend the PUBLISH marked DUP.
				InflightState::Qos1 | InflightState::Qos2Pending => {
					let mut publish = entry.publish.clone();
					publish.pkid = *pkid;
					publish.dup = true;
					publish
						.write(&mut buf)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				}
				// PUBLISH already acknowledged via PUBREC: resume at PUBREL.
				InflightState::Qos2Released => {
					mqtt_v5::PubRel::new(*pkid)
						.write(&mut buf)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				}
			}
			packets.push(buf);
		}

		if !packets.is_empty() {
			debug!(count = packets.len(), "retransmitting in-flight messages");
			for buf in packets {
				self.stream.write_all(&buf).await?;
			}
		}

		// Deliver messages that arrived while the session was suspended; each gets
		// a fresh packet id via the normal outbound path, respecting the window.
		if !offline_queue.is_empty() {
			debug!(count = offline_queue.len(), "flushing offline queue");
			for delivery in offline_queue {
				self.deliver(delivery).await?;
			}
		}

		Ok(())
	}

	async fn handle_disconnect(&mut self, disconnect: mqtt_v5::Disconnect) -> Result<()> {
		// A normal DISCONNECT (0x00) suppresses the will; reason 0x04 explicitly
		// asks for it, and any other reason code leaves it to fire.
		let reason = disconnect.reason_code;
		if reason == mqtt_v5::DisconnectReasonCode::NormalDisconnection {
			self.will = None;
		}
		info!(reason = ?reason, "client sent disconnect");
		// Returning an error unwinds the event loop and closes the connection.
		Err(Error::new(ErrorKind::ConnectionAborted, "Client Disconnected"))
	}

	async fn handle_ping(&mut self) -> Result<()> {
		let mut buf = BytesMut::new();
		mqtt_v5::PingResp
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	async fn handle_publish(&mut self, mut publish: mqtt_v5::Publish) -> Result<()> {
		// Resolve an inbound topic alias (MQTT 5) before anything else reads the
		// topic. A PUBLISH may register an alias (topic + alias) or use one (empty
		// topic + alias); an out-of-range or unknown alias is a protocol error.
		if let Some(alias) = publish.properties.as_ref().and_then(|p| p.topic_alias) {
			if alias == 0 || alias > Self::INBOUND_TOPIC_ALIAS_MAX {
				warn!(alias, "topic alias out of range, disconnecting");
				self.send_disconnect(mqtt_v5::DisconnectReasonCode::TopicAliasInvalid)
					.await?;
				return Err(Error::new(ErrorKind::InvalidData, "topic alias invalid"));
			}
			if publish.topic.is_empty() {
				match self.inbound_aliases.get(&alias) {
					Some(topic) => publish.topic = topic.clone(),
					None => {
						warn!(alias, "unknown topic alias, disconnecting");
						self.send_disconnect(mqtt_v5::DisconnectReasonCode::TopicAliasInvalid)
							.await?;
						return Err(Error::new(ErrorKind::InvalidData, "unknown topic alias"));
					}
				}
			} else {
				self.inbound_aliases.insert(alias, publish.topic.clone());
			}
		}

		// Payload contents are never logged — only topic, QoS, and byte length.
		debug!(
			topic = %publish.topic,
			qos = ?publish.qos,
			retain = publish.retain,
			payload = %redact::payload(&publish.payload),
			"publish received"
		);

		self.metrics.message_received(publish.payload.len());

		// Enforce publish authorization before routing. On denial the message is
		// not fanned out: QoS > 0 gets a negative acknowledgement (Not Authorized),
		// QoS 0 is dropped silently as there is no way to signal the sender.
		if !self
			.auth
			.authorize_publish(self.username.as_deref(), &publish.topic)
		{
			warn!(topic = %publish.topic, "publish not authorized, rejecting");
			return match publish.qos {
				QoS::AtMostOnce => Ok(()),
				QoS::AtLeastOnce => {
					let mut ack = mqtt_v5::PubAck::new(publish.pkid);
					ack.reason = mqtt_v5::PubAckReason::NotAuthorized;
					let mut buf = BytesMut::new();
					ack.write(&mut buf)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
					self.stream.write_all(&buf).await
				}
				QoS::ExactlyOnce => {
					let mut rec = mqtt_v5::PubRec::new(publish.pkid);
					rec.reason = mqtt_v5::PubRecReason::NotAuthorized;
					let mut buf = BytesMut::new();
					rec.write(&mut buf)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
					self.stream.write_all(&buf).await
				}
			};
		}

		// Normalize for fan-out: clear the publisher's packet id and dup flag, but
		// keep the original QoS so each subscriber can be downgraded individually
		// to `min(publish QoS, granted QoS)` at delivery time.
		let mut msg = publish.clone();
		msg.pkid = 0;
		msg.dup = false;

		// Inbound QoS handshake (receiver side).
		match publish.qos {
			// Fire and forget.
			QoS::AtMostOnce => {
				self.fan_out(msg, Some(&self.client_id)).await;
				Ok(())
			}
			// At least once: forward (awaiting mesh backpressure), then acknowledge —
			// the PUBACK is only sent once the message has been accepted for delivery
			// on every shard, so the guarantee holds cross-shard.
			QoS::AtLeastOnce => {
				self.fan_out(msg, Some(&self.client_id)).await;
				let mut buf = BytesMut::new();
				mqtt_v5::PubAck::new(publish.pkid)
					.write(&mut buf)
					.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				self.stream.write_all(&buf).await
			}
			// Exactly once: store the message and acknowledge receipt with PubRec.
			// Actual delivery is deferred to PUBREL so it happens exactly once even
			// if the publisher re-sends the PUBLISH.
			QoS::ExactlyOnce => {
				// Enforce the inbound Receive Maximum we advertised: bound the number
				// of concurrent unacknowledged QoS 2 PUBLISHes. A fresh pkid past the
				// quota is a protocol violation → DISCONNECT (0x93). A re-send of a
				// pkid we already hold doesn't count against the quota.
				if !self.incoming_qos2.contains_key(&publish.pkid)
					&& self.incoming_qos2.len() >= usize::from(self.limits.max_inflight)
				{
					warn!(
						quota = self.limits.max_inflight,
						"inbound receive maximum exceeded, disconnecting"
					);
					self.send_disconnect(mqtt_v5::DisconnectReasonCode::ReceiveMaximumExceeded)
						.await?;
					return Err(Error::new(ErrorKind::InvalidData, "receive maximum exceeded"));
				}
				self.incoming_qos2.insert(publish.pkid, msg);
				let mut buf = BytesMut::new();
				mqtt_v5::PubRec::new(publish.pkid)
					.write(&mut buf)
					.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				self.stream.write_all(&buf).await
			}
		}
	}

	async fn handle_subscribe(&mut self, subscribe: mqtt_v5::Subscribe) -> Result<()> {
		// Register each filter in this shard's subscription table, build the
		// per-filter SubAck reason codes, and collect any retained messages whose
		// topic matches (to replay to this client after the SubAck).
		let mut return_codes = Vec::with_capacity(subscribe.filters.len());
		let mut retained = Vec::new();

		// A single Subscription Identifier (if any) applies to every filter in this
		// SUBSCRIBE and is echoed on matching deliveries.
		let sub_id = subscribe.properties.as_ref().and_then(|p| p.id);

		for filter in &subscribe.filters {
			// A Shared Subscription filter is `$share/{group}/{topic-filter}`; the
			// effective filter used for matching, ACL, and retained replay is the
			// `{topic-filter}` part, and `group` load-balances delivery.
			let (effective, share_group) = match parse_shared_filter(&filter.path) {
				Ok(pair) => pair,
				Err(()) => {
					warn!(filter = %filter.path, "invalid shared subscription filter");
					return_codes.push(mqtt_v5::SubscribeReasonCode::TopicFilterInvalid);
					continue;
				}
			};

			// Deny unauthorized filters: no trie entry, no retained replay, and a
			// Not Authorized reason code in the SubAck for this filter.
			if !self
				.auth
				.authorize_subscribe(self.username.as_deref(), effective)
			{
				warn!(filter = %effective, "subscribe not authorized, rejecting");
				return_codes.push(mqtt_v5::SubscribeReasonCode::NotAuthorized);
				continue;
			}

			let granted = min_qos(filter.qos, self.limits.max_qos());

			{
				let mut state = self.state.borrow_mut();
				let is_new = state.subscribe(
					effective,
					&self.client_id,
					granted,
					filter.nolocal,
					filter.preserve_retain,
					share_group,
					sub_id,
				);
				// Retain Handling decides whether to replay retained messages now.
				// Shared subscriptions never receive retained messages on subscribe.
				let send_retained = share_group.is_none()
					&& match filter.retain_forward_rule {
						mqtt_v5::RetainForwardRule::OnEverySubscribe => true,
						mqtt_v5::RetainForwardRule::OnNewSubscribe => is_new,
						mqtt_v5::RetainForwardRule::Never => false,
					};
				if send_retained {
					for message in state.retained_matching(effective) {
						retained.push((message, granted));
					}
				}
			}

			debug!(filter = %effective, group = ?share_group, granted = ?granted, "subscribed");

			return_codes.push(reason_code(granted));
		}

		let sub_ack = mqtt_v5::SubAck::new(subscribe.pkid, return_codes);
		let mut buf = BytesMut::new();
		sub_ack
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await?;

		// Replay matching retained messages, delivered with the retain flag set and
		// downgraded to min(message QoS, granted QoS) for this subscription. Routed
		// through `deliver` so the in-flight window is respected. Each carries the
		// SUBSCRIBE's Subscription Identifier (if any).
		let sub_ids: Vec<usize> = sub_id.into_iter().collect();
		for (message, granted) in retained {
			let qos = min_qos(message.qos, granted);
			self.deliver(Delivery {
				publish: Rc::new(message),
				qos,
				retain: true,
				sub_ids: sub_ids.clone(),
			})
			.await?;
		}

		Ok(())
	}

	async fn handle_unsubscribe(&mut self, unsubscribe: mqtt_v5::Unsubscribe) -> Result<()> {
		let mut reasons = Vec::with_capacity(unsubscribe.filters.len());

		for filter in &unsubscribe.filters {
			// Mirror the SUBSCRIBE parse so a `$share/{group}/{topic}` unsubscribe
			// removes the matching shared entry rather than a phantom literal filter.
			let (effective, share_group) = parse_shared_filter(filter).unwrap_or((filter, None));
			self.state
				.borrow_mut()
				.unsubscribe(effective, &self.client_id, share_group);
			debug!(filter = %effective, group = ?share_group, "unsubscribed");
			reasons.push(mqtt_v5::UnsubAckReason::Success);
		}

		let mut unsub_ack = mqtt_v5::UnsubAck::new(unsubscribe.pkid);
		unsub_ack.reasons = reasons;
		let mut buf = BytesMut::new();
		unsub_ack
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	// --- QoS Handlers ---

	async fn handle_puback(&mut self, puback: mqtt_v5::PubAck) -> Result<()> {
		// QoS 1, sender side: the client acknowledged a message we delivered. The
		// transaction is complete; release the packet id and let a held message
		// through the freed window slot.
		if self.inflight.remove(&puback.pkid).is_some() {
			self.drain_pending().await?;
		}
		Ok(())
	}

	async fn handle_pubrec(&mut self, pubrec: mqtt_v5::PubRec) -> Result<()> {
		// QoS 2, sender side (step 2): the client received our PUBLISH. Advance the
		// transaction to "released" and send PUBREL.
		if let Some(entry) = self.inflight.get_mut(&pubrec.pkid) {
			if matches!(entry.state, InflightState::Qos2Pending) {
				entry.state = InflightState::Qos2Released;
			}
		}

		let mut buf = BytesMut::new();
		mqtt_v5::PubRel::new(pubrec.pkid)
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	async fn handle_pubrel(&mut self, pubrel: mqtt_v5::PubRel) -> Result<()> {
		// QoS 2, receiver side: the publisher has released the message. Commit it
		// (deliver exactly once) if we still hold it, then finalize with PubComp.
		if let Some(message) = self.incoming_qos2.remove(&pubrel.pkid) {
			self.fan_out(message, Some(&self.client_id)).await;
		}

		let mut buf = BytesMut::new();
		mqtt_v5::PubComp::new(pubrel.pkid)
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	async fn handle_pubcomp(&mut self, pubcomp: mqtt_v5::PubComp) -> Result<()> {
		// QoS 2, sender side (step 4): the client finalized the transaction.
		// Release the packet id and admit a held message into the freed slot.
		if self.inflight.remove(&pubcomp.pkid).is_some() {
			self.drain_pending().await?;
		}
		Ok(())
	}
}

/// Returns the lower of two QoS levels (the granted QoS is `min(requested, max)`).
fn min_qos(a: QoS, b: QoS) -> QoS {
	if (a as u8) <= (b as u8) { a } else { b }
}

/// Splits a subscription filter into `(effective_filter, share_group)`.
///
/// A Shared Subscription filter is `$share/{ShareName}/{topic-filter}`: the group
/// is `ShareName` and the effective filter is `{topic-filter}`. An ordinary filter
/// returns itself with `None`. A malformed `$share/…` (missing/empty ShareName or
/// topic, or a wildcard in the ShareName) returns `Err(())` so the caller can
/// answer the SUBSCRIBE with `TopicFilterInvalid`.
fn parse_shared_filter(filter: &str) -> std::result::Result<(&str, Option<&str>), ()> {
	let Some(rest) = filter.strip_prefix("$share/") else {
		return Ok((filter, None));
	};
	match rest.split_once('/') {
		Some((group, topic))
			if !group.is_empty()
				&& !topic.is_empty()
				&& !group.contains('+')
				&& !group.contains('#') =>
		{
			Ok((topic, Some(group)))
		}
		_ => Err(()),
	}
}

/// Maps a granted QoS to its SubAck success reason code.
fn reason_code(qos: QoS) -> mqtt_v5::SubscribeReasonCode {
	match qos {
		QoS::AtMostOnce => mqtt_v5::SubscribeReasonCode::QoS0,
		QoS::AtLeastOnce => mqtt_v5::SubscribeReasonCode::QoS1,
		QoS::ExactlyOnce => mqtt_v5::SubscribeReasonCode::QoS2,
	}
}
