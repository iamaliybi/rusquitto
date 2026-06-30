use bytes::BytesMut;
use futures_lite::{AsyncReadExt, AsyncWriteExt, FutureExt};
use glommio::channels::local_channel::{self, LocalReceiver};
use glommio::net::TcpStream;
use mqttbytes::{
	v5::{self as mqtt_v5, Packet},
	Error as MqttError, QoS,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{debug, info, warn};

use crate::broker::engine::{Delivery, Mailbox, ShardState};
use crate::config::LimitsConfig;
use crate::logger::redact;

/// Monotonic counter for assigning identifiers to clients that connect without
/// one (MQTT 5 allows an empty client id, leaving the server to assign it).
/// Combined with the shard id it is unique across the whole broker.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

/// State of an outbound QoS 1/2 message awaiting acknowledgement from the client.
enum Inflight {
	/// QoS 1 PUBLISH sent, awaiting PUBACK.
	Qos1,
	/// QoS 2 PUBLISH sent, awaiting PUBREC.
	Qos2Pending,
	/// QoS 2 PUBREL sent, awaiting PUBCOMP.
	Qos2Released,
}

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
	/// we assigned, awaiting their acknowledgement.
	inflight: HashMap<u16, Inflight>,
	/// Rolling packet-id allocator for outbound QoS 1/2 messages.
	next_pkid: u16,
	/// Broker resource limits (max payload, granted QoS, keep-alive, …).
	limits: LimitsConfig,
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
		}
	}

	/// Forwards a publish to peer shards and fans it out to local subscribers.
	fn fan_out(&self, message: mqtt_v5::Publish) {
		let mut state = self.state.borrow_mut();
		state.broadcast(&message);
		state.deliver_local(message);
	}

	pub async fn run(&mut self) -> Result<()> {
		debug!("connection opened");

		let result = self.event_loop().await;

		// Always deregister so the mailbox sender is dropped and stale
		// subscriptions are purged, whichever way the loop exited.
		if !self.client_id.is_empty() {
			self.state.borrow_mut().disconnect(&self.client_id);
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
					if let Err(e) = self.send_publish(&delivery.publish, delivery.qos).await {
						warn!(error = %e, "delivery error, closing connection");
						return Err(e);
					}
				}
				// The mailbox sender was dropped from the registry — e.g. a new
				// connection reused our client_id and took over the session.
				Event::Outgoing(None) => break,
			}
		}

		Ok(())
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
				// `PayloadRequired`, but a bare `E0 00` DISCONNECT is valid MQTT 5.
				// Treat it as a clean client close.
				Err(MqttError::PayloadRequired) if first_byte.map(|b| b >> 4) == Some(14) => {
					return Ok(None);
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

	/// Delivers a routed message to this client at the given effective QoS.
	///
	/// QoS 0 is fire-and-forget. QoS 1/2 are assigned a fresh packet id, recorded
	/// in the in-flight window, and delivered with their QoS set; the rest of the
	/// handshake (PUBACK / PUBREC+PUBREL+PUBCOMP) is driven by the ack handlers.
	/// The source `publish`'s retain flag is preserved (set for retained replay,
	/// cleared for live fan-out).
	async fn send_publish(&mut self, publish: &mqtt_v5::Publish, qos: QoS) -> Result<()> {
		let mut message = publish.clone();
		message.qos = qos;
		message.dup = false;

		match qos {
			QoS::AtMostOnce => message.pkid = 0,
			QoS::AtLeastOnce => {
				let pkid = self.alloc_pkid();
				message.pkid = pkid;
				self.inflight.insert(pkid, Inflight::Qos1);
			}
			QoS::ExactlyOnce => {
				let pkid = self.alloc_pkid();
				message.pkid = pkid;
				self.inflight.insert(pkid, Inflight::Qos2Pending);
			}
		}

		let mut buf = BytesMut::new();
		message
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
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
			Packet::Disconnect(_) => self.handle_disconnect().await,

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
	async fn handle_connect(&mut self, connect: mqtt_v5::Connect) -> Result<()> {
		self.client_id = if connect.client_id.is_empty() {
			let n = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
			format!("auto-{}-{}", self.shard_id, n)
		} else {
			connect.client_id
		};

		// Backfill the connection span's `client_id` so every subsequent log line
		// for this connection carries it automatically.
		tracing::Span::current().record("client_id", self.client_id.as_str());

		// Credentials are redacted: the username is logged, the password is never
		// passed to the logger — only its presence is noted.
		info!(
			credentials = %redact::credentials(
				connect.login.as_ref().map(|l| l.username.as_str()),
				connect.login.as_ref().is_some_and(|l| !l.password.is_empty()),
			),
			clean_session = connect.clean_session,
			keep_alive = connect.keep_alive,
			"client connected"
		);

		// Hand our mailbox sender to the shard registry so other connections can
		// route messages to us. The registry now owns it (the sender is not Clone).
		if let Some(mailbox) = self.mailbox_tx.take() {
			self.state
				.borrow_mut()
				.register(self.client_id.clone(), mailbox);
		}

		// NOTE: mqttbytes' `ConnAck::write` omits the mandatory MQTT v5 property-
		// length byte when `properties` is `None`, producing a malformed packet
		// that clients reject. Attach an empty property set so the 0-length is
		// emitted. (SubAck/Publish/PubAck handle the None case correctly.)
		let mut conn_ack = mqtt_v5::ConnAck::new(mqtt_v5::ConnectReturnCode::Success, false);
		let mut props = mqtt_v5::ConnAckProperties::new();
		// Advertise the server keep-alive so clients adopt our ceiling.
		if self.limits.keep_alive > 0 {
			props.server_keep_alive = Some(self.limits.keep_alive);
		}
		conn_ack.properties = Some(props);
		let mut buf = BytesMut::new();
		conn_ack
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

		self.stream.write_all(&buf).await
	}

	async fn handle_disconnect(&mut self) -> Result<()> {
		info!("client sent disconnect");
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

	async fn handle_publish(&mut self, publish: mqtt_v5::Publish) -> Result<()> {
		// Payload contents are never logged — only topic, QoS, and byte length.
		debug!(
			topic = %publish.topic,
			qos = ?publish.qos,
			retain = publish.retain,
			payload = %redact::payload(&publish.payload),
			"publish received"
		);

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
				self.fan_out(msg);
				Ok(())
			}
			// At least once: deliver, then acknowledge.
			QoS::AtLeastOnce => {
				self.fan_out(msg);
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

		for filter in &subscribe.filters {
			let granted = min_qos(filter.qos, self.limits.max_qos());

			{
				let mut state = self.state.borrow_mut();
				state.subscribe(&filter.path, &self.client_id, granted);
				for message in state.retained_matching(&filter.path) {
					retained.push((message, granted));
				}
			}

			debug!(filter = %filter.path, granted = ?granted, "subscribed");

			return_codes.push(reason_code(granted));
		}

		let sub_ack = mqtt_v5::SubAck::new(subscribe.pkid, return_codes);
		let mut buf = BytesMut::new();
		sub_ack
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await?;

		// Replay matching retained messages, delivered with the retain flag set and
		// downgraded to min(message QoS, granted QoS) for this subscription.
		for (mut message, granted) in retained {
			message.retain = true;
			let qos = min_qos(message.qos, granted);
			self.send_publish(&message, qos).await?;
		}

		Ok(())
	}

	async fn handle_unsubscribe(&mut self, unsubscribe: mqtt_v5::Unsubscribe) -> Result<()> {
		let mut reasons = Vec::with_capacity(unsubscribe.filters.len());

		for filter in &unsubscribe.filters {
			self.state.borrow_mut().unsubscribe(filter, &self.client_id);
			debug!(filter = %filter, "unsubscribed");
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
		// transaction is complete; release the packet id.
		self.inflight.remove(&puback.pkid);
		Ok(())
	}

	async fn handle_pubrec(&mut self, pubrec: mqtt_v5::PubRec) -> Result<()> {
		// QoS 2, sender side (step 2): the client received our PUBLISH. Advance the
		// transaction to "released" and send PUBREL.
		if let Some(state) = self.inflight.get_mut(&pubrec.pkid) {
			if matches!(state, Inflight::Qos2Pending) {
				*state = Inflight::Qos2Released;
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
			self.fan_out(message);
		}

		let mut buf = BytesMut::new();
		mqtt_v5::PubComp::new(pubrel.pkid)
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	async fn handle_pubcomp(&mut self, pubcomp: mqtt_v5::PubComp) -> Result<()> {
		// QoS 2, sender side (step 4): the client finalized the transaction.
		// Release the packet id.
		self.inflight.remove(&pubcomp.pkid);
		Ok(())
	}
}

/// Returns the lower of two QoS levels (the granted QoS is `min(requested, max)`).
fn min_qos(a: QoS, b: QoS) -> QoS {
	if (a as u8) <= (b as u8) { a } else { b }
}

/// Maps a granted QoS to its SubAck success reason code.
fn reason_code(qos: QoS) -> mqtt_v5::SubscribeReasonCode {
	match qos {
		QoS::AtMostOnce => mqtt_v5::SubscribeReasonCode::QoS0,
		QoS::AtLeastOnce => mqtt_v5::SubscribeReasonCode::QoS1,
		QoS::ExactlyOnce => mqtt_v5::SubscribeReasonCode::QoS2,
	}
}
