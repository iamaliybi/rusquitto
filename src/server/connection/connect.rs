//! The CONNECT handshake: client-id assignment, authentication, session
//! open/resume (including cross-shard migration), and the CONNACK reply.

use futures_lite::FutureExt;
use glommio::channels::local_channel;
use mqttbytes::v5::{self as mqtt_v5};
use std::collections::VecDeque;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::{Connection, MAX_CLIENT_ID_LEN, NEXT_CLIENT_ID, SESSION_CLAIM_TIMEOUT};
use crate::auth::AuthResult;
use crate::broker::mesh::MigratedSession;
use crate::telemetry::logging::redact;
use crate::transport::ByteStream;

impl<S: ByteStream> Connection<S> {
	/// Replies to a rejected CONNECT with a failure CONNACK (session present is
	/// always false) and returns an error to unwind and close the connection.
	async fn reject_connect(&mut self, code: mqtt_v5::ConnectReturnCode) -> Result<()> {
		let mut conn_ack = mqtt_v5::ConnAck::new(code, false);
		// Attach empty properties so mqttbytes emits the mandatory v5 length byte.
		conn_ack.properties = Some(mqtt_v5::ConnAckProperties::new());
		self.send(|buf| conn_ack.write(buf)).await?;
		Err(Error::new(
			ErrorKind::PermissionDenied,
			"authentication failed",
		))
	}

	pub(super) async fn handle_connect(&mut self, connect: mqtt_v5::Connect) -> Result<()> {
		// Clean Start decides whether an existing session is resumed; the Session
		// Expiry Interval decides how long the session outlives a disconnect.
		let clean_start = connect.clean_session;
		let props = connect.properties.as_ref();
		self.session_expiry = props.and_then(|p| p.session_expiry_interval).unwrap_or(0);
		// Cap the session lifetime so a client can't pin broker memory indefinitely.
		if self.limits.max_session_expiry > 0 {
			self.session_expiry = self.session_expiry.min(self.limits.max_session_expiry);
		}

		// Client flow-control limits we must honour on the outbound path. Receive
		// Maximum bounds our unacked QoS 1/2 window (0 is invalid, so clamp to 1);
		// Maximum Packet Size caps the size of any packet we send it.
		self.peer_receive_max = props
			.and_then(|p| p.receive_maximum)
			.unwrap_or(u16::MAX)
			.max(1);
		self.peer_max_packet_size = props.and_then(|p| p.max_packet_size);

		// An empty client id has the server assign one, which must then be echoed
		// back in CONNACK so the client can reconnect to the same session. The
		// assigned id mixes a per-process random value with a counter so it is
		// unique and not guessable by other clients (which could otherwise force a
		// session takeover).
		let assigned = connect.client_id.is_empty();
		if assigned {
			let n = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
			let rand = RandomState::new().hash_one(n);
			self.client_id = format!("auto-{}-{}-{:016x}", self.shard_id, n, rand);
		} else {
			// Reject a hostile client id: bound its length and forbid NUL / control
			// characters (which could corrupt logs or downstream topic names).
			let id = &connect.client_id;
			if id.len() > MAX_CLIENT_ID_LEN || id.chars().any(|c| c.is_control()) {
				warn!(len = id.len(), "invalid client id, rejecting");
				return self
					.reject_connect(mqtt_v5::ConnectReturnCode::ClientIdentifierNotValid)
					.await;
			}
			self.client_id = connect.client_id;
		}

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
		let auth = self.auth.check(
			login.map(|l| l.username.as_str()),
			login.map(|l| l.password.as_str()),
		);
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
			let handle = self
				.state
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
		} else if !session_present && let Some(migrated) = self.claim_remote_session().await {
			info!("resumed session migrated from another shard");
			self.subscription_count = migrated.subscriptions.len();
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
		let mut conn_ack = mqtt_v5::ConnAck::new(mqtt_v5::ConnectReturnCode::Success, session_present);
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
		self.send(|buf| conn_ack.write(buf)).await?;

		// The handshake is complete: further packets are now expected, and the idle
		// deadline switches from the handshake timeout to keep-alive enforcement. The
		// effective keep-alive is the server override if set, else the client's value;
		// the broker drops the connection after 1.5× that with no traffic (MQTT §3.1.2.10).
		self.connected = true;
		let effective_ka = if self.limits.keep_alive > 0 {
			self.limits.keep_alive
		} else {
			connect.keep_alive
		};
		self.keepalive = (effective_ka > 0).then(|| Duration::from_millis(1500 * u64::from(effective_ka)));
		self.deadline = self.keepalive.map(|w| Instant::now() + w);

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
}
