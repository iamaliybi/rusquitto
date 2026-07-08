//! Unit tests for the connection state machine.
//!
//! These drive a [`Connection`] over an in-memory [`MockStream`] — the payoff of
//! the [`ByteStream`] abstraction: the full MQTT logic is exercised with no
//! sockets. Being a child module, the tests reach the private `process_one`
//! entry point directly (via `drive`, which encodes the packet into the
//! assembly buffer like a socket would) and assert on both the emitted wire
//! bytes and internal state, without standing up the racing event loop.

use super::*;
use crate::auth::Authenticator;
use crate::config::AuthConfig;
use bytes::BytesMut;
use mqttbytes::QoS;
use mqttbytes::v5::{ConnectReturnCode, DisconnectReasonCode, Publish, Subscribe};
use std::collections::VecDeque;

/// In-memory `ByteStream`: replays queued inbound bytes and records every byte
/// the connection writes so tests can decode the responses.
struct MockStream {
	inbound: VecDeque<u8>,
	outbound: Rc<RefCell<Vec<u8>>>,
}

impl ByteStream for MockStream {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		let mut n = 0;
		while n < buf.len() {
			match self.inbound.pop_front() {
				Some(b) => {
					buf[n] = b;
					n += 1;
				}
				None => break,
			}
		}
		Ok(n) // 0 when drained — the event loop reads this as EOF.
	}

	async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
		self.outbound.borrow_mut().extend_from_slice(buf);
		Ok(())
	}
}

/// Runs a future to completion on a throwaway glommio executor (required for the
/// connection's local-channel mailbox).
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
	glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
		.make()
		.expect("build test executor")
		.run(fut)
}

/// A connection wired to a `MockStream`, sharing `out` so the test can read
/// whatever the connection writes. Anonymous auth is open by default.
fn make_conn(out: Rc<RefCell<Vec<u8>>>) -> Connection<MockStream> {
	make_conn_with(out, LimitsConfig::default())
}

/// `make_conn` with caller-chosen limits.
fn make_conn_with(out: Rc<RefCell<Vec<u8>>>, limits: LimitsConfig) -> Connection<MockStream> {
	let stream = MockStream { inbound: VecDeque::new(), outbound: out };
	Connection::new(
		stream,
		0,
		ShardState::new(),
		limits,
		Rc::new(Authenticator::from_config(&AuthConfig::default())),
		Arc::new(Metrics::default()),
		Arc::new(AtomicBool::new(false)),
		TlsIdentity::None,
	)
}

/// `make_conn` with a caller-chosen mutual-TLS identity (anonymous auth otherwise).
fn make_conn_tls(out: Rc<RefCell<Vec<u8>>>, tls_identity: TlsIdentity) -> Connection<MockStream> {
	let stream = MockStream { inbound: VecDeque::new(), outbound: out };
	Connection::new(
		stream,
		0,
		ShardState::new(),
		LimitsConfig::default(),
		Rc::new(Authenticator::from_config(&AuthConfig::default())),
		Arc::new(Metrics::default()),
		Arc::new(AtomicBool::new(false)),
		tls_identity,
	)
}

#[test]
fn cert_cn_becomes_username_when_no_login() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn_tls(out.clone(), TlsIdentity::Cn("device-7".into()));
		// A clean CONNECT with no login: the verified cert's CN is the identity.
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		assert!(conn.connected);
		assert_eq!(
			conn.username.as_deref(),
			Some("device-7"),
			"cert CN mapped to the MQTT username"
		);
	});
}

#[test]
fn verified_cert_without_mapping_has_no_username() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn_tls(out.clone(), TlsIdentity::Verified);
		// Cert-authenticated but CN mapping off: connected, but no MQTT identity.
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		assert!(conn.connected);
		assert_eq!(
			conn.username, None,
			"verified-only cert grants but carries no identity"
		);
	});
}

/// A minimal clean-start CONNECT for client id `id`.
fn connect_packet(id: &str) -> Packet {
	let mut c = mqtt_v5::Connect::new(id);
	c.clean_session = true;
	Packet::Connect(c)
}

/// Serializes any v5 packet to its wire bytes (the parse/dispatch seam is one
/// function now, so tests feed the assembly buffer exactly like a socket would).
fn encode_packet(packet: &Packet, buf: &mut BytesMut) {
	let r = match packet {
		Packet::Connect(p) => p.write(buf),
		Packet::ConnAck(p) => p.write(buf),
		Packet::Publish(p) => p.write(buf),
		Packet::PubAck(p) => p.write(buf),
		Packet::PubRec(p) => p.write(buf),
		Packet::PubRel(p) => p.write(buf),
		Packet::PubComp(p) => p.write(buf),
		Packet::Subscribe(p) => p.write(buf),
		Packet::SubAck(p) => p.write(buf),
		Packet::Unsubscribe(p) => p.write(buf),
		Packet::UnsubAck(p) => p.write(buf),
		Packet::PingReq => mqtt_v5::PingReq.write(buf),
		Packet::PingResp => mqtt_v5::PingResp.write(buf),
		Packet::Disconnect(p) => p.write(buf),
	};
	r.expect("encode test packet");
}

/// Feeds one packet through the parse-and-process seam and flushes the
/// coalesced output buffer, exactly as one event-loop wakeup would, so tests
/// can assert on the emitted wire bytes. Flushes even when processing errors —
/// mirroring the connection's best-effort flush on its exit path — so reject
/// responses reach the mock stream too.
async fn drive(conn: &mut Connection<MockStream>, packet: Packet) -> Result<()> {
	let max_packet = conn.limits.max_payload_size;
	encode_packet(&packet, &mut conn.inbound);
	let result = conn.process_one(max_packet).await;
	conn.flush().await.expect("flush mock stream");
	result.map(|processed| assert!(processed, "test packet must parse completely"))
}

/// Decodes the single MQTT packet currently sitting in `out`.
fn decode(out: &Rc<RefCell<Vec<u8>>>) -> Packet {
	let mut buf = BytesMut::from(&out.borrow()[..]);
	mqtt_v5::read(&mut buf, 1 << 20).expect("decode a complete packet")
}

/// The reason code of a server-sent DISCONNECT, read straight from the wire.
///
/// mqttbytes' `read` can't parse the minimal `E0 01 <reason>` form the broker
/// emits (it wants a property-length byte, the same asymmetry `read_packet`
/// works around), so the bytes are inspected directly: `[0xE0, len, reason, …]`.
fn disconnect_reason(out: &Rc<RefCell<Vec<u8>>>) -> u8 {
	let bytes = out.borrow();
	assert_eq!(bytes[0] >> 4, 14, "packet type is DISCONNECT");
	bytes[2]
}

#[test]
fn connect_handshake_emits_success_connack() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());

		drive(&mut conn, connect_packet("c1")).await.unwrap();

		assert!(conn.connected, "connection marked connected after CONNECT");
		match decode(&out) {
			Packet::ConnAck(ack) => assert_eq!(ack.code, ConnectReturnCode::Success),
			other => panic!("expected CONNACK, got {other:?}"),
		}
	});
}

#[test]
fn first_packet_must_be_connect() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());

		// A PUBLISH before CONNECT is a protocol violation — the pre-auth bypass guard.
		let publish = Packet::Publish(Publish::new("a/b", QoS::AtMostOnce, b"x".to_vec()));
		assert!(drive(&mut conn, publish).await.is_err());
		assert!(!conn.connected);
	});
}

#[test]
fn ping_before_connect_is_rejected() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		assert!(drive(&mut conn, Packet::PingReq).await.is_err());
	});
}

#[test]
fn second_connect_is_a_protocol_error() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		// A second CONNECT after a successful one must be refused with DISCONNECT.
		let err = drive(&mut conn, connect_packet("c1")).await;
		assert!(err.is_err());
		assert_eq!(
			disconnect_reason(&out),
			DisconnectReasonCode::ProtocolError as u8
		);
	});
}

#[test]
fn ping_after_connect_emits_pingresp() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		drive(&mut conn, Packet::PingReq).await.unwrap();
		assert!(matches!(decode(&out), Packet::PingResp));
	});
}

#[test]
fn reserved_publish_topic_triggers_disconnect() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		// `$`-prefixed topics are broker-reserved; a client publish to one is invalid.
		let publish = Packet::Publish(Publish::new("$SYS/hack", QoS::AtMostOnce, b"x".to_vec()));
		assert!(drive(&mut conn, publish).await.is_err());
		assert_eq!(
			disconnect_reason(&out),
			DisconnectReasonCode::TopicNameInvalid as u8
		);
	});
}

#[test]
fn publish_qos1_is_acknowledged() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		let mut publish = Publish::new("a/b", QoS::AtLeastOnce, b"hi".to_vec());
		publish.pkid = 42;
		drive(&mut conn, Packet::Publish(publish)).await.unwrap();

		match decode(&out) {
			Packet::PubAck(ack) => assert_eq!(ack.pkid, 42),
			other => panic!("expected PUBACK, got {other:?}"),
		}
	});
}

#[test]
fn rate_limited_publish_still_delivers_within_burst() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		// A generous rate: the first publish is within the burst, so no throttle sleep.
		let limits = LimitsConfig { max_message_rate: 1000, ..LimitsConfig::default() };
		let mut conn = make_conn_with(out.clone(), limits);
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		let mut publish = Publish::new("a/b", QoS::AtLeastOnce, b"hi".to_vec());
		publish.pkid = 9;
		drive(&mut conn, Packet::Publish(publish)).await.unwrap();

		match decode(&out) {
			Packet::PubAck(ack) => assert_eq!(ack.pkid, 9),
			other => panic!("expected PUBACK, got {other:?}"),
		}
	});
}

#[test]
fn subscribe_emits_suback_and_counts_subscription() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		let mut sub = Subscribe::new("home/+/temp", QoS::AtLeastOnce);
		sub.pkid = 7;
		drive(&mut conn, Packet::Subscribe(sub)).await.unwrap();

		assert_eq!(conn.subscription_count, 1);
		match decode(&out) {
			Packet::SubAck(ack) => assert_eq!(ack.pkid, 7),
			other => panic!("expected SUBACK, got {other:?}"),
		}
	});
}

/// Decodes every complete MQTT packet sitting in `out`, in order.
fn decode_all(out: &Rc<RefCell<Vec<u8>>>) -> Vec<Packet> {
	let mut buf = BytesMut::from(&out.borrow()[..]);
	let mut packets = Vec::new();
	while !buf.is_empty() {
		packets.push(mqtt_v5::read(&mut buf, 1 << 20).expect("decode packet"));
	}
	packets
}

/// A CONNECT advertising a Topic Alias Maximum, so the broker may assign
/// outbound aliases on the publishes it sends this client.
fn connect_with_alias_max(id: &str, alias_max: u16) -> Packet {
	let mut c = mqtt_v5::Connect::new(id);
	c.clean_session = true;
	c.properties = Some(mqtt_v5::ConnectProperties {
		session_expiry_interval: None,
		receive_maximum: None,
		max_packet_size: None,
		topic_alias_max: Some(alias_max),
		request_response_info: None,
		request_problem_info: None,
		user_properties: Vec::new(),
		authentication_method: None,
		authentication_data: None,
	});
	Packet::Connect(c)
}

/// A QoS 0 delivery for `topic`, as the routing layer would hand it over.
fn qos0_delivery(topic: &str) -> Delivery {
	Delivery {
		publish: Rc::new(Publish::new(topic, QoS::AtMostOnce, b"x".to_vec())),
		qos: QoS::AtMostOnce,
		retain: false,
		sub_ids: Vec::new(),
	}
}

#[test]
fn outbound_topic_alias_registers_then_substitutes() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_with_alias_max("c1", 4))
			.await
			.unwrap();
		out.borrow_mut().clear();

		// Same topic twice, then a second topic.
		conn.deliver(qos0_delivery("sensors/temp")).unwrap();
		conn.deliver(qos0_delivery("sensors/temp")).unwrap();
		conn.deliver(qos0_delivery("sensors/hum")).unwrap();
		conn.flush().await.unwrap();

		let packets = decode_all(&out);
		assert_eq!(packets.len(), 3);
		let publish = |p: &Packet| match p {
			Packet::Publish(p) => p.clone(),
			other => panic!("expected PUBLISH, got {other:?}"),
		};

		// First use registers alias 1 alongside the full topic.
		let p1 = publish(&packets[0]);
		assert_eq!(p1.topic, "sensors/temp");
		assert_eq!(p1.properties.unwrap().topic_alias, Some(1));
		// Repeat carries only the alias — the topic name is gone from the wire.
		let p2 = publish(&packets[1]);
		assert_eq!(p2.topic, "");
		assert_eq!(p2.properties.unwrap().topic_alias, Some(1));
		// A different topic gets the next alias.
		let p3 = publish(&packets[2]);
		assert_eq!(p3.topic, "sensors/hum");
		assert_eq!(p3.properties.unwrap().topic_alias, Some(2));
	});
}

#[test]
fn outbound_alias_table_full_falls_back_to_full_topic() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_with_alias_max("c1", 1))
			.await
			.unwrap();
		out.borrow_mut().clear();

		conn.deliver(qos0_delivery("a")).unwrap(); // takes the only alias
		conn.deliver(qos0_delivery("b")).unwrap(); // table full: unaliased
		conn.flush().await.unwrap();

		let packets = decode_all(&out);
		let p2 = match &packets[1] {
			Packet::Publish(p) => p.clone(),
			other => panic!("expected PUBLISH, got {other:?}"),
		};
		assert_eq!(p2.topic, "b");
		assert_eq!(p2.properties.and_then(|p| p.topic_alias), None);
	});
}

#[test]
fn no_outbound_alias_when_client_does_not_offer() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_packet("c1")).await.unwrap(); // no alias max
		out.borrow_mut().clear();

		conn.deliver(qos0_delivery("t")).unwrap();
		conn.deliver(qos0_delivery("t")).unwrap();
		conn.flush().await.unwrap();

		for p in decode_all(&out) {
			let p = match p {
				Packet::Publish(p) => p,
				other => panic!("expected PUBLISH, got {other:?}"),
			};
			assert_eq!(p.topic, "t", "full topic on every send");
			assert_eq!(p.properties.and_then(|p| p.topic_alias), None);
		}
	});
}

#[test]
fn no_local_on_shared_subscription_is_rejected() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		drive(&mut conn, connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		// No Local on a Shared Subscription is a Protocol Error (MQTT 5 §3.8.3.1).
		let mut sub = Subscribe::new("$share/g/data/#", QoS::AtLeastOnce);
		sub.filters[0].nolocal = true;
		sub.pkid = 3;
		drive(&mut conn, Packet::Subscribe(sub)).await.unwrap();

		assert_eq!(conn.subscription_count, 0, "filter must not be armed");
		match decode(&out) {
			Packet::SubAck(ack) => {
				assert_eq!(
					ack.return_codes,
					vec![mqtt_v5::SubscribeReasonCode::TopicFilterInvalid]
				);
			}
			other => panic!("expected SUBACK, got {other:?}"),
		}
	});
}

/// Diagnostic, not a regression test (hence ignored): prints the size of every
/// future in the connection state machine — the numbers behind the memory work
/// in 1.6.x. Run with `cargo test probe_future_tree -- --ignored --nocapture`.
///
/// History: pre-diet, run() was ≈ 3.3 KiB via process_packet (2.4 KiB) →
/// handle_publish (1.6 KiB) → fan_out (1.2 KiB), holding several
/// `Publish`-sized (208 B) slots. Source-level slot elimination (in-place
/// transforms, by-ref passing) did NOT shrink the machine — rustc allocates
/// await-spanning slots conservatively. What worked was *boxing through
/// plain-fn seams*: the cold mesh-backpressure send, the throttle sleep, and
/// the PUBLISH/PUBREL/CONNECT handler arms — bringing run() to ≈ 0.6 KiB.
/// Watch these numbers when touching the event loop or handlers.
#[test]
#[ignore]
fn probe_future_tree() {
	use std::mem::{size_of, size_of_val};
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out);
		println!("Packet enum:            {}", size_of::<Packet>());
		println!("Publish struct:         {}", size_of::<Publish>());
		println!(
			"Connection<MockStream>: {}",
			size_of::<Connection<MockStream>>()
		);
		let f = conn.run();
		println!("run():                  {}", size_of_val(&f));
		drop(f);
		let f = conn.event_loop();
		println!("event_loop():           {}", size_of_val(&f));
		drop(f);
		let f = conn.process_one(1 << 20);
		println!("process_one():          {}", size_of_val(&f));
		drop(f);
		let publish = Publish::new("t", QoS::AtLeastOnce, b"x".to_vec());
		let f = conn.handle_publish(publish.clone());
		println!("handle_publish():       {}", size_of_val(&f));
		drop(f);
		let f = conn.fan_out(publish, None);
		println!("fan_out():              {}", size_of_val(&f));
		drop(f);
		let sub = Subscribe::new("a/b", QoS::AtLeastOnce);
		let f = conn.handle_subscribe(sub);
		println!("handle_subscribe():     {}", size_of_val(&f));
		drop(f);
		let f = conn.resume_delivery(VecDeque::new());
		println!("resume_delivery():      {}", size_of_val(&f));
		drop(f);
		let f = conn.flush();
		println!("flush():                {}", size_of_val(&f));
	});
}

// --- partial-frame stall guard (the 15th adversarial case) -------------------

/// A `ByteStream` that replays its queued inbound bytes once, then *parks
/// forever* on the next read instead of returning EOF — a live socket that has
/// gone silent mid-frame. This lets a test observe the event loop's own idle /
/// framing deadline fire (a plain EOF would close the connection immediately and
/// prove nothing about the timeout).
struct StallStream {
	inbound: VecDeque<u8>,
	outbound: Rc<RefCell<Vec<u8>>>,
}

impl ByteStream for StallStream {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		if self.inbound.is_empty() {
			// No more bytes will ever arrive, and we do NOT signal EOF: park so the
			// read can never win the race against the deadline.
			std::future::pending::<()>().await;
		}
		let mut n = 0;
		while n < buf.len() {
			match self.inbound.pop_front() {
				Some(b) => {
					buf[n] = b;
					n += 1;
				}
				None => break,
			}
		}
		Ok(n)
	}

	async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
		self.outbound.borrow_mut().extend_from_slice(buf);
		Ok(())
	}
}

fn stall_conn(out: Rc<RefCell<Vec<u8>>>, inbound: VecDeque<u8>, limits: LimitsConfig) -> Connection<StallStream> {
	let stream = StallStream { inbound, outbound: out };
	Connection::new(
		stream,
		0,
		ShardState::new(),
		limits,
		Rc::new(Authenticator::from_config(&AuthConfig::default())),
		Arc::new(Metrics::default()),
		Arc::new(AtomicBool::new(false)),
		TlsIdentity::None,
	)
}

/// Runs the connection's event loop against a stalling stream and asserts it
/// returns within the framing window; a generous watchdog turns a regression
/// (the loop hanging on a never-completing read) into a fast test failure.
async fn run_until_reaped(conn: &mut Connection<StallStream>) {
	use futures_lite::FutureExt;
	let watchdog = async {
		glommio::timer::sleep(std::time::Duration::from_secs(5)).await;
		panic!("event loop did not reap the stalled connection within the framing window");
	};
	conn.event_loop()
		.or(watchdog)
		.await
		.expect("event loop exits cleanly on timeout");
}

#[test]
fn truncated_connect_header_is_reaped_by_the_handshake_timeout() {
	// The 15th adversarial case: a complete CONNECT fixed header claiming 10 body
	// bytes, then silence. Bounded by `connect_timeout`, even pre-CONNECT.
	block_on(async {
		let limits = LimitsConfig { connect_timeout: 1, ..LimitsConfig::default() };
		let inbound = VecDeque::from(vec![0x10, 0x0A]); // CONNECT, remaining length 10, no body
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = stall_conn(out, inbound, limits);

		run_until_reaped(&mut conn).await;
		assert!(!conn.connected, "never completed the CONNECT");
	});
}

// --- parking ------------------------------------------------------------------

/// The park predicate must hold only when the connection is *fully* idle: every
/// disqualifier — pre-CONNECT, buffered inbound bytes, a stalled partial frame,
/// in-flight QoS state in either direction, window-held messages, unflushed
/// output — individually blocks it.
#[test]
fn park_ready_only_when_fully_idle() {
	use crate::broker::session::{InflightMessage, InflightState};
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		assert!(!conn.park_ready(), "never before CONNECT");

		drive(&mut conn, connect_packet("c1")).await.unwrap();
		assert!(conn.park_ready(), "connected and idle");

		conn.inbound.extend_from_slice(&[0x30]);
		assert!(!conn.park_ready(), "buffered inbound bytes");
		conn.inbound.clear();

		conn.partial_since = Some(Instant::now());
		assert!(!conn.park_ready(), "stalled partial frame");
		conn.partial_since = None;

		conn.inflight.insert(
			1,
			InflightMessage {
				publish: Publish::new("t", QoS::AtLeastOnce, b"x".to_vec()),
				state: InflightState::Qos1,
			},
		);
		assert!(!conn.park_ready(), "outbound QoS in flight");
		conn.inflight.clear();

		conn.incoming_qos2
			.insert(1, Publish::new("t", QoS::ExactlyOnce, b"x".to_vec()));
		assert!(!conn.park_ready(), "inbound QoS 2 uncommitted");
		conn.incoming_qos2.clear();

		conn.pending_outbound.push_back(qos0_delivery("t"));
		assert!(!conn.park_ready(), "window-held messages");
		conn.pending_outbound.clear();

		conn.outbound.extend_from_slice(b"x");
		assert!(!conn.park_ready(), "unflushed output");
		conn.outbound.clear();

		assert!(conn.park_ready(), "idle again once everything drained");
	});
}

/// Feeds CONNECT (+ optionally SUBSCRIBE) wire bytes to a parkable connection
/// over a stalling stream and returns it just before its event loop runs.
fn parkable_conn(out: Rc<RefCell<Vec<u8>>>, subscribe: Option<&str>) -> Connection<StallStream> {
	let mut buf = BytesMut::new();
	let mut connect = mqtt_v5::Connect::new("parker");
	connect.clean_session = true;
	connect.write(&mut buf).expect("encode CONNECT");
	if let Some(filter) = subscribe {
		let mut sub = Subscribe::new(filter, QoS::AtLeastOnce);
		sub.pkid = 1;
		sub.write(&mut buf).expect("encode SUBSCRIBE");
	}
	let mut conn = stall_conn(out, VecDeque::from(buf.to_vec()), LimitsConfig::default());
	conn.set_parkable(std::time::Duration::from_millis(20));
	conn
}

/// Races an event loop against a generous watchdog so a hang is a fast failure.
async fn flow_or_watchdog(conn: &mut Connection<StallStream>) -> Flow {
	use futures_lite::FutureExt;
	let watchdog = async {
		glommio::timer::sleep(std::time::Duration::from_secs(5)).await;
		panic!("event loop neither parked nor closed within the watchdog window");
	};
	conn.event_loop()
		.or(watchdog)
		.await
		.expect("event loop exits cleanly")
}

#[test]
fn fully_idle_connection_parks_after_the_grace() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = parkable_conn(out.clone(), Some("t"));

		let flow = flow_or_watchdog(&mut conn).await;
		assert_eq!(flow, Flow::Park, "idle past the grace ⇒ park, not close");
		assert!(conn.park_ready(), "handed over fully idle");

		// The handshake completed normally on the way: CONNACK then SUBACK.
		let packets = decode_all(&out);
		assert!(matches!(packets[0], Packet::ConnAck(_)));
		assert!(matches!(packets[1], Packet::SubAck(_)));
	});
}

/// The full park → deliver-while-parked → resume → replay → re-park cycle, at
/// the unit level: exactly what the serve path and the parking task perform,
/// minus the fd and the ring.
#[test]
fn parked_connection_resumes_replays_and_reparks() {
	use crate::broker::session::SessionSnapshot;
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = parkable_conn(out.clone(), Some("t"));
		assert_eq!(flow_or_watchdog(&mut conn).await, Flow::Park);

		// The serve path's synchronous transition: destructure, flip the session.
		let shard = conn.shard.clone();
		let (stream, state) = conn.into_parts();
		assert!(shard.borrow_mut().park_session(
			state.client_id(),
			state.generation(),
			SessionSnapshot { next_pkid: state.next_pkid, ..Default::default() },
		));

		// A message routed while parked queues on the session (and would Wake).
		shard.borrow_mut().deliver_local(
			Publish::new("t", QoS::AtMostOnce, b"while-parked".to_vec()),
			None,
		);

		// Resurrect around the same stream and run: reattach, replay, re-park.
		out.borrow_mut().clear();
		let mut conn = Connection::resume(
			stream,
			*state,
			0,
			shard,
			LimitsConfig::default(),
			Rc::new(Authenticator::from_config(&AuthConfig::default())),
			Arc::new(Metrics::default()),
			Arc::new(AtomicBool::new(false)),
			std::time::Duration::from_millis(20),
		);
		let flow = {
			use futures_lite::FutureExt;
			let watchdog = async {
				glommio::timer::sleep(std::time::Duration::from_secs(5)).await;
				panic!("resumed connection neither re-parked nor closed");
			};
			conn.run()
				.or(watchdog)
				.await
				.expect("resumed run exits cleanly")
		};
		assert_eq!(flow, Flow::Park, "drained the backlog and re-parked");

		// The queued message reached the wire during the replay.
		let packets = decode_all(&out);
		match &packets[0] {
			Packet::Publish(p) => {
				assert_eq!(p.topic, "t");
				assert_eq!(&p.payload[..], b"while-parked");
			}
			other => panic!("expected the replayed PUBLISH, got {other:?}"),
		}
	});
}

#[test]
fn resume_after_takeover_closes_quietly() {
	use crate::broker::session::SessionSnapshot;
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = parkable_conn(out.clone(), None);
		assert_eq!(flow_or_watchdog(&mut conn).await, Flow::Park);

		let shard = conn.shard.clone();
		let (stream, state) = conn.into_parts();
		assert!(shard.borrow_mut().park_session(
			state.client_id(),
			state.generation(),
			SessionSnapshot { next_pkid: state.next_pkid, ..Default::default() },
		));

		// A new connection takes the client id over before the resume lands.
		let (tx2, _rx2) = local_channel::new_unbounded();
		shard.borrow_mut().open_session("parker", tx2, false);

		out.borrow_mut().clear();
		let mut conn = Connection::resume(
			stream,
			*state,
			0,
			shard,
			LimitsConfig::default(),
			Rc::new(Authenticator::from_config(&AuthConfig::default())),
			Arc::new(Metrics::default()),
			Arc::new(AtomicBool::new(false)),
			std::time::Duration::from_millis(20),
		);
		assert_eq!(
			conn.run().await.expect("displaced resume exits cleanly"),
			Flow::Closed,
			"stale generation ⇒ displaced-connection semantics"
		);
		assert!(
			out.borrow().is_empty(),
			"quiet close: no DISCONNECT, no will, nothing on the wire"
		);
	});
}

#[test]
fn stalled_partial_frame_is_reaped_even_without_keepalive() {
	// The dangerous post-CONNECT sibling: keep-alive disabled on BOTH ends (so the
	// idle deadline is `None`), a completed CONNECT, then a partial PUBLISH header
	// that never finishes. Only the partial-frame guard can close this.
	block_on(async {
		let limits = LimitsConfig { keep_alive: 0, connect_timeout: 1, ..LimitsConfig::default() };

		let mut buf = BytesMut::new();
		let mut connect = mqtt_v5::Connect::new("stall");
		connect.clean_session = true;
		connect.keep_alive = 0;
		connect.write(&mut buf).expect("encode CONNECT");
		buf.extend_from_slice(&[0x30, 0x0A]); // PUBLISH, remaining length 10, no body
		let inbound = VecDeque::from(buf.to_vec());

		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = stall_conn(out, inbound, limits);

		run_until_reaped(&mut conn).await;
		assert!(
			conn.connected,
			"CONNECT completed before the mid-frame stall"
		);
	});
}

// --- property-based fuzzing --------------------------------------------------
//
// Closes the audit's standing gap: the malformed-frame surface was only covered
// by the hand-curated adversarial battery. These proptest cases generate
// adversarial byte streams — pure random, single plausible-but-malformed
// packets, and concatenations of them — and drive them through the real
// parse-and-dispatch seam, asserting the broker never panics and every parse
// loop terminates. Runs in `cargo test`, so the parser is continuously fuzzed in
// CI rather than only spot-checked.
mod fuzz {
	use super::*;
	use proptest::prelude::*;

	/// One byte sequence shaped like an MQTT packet: a type+flags header byte, a
	/// remaining-length varint of the body, then a random body. Hits deeper
	/// handler code than pure noise, which rarely forms a valid fixed header.
	fn packetish() -> impl Strategy<Value = Vec<u8>> {
		(
			0u8..16,
			0u8..16,
			proptest::collection::vec(any::<u8>(), 0..300),
		)
			.prop_map(|(t, f, body)| {
				let mut v = vec![(t << 4) | f];
				let mut n = body.len();
				loop {
					let mut b = (n % 128) as u8;
					n /= 128;
					if n > 0 {
						b |= 0x80;
					}
					v.push(b);
					if n == 0 {
						break;
					}
				}
				v.extend_from_slice(&body);
				v
			})
	}

	/// The adversarial input distribution: pure noise, one plausible packet, or a
	/// stream of several concatenated (framing-boundary fuzzing).
	fn byte_soup() -> impl Strategy<Value = Vec<u8>> {
		prop_oneof![
			proptest::collection::vec(any::<u8>(), 0..512),
			packetish(),
			proptest::collection::vec(packetish(), 0..6).prop_map(|v| v.concat()),
		]
	}

	proptest! {
		#![proptest_config(ProptestConfig::with_cases(3000))]

		/// The frame parser must never panic on any input, and every `Ok(Some)`
		/// must consume bytes so a bounded drain always terminates.
		#[test]
		fn parse_packet_never_panics(data in byte_soup()) {
			let mut buf = BytesMut::from(&data[..]);
			let max = 64 * 1024;
			let mut guard = 0usize;
			loop {
				guard += 1;
				prop_assert!(guard < 100_000, "parse loop must terminate");
				match Connection::<MockStream>::parse_packet(&mut buf, max) {
					Ok(Some(_)) => continue,
					Ok(None) | Err(_) => break,
				}
			}
		}
	}

	proptest! {
		#![proptest_config(ProptestConfig::with_cases(256))]

		/// A fully-connected connection fed arbitrary bytes must never panic:
		/// every handler (publish, subscribe, the QoS ack flows, ping, …) is
		/// exercised through the real dispatch seam, and the drain must terminate.
		#[test]
		fn connected_dispatch_never_panics(data in byte_soup()) {
			block_on(async {
				let out = Rc::new(RefCell::new(Vec::new()));
				let mut conn = make_conn(out);
				drive(&mut conn, connect_packet("fuzz")).await.ok();
				conn.inbound.extend_from_slice(&data);
				let max = conn.limits.max_payload_size;
				let mut guard = 0usize;
				loop {
					guard += 1;
					if guard > 100_000 {
						break;
					}
					match conn.process_one(max).await {
						Ok(true) => continue,
						Ok(false) | Err(_) => break,
					}
				}
			});
		}

		/// Pre-CONNECT: an arbitrary first packet must be rejected cleanly by the
		/// CONNECT-first guard, never panic.
		#[test]
		fn preconnect_dispatch_never_panics(data in byte_soup()) {
			block_on(async {
				let out = Rc::new(RefCell::new(Vec::new()));
				let mut conn = make_conn(out);
				conn.inbound.extend_from_slice(&data);
				let max = conn.limits.max_payload_size;
				let _ = conn.process_one(max).await;
			});
		}
	}
}
