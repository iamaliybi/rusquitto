//! Unit tests for the connection state machine.
//!
//! These drive a [`Connection`] over an in-memory [`MockStream`] — the payoff of
//! the [`ByteStream`] abstraction: the full MQTT logic is exercised with no
//! sockets. Being a child module, the tests reach the private `process_packet`
//! entry point directly and assert on both the emitted wire bytes and internal
//! state, without standing up the racing event loop.

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
	let stream = MockStream { inbound: VecDeque::new(), outbound: out };
	Connection::new(
		stream,
		0,
		ShardState::new(),
		LimitsConfig::default(),
		Rc::new(Authenticator::from_config(&AuthConfig::default())),
		Arc::new(Metrics::default()),
		Arc::new(AtomicBool::new(false)),
	)
}

/// A minimal clean-start CONNECT for client id `id`.
fn connect_packet(id: &str) -> Packet {
	let mut c = mqtt_v5::Connect::new(id);
	c.clean_session = true;
	Packet::Connect(c)
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

		conn.process_packet(connect_packet("c1")).await.unwrap();

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
		assert!(conn.process_packet(publish).await.is_err());
		assert!(!conn.connected);
	});
}

#[test]
fn ping_before_connect_is_rejected() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		assert!(conn.process_packet(Packet::PingReq).await.is_err());
	});
}

#[test]
fn second_connect_is_a_protocol_error() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		conn.process_packet(connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		// A second CONNECT after a successful one must be refused with DISCONNECT.
		let err = conn.process_packet(connect_packet("c1")).await;
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
		conn.process_packet(connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		conn.process_packet(Packet::PingReq).await.unwrap();
		assert!(matches!(decode(&out), Packet::PingResp));
	});
}

#[test]
fn reserved_publish_topic_triggers_disconnect() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		conn.process_packet(connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		// `$`-prefixed topics are broker-reserved; a client publish to one is invalid.
		let publish = Packet::Publish(Publish::new("$SYS/hack", QoS::AtMostOnce, b"x".to_vec()));
		assert!(conn.process_packet(publish).await.is_err());
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
		conn.process_packet(connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		let mut publish = Publish::new("a/b", QoS::AtLeastOnce, b"hi".to_vec());
		publish.pkid = 42;
		conn.process_packet(Packet::Publish(publish)).await.unwrap();

		match decode(&out) {
			Packet::PubAck(ack) => assert_eq!(ack.pkid, 42),
			other => panic!("expected PUBACK, got {other:?}"),
		}
	});
}

#[test]
fn subscribe_emits_suback_and_counts_subscription() {
	block_on(async {
		let out = Rc::new(RefCell::new(Vec::new()));
		let mut conn = make_conn(out.clone());
		conn.process_packet(connect_packet("c1")).await.unwrap();
		out.borrow_mut().clear();

		let mut sub = Subscribe::new("home/+/temp", QoS::AtLeastOnce);
		sub.pkid = 7;
		conn.process_packet(Packet::Subscribe(sub)).await.unwrap();

		assert_eq!(conn.subscription_count, 1);
		match decode(&out) {
			Packet::SubAck(ack) => assert_eq!(ack.pkid, 7),
			other => panic!("expected SUBACK, got {other:?}"),
		}
	});
}
