//! End-to-end integration tests.
//!
//! Unlike the unit tests (which drive the connection state machine over an
//! in-memory `MockStream`), these boot a **real broker in-process** and talk to
//! it over real TCP sockets with a minimal MQTT 5 client built on `mqttbytes`.
//! They exercise the whole stack — accept loop, transport, connection engine,
//! routing, sessions, auth, and the cross-shard mesh — the way a client does.
//!
//! Brokers are started lazily and shared per configuration (a `OnceLock` guards
//! each), so the suite spins up only a handful of glommio executor pools no matter
//! how many tests run, and tests keep out of each other's way by using unique
//! client ids and topics.
//!
//! This is a client-side test harness, not broker code, so the crate-wide
//! thread-per-core lints (which forbid `std::thread`) don't apply — it drives the
//! broker from ordinary OS threads, exactly as an external client would.
#![allow(clippy::disallowed_methods)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use mqttbytes::v5::{self as v5, Packet};
use mqttbytes::{Error as MqttError, QoS};
use rusquitto::config::{AuthConfig, Config, Placement, UserConfig};

// --- broker harness ----------------------------------------------------------

fn free_port() -> u16 {
	// Bind :0, read the assigned port, drop the listener. A tiny race window, but
	// fine for a test harness on loopback.
	std::net::TcpListener::bind("127.0.0.1:0")
		.unwrap()
		.local_addr()
		.unwrap()
		.port()
}

fn base_config(port: u16, cores: usize) -> Config {
	let mut c = Config::default();
	c.server.bind = "127.0.0.1".parse().unwrap();
	c.server.port = port;
	c.server.websocket = false;
	c.runtime.cores = Some(cores);
	c.runtime.placement = Placement::Unbound; // don't fight over pinned cores across test brokers
	c.logging.level = "error".into();
	c.logging.enable_terminal = false;
	c.logging.dir = std::env::temp_dir().join(format!("rusq-it-{port}"));
	c
}

/// Boots a broker on its configured port in a background thread and blocks until
/// it is accepting connections. The broker runs for the lifetime of the test
/// process (there is no in-process shutdown), which is exactly what a shared,
/// lazily-started fixture wants.
fn start(cfg: Config) -> u16 {
	let port = cfg.server.port;
	std::thread::spawn(move || {
		let _ = rusquitto::run(cfg);
	});
	let deadline = Instant::now() + Duration::from_secs(10);
	while Instant::now() < deadline {
		if TcpStream::connect(("127.0.0.1", port)).is_ok() {
			std::thread::sleep(Duration::from_millis(100)); // let the accept loop settle
			return port;
		}
		std::thread::sleep(Duration::from_millis(25));
	}
	panic!("broker on port {port} did not start accepting");
}

/// Anonymous, single-shard, no persistence — the default fixture for pub/sub,
/// retained, will, wildcard, unsubscribe, and malformed-frame tests.
fn default_broker() -> u16 {
	static PORT: OnceLock<u16> = OnceLock::new();
	*PORT.get_or_init(|| start(base_config(free_port(), 1)))
}

/// Authenticated fixture: `allow_anonymous = false`, one user `alice` with
/// publish/subscribe ACLs limited to `allowed/#`.
fn auth_broker() -> u16 {
	static PORT: OnceLock<u16> = OnceLock::new();
	*PORT.get_or_init(|| {
		let mut cfg = base_config(free_port(), 1);
		cfg.auth = AuthConfig {
			allow_anonymous: false,
			users: vec![UserConfig {
				username: "alice".into(),
				password: Some("s3cret".into()),
				password_hash: None,
				publish: Some(vec!["allowed/#".into()]),
				subscribe: Some(vec!["allowed/#".into()]),
			}],
			anonymous_publish: None,
			anonymous_subscribe: None,
		};
		start(cfg)
	})
}

/// Three-shard fixture for cross-shard routing and shared subscriptions.
fn multishard_broker() -> u16 {
	static PORT: OnceLock<u16> = OnceLock::new();
	*PORT.get_or_init(|| start(base_config(free_port(), 3)))
}

// --- minimal MQTT 5 client ---------------------------------------------------

#[derive(Debug)]
struct Client {
	sock: TcpStream,
	buf: BytesMut,
	pkid: u16,
}

fn session_props(expiry: u32) -> v5::ConnectProperties {
	v5::ConnectProperties {
		session_expiry_interval: Some(expiry),
		receive_maximum: None,
		max_packet_size: None,
		topic_alias_max: None,
		request_response_info: None,
		request_problem_info: None,
		user_properties: Vec::new(),
		authentication_method: None,
		authentication_data: None,
	}
}

impl Client {
	fn connect(port: u16, id: &str) -> Client {
		Self::try_connect(port, id, true, None, None).expect("CONNACK success")
	}

	/// Full-control connect; returns `Err(reason)` if the CONNACK is a failure.
	fn try_connect(
		port: u16,
		id: &str,
		clean_start: bool,
		login: Option<(&str, &str)>,
		session_expiry: Option<u32>,
	) -> Result<Client, v5::ConnectReturnCode> {
		let sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
		sock.set_nodelay(true).ok();
		let mut c = v5::Connect::new(id);
		c.clean_session = clean_start;
		c.keep_alive = 30;
		if let Some((u, p)) = login {
			c.login = Some(v5::Login::new(u, p));
		}
		if let Some(se) = session_expiry {
			c.properties = Some(session_props(se));
		}
		let mut client = Client { sock, buf: BytesMut::new(), pkid: 0 };
		client.write_packet(|b| c.write(b));
		match client.read(Duration::from_secs(3)) {
			Some(Packet::ConnAck(ack)) if ack.code == v5::ConnectReturnCode::Success => Ok(client),
			Some(Packet::ConnAck(ack)) => Err(ack.code),
			other => panic!("expected CONNACK, got {other:?}"),
		}
	}

	fn write_packet(&mut self, encode: impl FnOnce(&mut BytesMut) -> Result<usize, MqttError>) {
		let mut b = BytesMut::new();
		encode(&mut b).expect("encode packet");
		self.sock.write_all(&b).expect("socket write");
	}

	fn next_pkid(&mut self) -> u16 {
		self.pkid = self.pkid.wrapping_add(1).max(1);
		self.pkid
	}

	fn subscribe(&mut self, filter: &str, qos: QoS) {
		let mut sub = v5::Subscribe::new(filter, qos);
		sub.pkid = self.next_pkid();
		self.write_packet(|b| sub.write(b));
		match self.read(Duration::from_secs(2)) {
			Some(Packet::SubAck(_)) => {}
			other => panic!("expected SUBACK, got {other:?}"),
		}
	}

	fn unsubscribe(&mut self, filter: &str) {
		let mut un = v5::Unsubscribe::new(filter);
		un.pkid = self.next_pkid();
		self.write_packet(|b| un.write(b));
		match self.read(Duration::from_secs(2)) {
			Some(Packet::UnsubAck(_)) => {}
			other => panic!("expected UNSUBACK, got {other:?}"),
		}
	}

	/// Publishes and completes the QoS handshake (PUBACK for 1, PUBREC→PUBREL→
	/// PUBCOMP for 2). Returns `false` if the broker rejected/aborted the flow.
	fn publish(&mut self, topic: &str, payload: &[u8], qos: QoS) -> bool {
		self.publish_opts(topic, payload, qos, false)
	}

	fn publish_retain(&mut self, topic: &str, payload: &[u8], qos: QoS) -> bool {
		self.publish_opts(topic, payload, qos, true)
	}

	fn publish_opts(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) -> bool {
		let mut p = v5::Publish::new(topic, qos, payload.to_vec());
		p.retain = retain;
		let pkid = if qos == QoS::AtMostOnce {
			0
		} else {
			self.next_pkid()
		};
		p.pkid = pkid;
		self.write_packet(|b| p.write(b));
		match qos {
			QoS::AtMostOnce => true,
			QoS::AtLeastOnce => matches!(self.read(Duration::from_secs(2)), Some(Packet::PubAck(a)) if a.pkid == pkid),
			QoS::ExactlyOnce => {
				if !matches!(self.read(Duration::from_secs(2)), Some(Packet::PubRec(r)) if r.pkid == pkid) {
					return false;
				}
				self.write_packet(|b| v5::PubRel::new(pkid).write(b));
				matches!(self.read(Duration::from_secs(2)), Some(Packet::PubComp(c)) if c.pkid == pkid)
			}
		}
	}

	/// Reads the next delivered PUBLISH (completing the receiver-side QoS
	/// handshake so the broker's window doesn't stall), or `None` on timeout.
	fn recv(&mut self, timeout: Duration) -> Option<v5::Publish> {
		match self.read(timeout)? {
			Packet::Publish(p) => {
				match p.qos {
					QoS::AtMostOnce => {}
					QoS::AtLeastOnce => self.write_packet(|b| v5::PubAck::new(p.pkid).write(b)),
					QoS::ExactlyOnce => {
						self.write_packet(|b| v5::PubRec::new(p.pkid).write(b));
						// broker → PUBREL, client → PUBCOMP
						if let Some(Packet::PubRel(r)) = self.read(timeout) {
							self.write_packet(|b| v5::PubComp::new(r.pkid).write(b));
						}
					}
				}
				Some(p)
			}
			_ => None,
		}
	}

	/// Reads one MQTT packet, accumulating socket bytes until one frames. `None`
	/// on timeout or connection close (incl. the broker's minimal DISCONNECT,
	/// which `mqttbytes` can't parse — treated as a close).
	fn read(&mut self, timeout: Duration) -> Option<Packet> {
		let deadline = Instant::now() + timeout;
		loop {
			match v5::read(&mut self.buf, 1 << 20) {
				Ok(pkt) => return Some(pkt),
				Err(MqttError::InsufficientBytes(_)) => {}
				Err(_) => return None,
			}
			let remaining = deadline.checked_duration_since(Instant::now())?;
			self.sock
				.set_read_timeout(Some(remaining.max(Duration::from_millis(1))))
				.ok();
			let mut tmp = [0u8; 8192];
			match self.sock.read(&mut tmp) {
				Ok(0) => return None,
				Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
				Err(_) => return None,
			}
		}
	}

	fn raw(&mut self, bytes: &[u8]) {
		self.sock.write_all(bytes).ok();
	}

	fn is_closed(&mut self) -> bool {
		self.read(Duration::from_secs(2)).is_none()
	}
}

fn payload(p: &v5::Publish) -> &[u8] {
	&p.payload
}

// --- tests: core pub/sub -----------------------------------------------------

#[test]
fn connect_returns_success_connack() {
	let port = default_broker();
	let _c = Client::connect(port, "it-connect");
	// reaching here means CONNACK Success was received and asserted in `connect`
}

#[test]
fn qos0_publish_is_delivered() {
	let port = default_broker();
	let mut sub = Client::connect(port, "it-q0-sub");
	sub.subscribe("it/q0", QoS::AtMostOnce);
	let mut pubc = Client::connect(port, "it-q0-pub");
	assert!(pubc.publish("it/q0", b"hello0", QoS::AtMostOnce));
	let got = sub.recv(Duration::from_secs(2)).expect("delivery");
	assert_eq!(payload(&got), b"hello0");
}

#[test]
fn qos1_publish_round_trips() {
	let port = default_broker();
	let mut sub = Client::connect(port, "it-q1-sub");
	sub.subscribe("it/q1", QoS::AtLeastOnce);
	let mut pubc = Client::connect(port, "it-q1-pub");
	assert!(
		pubc.publish("it/q1", b"hello1", QoS::AtLeastOnce),
		"publisher got PUBACK"
	);
	let got = sub.recv(Duration::from_secs(2)).expect("delivery");
	assert_eq!(got.qos, QoS::AtLeastOnce);
	assert_eq!(payload(&got), b"hello1");
}

#[test]
fn qos2_publish_completes_both_handshakes() {
	let port = default_broker();
	let mut sub = Client::connect(port, "it-q2-sub");
	sub.subscribe("it/q2", QoS::ExactlyOnce);
	let mut pubc = Client::connect(port, "it-q2-pub");
	assert!(
		pubc.publish("it/q2", b"hello2", QoS::ExactlyOnce),
		"publisher completed PUBREC/PUBREL/PUBCOMP"
	);
	let got = sub.recv(Duration::from_secs(2)).expect("delivery");
	assert_eq!(got.qos, QoS::ExactlyOnce);
	assert_eq!(payload(&got), b"hello2");
}

#[test]
fn qos_downgraded_to_granted() {
	// Subscribed at QoS 0, a QoS 2 publish is delivered at QoS 0.
	let port = default_broker();
	let mut sub = Client::connect(port, "it-dg-sub");
	sub.subscribe("it/dg", QoS::AtMostOnce);
	let mut pubc = Client::connect(port, "it-dg-pub");
	assert!(pubc.publish("it/dg", b"x", QoS::ExactlyOnce));
	let got = sub.recv(Duration::from_secs(2)).expect("delivery");
	assert_eq!(got.qos, QoS::AtMostOnce);
}

#[test]
fn retained_message_reaches_late_subscriber() {
	let port = default_broker();
	let mut pubc = Client::connect(port, "it-ret-pub");
	assert!(pubc.publish_retain("it/ret", b"retained-value", QoS::AtLeastOnce));
	// Subscriber connects *after* the publish — must still receive it.
	let mut sub = Client::connect(port, "it-ret-sub");
	sub.subscribe("it/ret", QoS::AtLeastOnce);
	let got = sub.recv(Duration::from_secs(2)).expect("retained replay");
	assert_eq!(payload(&got), b"retained-value");
	assert!(got.retain, "retained replay carries the retain flag");
	// Clear it with an empty retained publish; a new subscriber gets nothing.
	assert!(pubc.publish_retain("it/ret", b"", QoS::AtLeastOnce));
	let mut sub2 = Client::connect(port, "it-ret-sub2");
	sub2.subscribe("it/ret", QoS::AtLeastOnce);
	assert!(
		sub2.recv(Duration::from_millis(600)).is_none(),
		"cleared retained is gone"
	);
}

#[test]
fn wildcard_subscriptions_match() {
	let port = default_broker();
	let mut sub = Client::connect(port, "it-wild-sub");
	sub.subscribe("it/wild/+/temp", QoS::AtMostOnce);
	sub.subscribe("it/deep/#", QoS::AtMostOnce);
	let mut pubc = Client::connect(port, "it-wild-pub");
	assert!(pubc.publish("it/wild/kitchen/temp", b"+match", QoS::AtMostOnce));
	assert_eq!(
		payload(&sub.recv(Duration::from_secs(2)).expect("+ match")),
		b"+match"
	);
	assert!(pubc.publish("it/deep/a/b/c", b"#match", QoS::AtMostOnce));
	assert_eq!(
		payload(&sub.recv(Duration::from_secs(2)).expect("# match")),
		b"#match"
	);
}

#[test]
fn unsubscribe_stops_delivery() {
	let port = default_broker();
	let mut sub = Client::connect(port, "it-unsub-sub");
	sub.subscribe("it/unsub", QoS::AtMostOnce);
	let mut pubc = Client::connect(port, "it-unsub-pub");
	assert!(pubc.publish("it/unsub", b"before", QoS::AtMostOnce));
	assert_eq!(
		payload(&sub.recv(Duration::from_secs(2)).expect("before unsub")),
		b"before"
	);
	sub.unsubscribe("it/unsub");
	assert!(pubc.publish("it/unsub", b"after", QoS::AtMostOnce));
	assert!(
		sub.recv(Duration::from_millis(600)).is_none(),
		"no delivery after unsubscribe"
	);
}

// --- tests: sessions ---------------------------------------------------------

#[test]
fn persistent_session_queues_offline_then_replays() {
	let port = default_broker();
	// Durable subscriber (clean_start=false, non-zero session expiry), then vanish.
	let mut sub = Client::try_connect(port, "it-session", false, None, Some(3600)).unwrap();
	sub.subscribe("it/session", QoS::AtLeastOnce);
	drop(sub); // socket close = suspend, session lingers

	// Publish while the subscriber is offline — must queue in its session.
	let mut pubc = Client::connect(port, "it-session-pub");
	assert!(pubc.publish("it/session", b"while-offline", QoS::AtLeastOnce));

	// Reconnect the same client id: the queued message is replayed on resume.
	let mut sub = Client::try_connect(port, "it-session", false, None, Some(3600)).unwrap();
	let got = sub
		.recv(Duration::from_secs(2))
		.expect("offline queue replayed");
	assert_eq!(payload(&got), b"while-offline");
}

// --- tests: will -------------------------------------------------------------

#[test]
fn will_message_fires_on_abrupt_disconnect() {
	let port = default_broker();
	let mut watcher = Client::connect(port, "it-will-watch");
	watcher.subscribe("it/will", QoS::AtMostOnce);

	// A client with a will, killed abruptly (no DISCONNECT) → the will fires.
	let willer_sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
	willer_sock.set_nodelay(true).ok();
	let mut c = v5::Connect::new("it-willer");
	c.clean_session = true;
	c.keep_alive = 30;
	c.last_will = Some(v5::LastWill::new(
		"it/will",
		b"rip".to_vec(),
		QoS::AtMostOnce,
		false,
	));
	let mut willer = Client { sock: willer_sock, buf: BytesMut::new(), pkid: 0 };
	willer.write_packet(|b| c.write(b));
	assert!(matches!(
		willer.read(Duration::from_secs(2)),
		Some(Packet::ConnAck(_))
	));
	drop(willer); // abrupt close

	let got = watcher
		.recv(Duration::from_secs(2))
		.expect("will delivered");
	assert_eq!(payload(&got), b"rip");
}

#[test]
fn will_on_reserved_or_wildcard_topic_is_dropped() {
	let port = default_broker();
	// Watch the exact reserved topic a malicious will would try to forge (a
	// literal $SYS/... subscription; wildcards don't match $-topics).
	let mut watcher = Client::connect(port, "it-will-sys-watch");
	watcher.subscribe("$SYS/broker/version", QoS::AtMostOnce);

	// A client whose will targets the broker-reserved $SYS namespace, retained.
	let sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
	sock.set_nodelay(true).ok();
	let mut c = v5::Connect::new("it-will-sys");
	c.clean_session = true;
	c.keep_alive = 30;
	c.last_will = Some(v5::LastWill::new(
		"$SYS/broker/version",
		b"pwned".to_vec(),
		QoS::AtMostOnce,
		true, // retain — would poison future subscribers too
	));
	let mut willer = Client { sock, buf: BytesMut::new(), pkid: 0 };
	willer.write_packet(|b| c.write(b));
	assert!(matches!(
		willer.read(Duration::from_secs(2)),
		Some(Packet::ConnAck(_))
	));
	drop(willer); // abrupt close — a valid will would fire here

	// The forged $SYS will must have been dropped at CONNECT: nothing arrives.
	assert!(
		watcher.recv(Duration::from_millis(800)).is_none(),
		"will on the reserved $SYS namespace must be dropped, not published"
	);

	// And a wildcard will topic is likewise refused (it would reach the router).
	let sock2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
	sock2.set_nodelay(true).ok();
	let mut c2 = v5::Connect::new("it-will-wild");
	c2.clean_session = true;
	c2.keep_alive = 30;
	c2.last_will = Some(v5::LastWill::new(
		"it/#",
		b"nope".to_vec(),
		QoS::AtMostOnce,
		false,
	));
	let mut willer2 = Client { sock: sock2, buf: BytesMut::new(), pkid: 0 };
	willer2.write_packet(|b| c2.write(b));
	assert!(matches!(
		willer2.read(Duration::from_secs(2)),
		Some(Packet::ConnAck(_))
	));
	let mut wildwatch = Client::connect(port, "it-will-wild-watch");
	wildwatch.subscribe("it/room", QoS::AtMostOnce);
	drop(willer2);
	assert!(
		wildwatch.recv(Duration::from_millis(800)).is_none(),
		"will on a wildcard topic must be dropped"
	);
}

// --- tests: resilience -------------------------------------------------------

#[test]
fn malformed_frame_closes_connection_but_broker_survives() {
	let port = default_broker();
	// A reserved packet type (0) is a protocol violation — broker closes the socket.
	let mut bad = Client::connect(port, "it-malformed");
	bad.raw(&[0x00, 0x00]);
	assert!(bad.is_closed(), "broker closed the malformed connection");
	// The broker is still healthy: a fresh honest client connects and pub/subs fine.
	let mut sub = Client::connect(port, "it-after-malformed-sub");
	sub.subscribe("it/health", QoS::AtMostOnce);
	let mut pubc = Client::connect(port, "it-after-malformed-pub");
	assert!(pubc.publish("it/health", b"ok", QoS::AtMostOnce));
	assert_eq!(
		payload(&sub.recv(Duration::from_secs(2)).expect("still serving")),
		b"ok"
	);
}

// --- tests: auth + ACL -------------------------------------------------------

#[test]
fn auth_rejects_bad_password_and_anonymous() {
	let port = auth_broker();
	// Wrong password → BadUserNamePassword.
	let err = Client::try_connect(port, "it-auth-bad", true, Some(("alice", "wrong")), None).unwrap_err();
	assert_eq!(err, v5::ConnectReturnCode::BadUserNamePassword);
	// No credentials + allow_anonymous=false → NotAuthorized.
	let err = Client::try_connect(port, "it-auth-anon", true, None, None).unwrap_err();
	assert_eq!(err, v5::ConnectReturnCode::NotAuthorized);
	// Correct credentials → success.
	let _ok = Client::try_connect(port, "it-auth-good", true, Some(("alice", "s3cret")), None).unwrap();
}

#[test]
fn acl_blocks_out_of_scope_topics() {
	let port = auth_broker();
	// alice may pub/sub only under allowed/#.
	let mut alice = Client::try_connect(port, "it-acl-a", true, Some(("alice", "s3cret")), None).unwrap();
	let mut sub = Client::try_connect(port, "it-acl-sub", true, Some(("alice", "s3cret")), None).unwrap();
	sub.subscribe("allowed/#", QoS::AtMostOnce);

	// In-scope publish is delivered.
	assert!(alice.publish("allowed/room", b"ok", QoS::AtMostOnce));
	assert_eq!(
		payload(
			&sub.recv(Duration::from_secs(2))
				.expect("in-scope delivered")
		),
		b"ok"
	);

	// Out-of-scope publish is dropped (QoS 0, silently) — the subscriber to a
	// matching filter sees nothing, proving the publish was refused at the ACL.
	let mut wide = Client::try_connect(port, "it-acl-wide", true, Some(("alice", "s3cret")), None).unwrap();
	wide.subscribe("blocked/#", QoS::AtMostOnce);
	assert!(alice.publish("blocked/room", b"nope", QoS::AtMostOnce));
	assert!(
		wide.recv(Duration::from_millis(600)).is_none(),
		"ACL blocked the out-of-scope publish"
	);
}

// --- tests: cross-shard (multi-shard mesh) -----------------------------------

#[test]
fn cross_shard_delivery() {
	let port = multishard_broker();
	let mut sub = Client::connect(port, "it-xshard-sub");
	sub.subscribe("it/xshard/#", QoS::AtLeastOnce);
	std::thread::sleep(Duration::from_millis(150));
	// Fresh publisher connections spread across shards via SO_REUSEPORT.
	let n = 20;
	for i in 0..n {
		let mut pubc = Client::connect(port, &format!("it-xshard-pub-{i}"));
		assert!(pubc.publish("it/xshard/x", format!("m{i}").as_bytes(), QoS::AtLeastOnce));
	}
	let mut got = 0;
	while got < n && sub.recv(Duration::from_secs(2)).is_some() {
		got += 1;
	}
	assert_eq!(got, n, "every cross-shard publish was delivered");
}

#[test]
fn shared_subscription_delivers_each_message_once() {
	let port = multishard_broker();
	let mut a = Client::connect(port, "it-share-a");
	let mut b = Client::connect(port, "it-share-b");
	a.subscribe("$share/g/it/share/#", QoS::AtLeastOnce);
	b.subscribe("$share/g/it/share/#", QoS::AtLeastOnce);
	std::thread::sleep(Duration::from_millis(300)); // let Join events replicate across shards

	let m = 30;
	let mut pubc = Client::connect(port, "it-share-pub");
	for i in 0..m {
		assert!(pubc.publish("it/share/x", format!("s{i}").as_bytes(), QoS::AtLeastOnce));
	}
	let mut seen: Vec<Vec<u8>> = Vec::new();
	for c in [&mut a, &mut b] {
		while let Some(p) = c.recv(Duration::from_millis(800)) {
			seen.push(payload(&p).to_vec());
		}
	}
	seen.sort();
	seen.dedup();
	assert_eq!(
		seen.len(),
		m,
		"each message delivered exactly once across the group"
	);
}

// --- tests: connection parking -------------------------------------------------
//
// These run against a broker with `parking.idle_grace_secs = 1`, so a client
// that stays fully idle for ~1 s has its connection task torn down and its fd
// parked on the shard's readiness ring. Every test below first idles past the
// grace (2.5 s, comfortably beyond it) and then proves the client cannot tell
// the difference: deliveries arrive (egress wake), its own packets are answered
// (ingress wake), keep-alive and Will semantics hold, and takeover still works.

/// Parking fixture: single shard, 1-second idle grace (so tests can observe the
/// parked state), 1-second `$SYS` updates for the gauge test.
fn parking_broker() -> u16 {
	static PORT: OnceLock<u16> = OnceLock::new();
	*PORT.get_or_init(|| {
		let mut cfg = base_config(free_port(), 1);
		cfg.parking.idle_grace_secs = 1;
		cfg.sys.interval = 1;
		start(cfg)
	})
}

/// Parking fixture with no server keep-alive override, so a client's own tiny
/// keep-alive drives the parked keep-alive-expiry path.
fn parking_keepalive_broker() -> u16 {
	static PORT: OnceLock<u16> = OnceLock::new();
	*PORT.get_or_init(|| {
		let mut cfg = base_config(free_port(), 1);
		cfg.parking.idle_grace_secs = 1;
		cfg.limits.keep_alive = 0; // the client's own keep-alive rules
		start(cfg)
	})
}

/// Idle long enough that a fully-idle connection is certainly parked
/// (grace 1 s + scheduling slack).
fn idle_past_grace() {
	std::thread::sleep(Duration::from_millis(2500));
}

/// Connects with a Will Message and a caller-chosen keep-alive.
fn connect_with_will(port: u16, id: &str, keep_alive: u16, will_topic: &str) -> Client {
	let sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
	sock.set_nodelay(true).ok();
	let mut c = v5::Connect::new(id);
	c.clean_session = true;
	c.keep_alive = keep_alive;
	c.last_will = Some(v5::LastWill::new(
		will_topic,
		b"parked-rip".to_vec(),
		QoS::AtMostOnce,
		false,
	));
	let mut client = Client { sock, buf: BytesMut::new(), pkid: 0 };
	client.write_packet(|b| c.write(b));
	assert!(matches!(
		client.read(Duration::from_secs(2)),
		Some(Packet::ConnAck(_))
	));
	client
}

/// Egress wake: a publish routed to a parked subscriber resurrects it and is
/// delivered — twice, so the park → wake → re-park → wake cycle is covered (the
/// second round would fail on a stale-completion or re-arm bug).
#[test]
fn parked_subscriber_receives_publishes_across_park_cycles() {
	let port = parking_broker();
	let mut sub = Client::connect(port, "it-park-sub");
	sub.subscribe("it/park/egress", QoS::AtLeastOnce);

	for round in 0..2 {
		idle_past_grace(); // sub is parked now
		let mut pubc = Client::connect(port, &format!("it-park-pub-{round}"));
		let msg = format!("wake-{round}");
		assert!(pubc.publish("it/park/egress", msg.as_bytes(), QoS::AtLeastOnce));
		let got = sub
			.recv(Duration::from_secs(3))
			.unwrap_or_else(|| panic!("delivery to the parked subscriber, round {round}"));
		assert_eq!(payload(&got), msg.as_bytes(), "round {round}");
		assert_eq!(
			got.qos,
			QoS::AtLeastOnce,
			"QoS 1 handshake intact, round {round}"
		);
	}
}

/// Ingress wake: a parked client's own PINGREQ resurrects it and is answered.
#[test]
fn parked_client_ping_is_answered() {
	let port = parking_broker();
	let mut c = Client::connect(port, "it-park-ping");
	idle_past_grace();
	c.raw(&[0xC0, 0x00]); // PINGREQ
	assert!(
		matches!(c.read(Duration::from_secs(3)), Some(Packet::PingResp)),
		"parked connection answered PINGREQ after resurrection"
	);
}

/// Ingress wake with data: a parked client publishes and the full QoS 1
/// handshake completes; the message reaches a live subscriber.
#[test]
fn parked_client_can_publish() {
	let port = parking_broker();
	let mut sub = Client::connect(port, "it-park-pub-sub");
	sub.subscribe("it/park/ingress", QoS::AtLeastOnce);

	let mut c = Client::connect(port, "it-park-pub-client");
	idle_past_grace(); // c is parked
	assert!(
		c.publish("it/park/ingress", b"from-parked", QoS::AtLeastOnce),
		"parked client's publish completed its PUBACK handshake"
	);
	assert_eq!(
		payload(&sub.recv(Duration::from_secs(3)).expect("delivered")),
		b"from-parked"
	);
}

/// Keep-alive is enforced while parked: a silent client past 1.5× its own
/// keep-alive is reaped by the parking task's sweep and its Will fires.
#[test]
fn parked_keepalive_expiry_fires_will() {
	let port = parking_keepalive_broker();
	let mut watcher = Client::connect(port, "it-park-ka-watch");
	watcher.subscribe("it/park/ka-will", QoS::AtMostOnce);

	// keep_alive = 2 → deadline 3 s; the grace (1 s) parks it well before that.
	let _victim = connect_with_will(port, "it-park-ka-victim", 2, "it/park/ka-will");
	let got = watcher
		.recv(Duration::from_secs(8))
		.expect("will from the parked keep-alive expiry");
	assert_eq!(payload(&got), b"parked-rip");
}

/// An abrupt close (EOF) while parked resurrects the connection, which observes
/// the EOF and runs the normal abnormal-close path: the Will fires.
#[test]
fn parked_connection_eof_fires_will() {
	let port = parking_broker();
	let mut watcher = Client::connect(port, "it-park-eof-watch");
	watcher.subscribe("it/park/eof-will", QoS::AtMostOnce);

	let victim = connect_with_will(port, "it-park-eof-victim", 60, "it/park/eof-will");
	idle_past_grace(); // victim is parked
	drop(victim); // abrupt close, no DISCONNECT
	let got = watcher
		.recv(Duration::from_secs(5))
		.expect("will fired after EOF on a parked connection");
	assert_eq!(payload(&got), b"parked-rip");
}

/// Session takeover reaches a parked connection: a second CONNECT with the same
/// client id closes the dormant fd (no Will — takeover semantics) and the new
/// connection works normally.
#[test]
fn takeover_closes_parked_connection() {
	let port = parking_broker();
	let mut watcher = Client::connect(port, "it-park-tko-watch");
	watcher.subscribe("it/park/tko-will", QoS::AtMostOnce);

	let mut old = connect_with_will(port, "it-park-tko", 60, "it/park/tko-will");
	idle_past_grace(); // old is parked

	let mut new = Client::connect(port, "it-park-tko"); // takeover
	assert!(
		old.is_closed(),
		"the parked predecessor's socket was closed by the takeover"
	);
	assert!(
		watcher.recv(Duration::from_secs(2)).is_none(),
		"takeover publishes no Will"
	);
	// The new connection is fully functional.
	new.subscribe("it/park/tko-check", QoS::AtMostOnce);
	let mut pubc = Client::connect(port, "it-park-tko-pub");
	assert!(pubc.publish("it/park/tko-check", b"alive", QoS::AtMostOnce));
	assert_eq!(
		payload(&new.recv(Duration::from_secs(2)).expect("delivered")),
		b"alive"
	);
}

/// The `$SYS/broker/parked-connections` gauge reports parked connections.
#[test]
fn sys_gauge_reports_parked_connections() {
	let port = parking_broker();
	// A client that will park and stay parked for the whole test.
	let _idler = Client::connect(port, "it-park-gauge-idler");
	idle_past_grace();

	// The gauge is retained and republished every second; poll until it is ≥ 1.
	let mut sys = Client::connect(port, "it-park-gauge-sub");
	sys.subscribe("$SYS/broker/parked-connections", QoS::AtMostOnce);
	let deadline = Instant::now() + Duration::from_secs(8);
	let mut last = String::new();
	while Instant::now() < deadline {
		let Some(p) = sys.recv(Duration::from_secs(2)) else {
			continue;
		};
		last = String::from_utf8_lossy(payload(&p)).into_owned();
		if last.parse::<u64>().is_ok_and(|n| n >= 1) {
			return;
		}
	}
	panic!("$SYS parked gauge never reached ≥ 1 (last value: {last:?})");
}
