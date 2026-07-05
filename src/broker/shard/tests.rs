use super::*;
use glommio::channels::local_channel;
use mqttbytes::QoS;

fn pubm(topic: &str, qos: QoS, payload: &[u8], retain: bool) -> Publish {
	let mut p = Publish::new(topic, qos, payload.to_vec());
	p.retain = retain;
	p
}

fn opts(
	qos: QoS,
	nolocal: bool,
	retain_as_published: bool,
	share_group: Option<&str>,
	sub_id: Option<usize>,
) -> SubOptions<'_> {
	SubOptions { qos, nolocal, retain_as_published, share_group, sub_id }
}

/// Installs a *suspended* session (no live mailbox) for `client`, so any message
/// routed to it lands in its offline queue where the test can inspect it.
fn arm(state: &mut ShardState, client: &str) {
	state.sessions.insert(
		client.to_string(),
		Session {
			mailbox: None,
			expires_at: None,
			generation: 1,
			snapshot: SessionSnapshot::default(),
			offline_queue: VecDeque::new(),
			pending_will: None,
		},
	);
}

/// The offline queue of a client's session.
fn offline<'a>(state: &'a ShardState, client: &str) -> &'a VecDeque<Delivery> {
	&state.sessions[client].offline_queue
}

#[test]
fn route_fans_out_and_downgrades_qos_to_granted() {
	let mut s = ShardState::default();
	arm(&mut s, "c1");
	s.subscribe(
		"home/+/temp",
		"c1",
		opts(QoS::AtLeastOnce, false, false, None, None),
	);

	// A QoS 2 publish to a QoS 1 subscription is delivered at QoS 1.
	s.deliver_local(
		pubm("home/kitchen/temp", QoS::ExactlyOnce, b"21", false),
		None,
	);

	let q = offline(&s, "c1");
	assert_eq!(q.len(), 1);
	assert_eq!(q[0].qos, QoS::AtLeastOnce);
}

#[test]
fn route_delivers_one_copy_with_all_matching_sub_ids() {
	let mut s = ShardState::default();
	arm(&mut s, "c1");
	// Two overlapping subscriptions from the same client, different sub ids.
	s.subscribe(
		"a/+",
		"c1",
		opts(QoS::AtLeastOnce, false, false, None, Some(1)),
	);
	s.subscribe(
		"a/b",
		"c1",
		opts(QoS::AtLeastOnce, false, false, None, Some(2)),
	);

	s.deliver_local(pubm("a/b", QoS::AtLeastOnce, b"x", false), None);

	let q = offline(&s, "c1");
	assert_eq!(q.len(), 1, "one copy, not one per matching filter");
	let mut ids = q[0].sub_ids.clone();
	ids.sort();
	assert_eq!(ids, vec![1, 2]);
}

#[test]
fn route_honours_no_local() {
	let mut s = ShardState::default();
	arm(&mut s, "c1");
	s.subscribe("t", "c1", opts(QoS::AtLeastOnce, true, false, None, None));

	// Publisher is the subscriber -> skipped.
	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"x", false), Some("c1"));
	assert_eq!(offline(&s, "c1").len(), 0);

	// A different publisher -> delivered.
	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"y", false), Some("other"));
	assert_eq!(offline(&s, "c1").len(), 1);
}

#[test]
fn route_retain_as_published_kept_only_for_rap_subscribers() {
	let mut s = ShardState::default();
	arm(&mut s, "keep");
	arm(&mut s, "clear");
	s.subscribe("t", "keep", opts(QoS::AtLeastOnce, false, true, None, None));
	s.subscribe(
		"t",
		"clear",
		opts(QoS::AtLeastOnce, false, false, None, None),
	);

	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"x", true), None);

	assert!(offline(&s, "keep")[0].retain, "RAP subscriber keeps retain");
	assert!(
		!offline(&s, "clear")[0].retain,
		"ordinary subscriber clears it"
	);
}

#[test]
fn route_shared_group_load_balances_round_robin() {
	let mut s = ShardState::default();
	arm(&mut s, "c1");
	arm(&mut s, "c2");
	s.subscribe(
		"t",
		"c1",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);
	s.subscribe(
		"t",
		"c2",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);

	// Two messages to a two-member group -> one each (members sorted: c1, c2).
	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"1", false), None);
	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"2", false), None);

	assert_eq!(offline(&s, "c1").len(), 1);
	assert_eq!(offline(&s, "c2").len(), 1);
}

#[test]
fn retained_is_stored_matched_and_cleared() {
	let mut s = ShardState::default();
	s.deliver_local(pubm("sensors/temp", QoS::AtMostOnce, b"21", true), None);
	assert_eq!(s.retained_matching("sensors/#").len(), 1);

	// An empty retained payload clears it.
	s.deliver_local(pubm("sensors/temp", QoS::AtMostOnce, b"", true), None);
	assert!(s.retained_matching("sensors/#").is_empty());
}

#[test]
fn open_session_fresh_then_resumes_after_suspend() {
	let mut s = ShardState::default();
	let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
	let h = s.open_session("c1", tx, false);
	assert!(!h.resumed);

	// Suspend (non-zero expiry), then reconnect resumes.
	assert!(s.close_session(
		"c1",
		h.generation,
		60,
		SessionSnapshot::default(),
		VecDeque::new()
	));
	let (tx2, _rx2) = local_channel::new_unbounded::<Delivery>();
	let h2 = s.open_session("c1", tx2, false);
	assert!(h2.resumed);
	assert_ne!(h2.generation, h.generation);
}

#[test]
fn close_session_expiry_zero_destroys_session_and_subs() {
	let mut s = ShardState::default();
	let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
	let h = s.open_session("c1", tx, false);
	s.subscribe("t", "c1", opts(QoS::AtLeastOnce, false, false, None, None));

	assert!(s.close_session(
		"c1",
		h.generation,
		0,
		SessionSnapshot::default(),
		VecDeque::new()
	));
	assert!(!s.sessions.contains_key("c1"));
	let mut m = Vec::new();
	s.trie.matching("t", &mut m);
	assert!(m.is_empty(), "subscriptions removed with the session");
}

#[test]
fn close_session_generation_mismatch_is_noop() {
	let mut s = ShardState::default();
	arm(&mut s, "c1");
	// Wrong generation (a stale connection) must not tear down the session.
	assert!(!s.close_session("c1", 999, 0, SessionSnapshot::default(), VecDeque::new()));
	assert!(s.sessions.contains_key("c1"));
}

#[test]
fn shed_connections_drops_live_mailboxes_only() {
	let mut s = ShardState::default();
	let (tx1, _rx1) = local_channel::new_unbounded::<Delivery>();
	let (tx2, _rx2) = local_channel::new_unbounded::<Delivery>();
	s.open_session("live1", tx1, false);
	s.open_session("live2", tx2, false);
	arm(&mut s, "suspended"); // no live mailbox

	// Only the two live connections are shed; the suspended one is untouched.
	assert_eq!(s.shed_connections(5), 2);
	assert!(s.sessions["live1"].mailbox.is_none());
	assert!(s.sessions["live2"].mailbox.is_none());
	// Sessions stay (the connections' own cleanup handles them); nothing left to shed.
	assert_eq!(s.shed_connections(5), 0);
}

#[test]
fn shed_connections_respects_the_batch_size() {
	let mut s = ShardState::default();
	let mut keep = Vec::new();
	for i in 0..5 {
		let (tx, rx) = local_channel::new_unbounded::<Delivery>();
		s.open_session(&format!("c{i}"), tx, false);
		keep.push(rx);
	}
	assert_eq!(s.shed_connections(2), 2);
	let still_live = s.sessions.values().filter(|x| x.mailbox.is_some()).count();
	assert_eq!(still_live, 3);
}

#[test]
fn sweep_fires_due_delayed_will_and_reaps_expired_session() {
	let mut s = ShardState::default();
	arm(&mut s, "willed");
	s.sessions.get_mut("willed").unwrap().pending_will = Some((
		pubm("will/topic", QoS::AtLeastOnce, b"bye", false),
		Instant::now(),
	));

	arm(&mut s, "gone");
	s.subscribe(
		"t",
		"gone",
		opts(QoS::AtLeastOnce, false, false, None, None),
	);
	s.sessions.get_mut("gone").unwrap().expires_at = Some(Instant::now());

	let wills = s.sweep_expired();
	assert_eq!(wills.len(), 1);
	assert_eq!(wills[0].topic, "will/topic");
	assert!(
		!s.sessions.contains_key("gone"),
		"expired session reclaimed"
	);
}

/// The full on-shard persistence cycle, minus disk: a suspended session's
/// subscriptions, offline queue, in-flight QoS state, and expiry are captured by
/// `persist_sessions` and faithfully reinstalled by `load_sessions` into a fresh
/// shard. (The codec's byte round-trip is covered separately under `persistence`;
/// this guards the `ShardState` integration those bytes are produced from.)
#[test]
fn persist_then_load_restores_a_full_suspended_session() {
	use crate::broker::session::{InflightMessage, InflightState};

	let now = Instant::now();
	let mut src = ShardState::default();

	// A suspended session carrying durable QoS state and a finite expiry.
	src.sessions.insert(
		"psess".to_string(),
		Session {
			mailbox: None,
			expires_at: Some(now + Duration::from_secs(3600)),
			generation: 7,
			snapshot: SessionSnapshot {
				inflight: HashMap::from([(
					5,
					InflightMessage {
						publish: pubm("out/5", QoS::AtLeastOnce, b"i5", false),
						state: InflightState::Qos1,
					},
				)]),
				incoming_qos2: HashMap::from([(9, pubm("in/9", QoS::ExactlyOnce, b"i9", false))]),
				next_pkid: 42,
			},
			offline_queue: VecDeque::new(),
			pending_will: None,
		},
	);
	// A subscription (No Local + a sub id) and one message queued while offline.
	src.subscribe(
		"home/+/temp",
		"psess",
		opts(QoS::AtLeastOnce, true, false, None, Some(3)),
	);
	src.deliver_local(
		pubm("home/kitchen/temp", QoS::AtLeastOnce, b"21.5", false),
		Some("other"),
	);
	assert_eq!(
		offline(&src, "psess").len(),
		1,
		"message queued while offline"
	);

	// Persist, then restore into a brand-new shard.
	let persisted = src.persist_sessions(now);
	assert_eq!(persisted.len(), 1);
	let mut dst = ShardState::default();
	dst.load_sessions(persisted, now);

	// Restored as a suspended session, with its expiry.
	let session = dst.sessions.get("psess").expect("session restored");
	assert!(session.mailbox.is_none(), "restored session is suspended");
	assert!(session.expires_at.is_some(), "finite expiry restored");

	// Durable QoS state round-trips exactly.
	assert_eq!(session.snapshot.next_pkid, 42);
	assert!(matches!(
		session.snapshot.inflight[&5].state,
		InflightState::Qos1
	));
	assert_eq!(session.snapshot.inflight[&5].publish.topic, "out/5");
	assert_eq!(session.snapshot.incoming_qos2[&9].topic, "in/9");

	// Offline queue round-trips (payload + sub ids preserved).
	let q = offline(&dst, "psess");
	assert_eq!(q.len(), 1);
	assert_eq!(&q[0].publish.payload[..], b"21.5");
	assert_eq!(q[0].sub_ids, vec![3]);

	// The subscription itself was reinstalled in the trie, No Local option and all:
	// the client's own publish is skipped; another client's matches and is queued.
	dst.deliver_local(
		pubm("home/bath/temp", QoS::AtLeastOnce, b"self", false),
		Some("psess"),
	);
	assert_eq!(
		offline(&dst, "psess").len(),
		1,
		"No Local survived restore: own publish skipped"
	);
	dst.deliver_local(
		pubm("home/bath/temp", QoS::AtLeastOnce, b"warm", false),
		Some("other"),
	);
	assert_eq!(
		offline(&dst, "psess").len(),
		2,
		"restored subscription still matches"
	);
}
