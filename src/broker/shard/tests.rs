use super::*;
use glommio::channels::local_channel;
use mqttbytes::QoS;

/// Runs a future on a throwaway glommio executor — needed by the parking tests
/// to `recv()` from the [`UnparkCmd`] command channel.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
	glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
		.make()
		.expect("build test executor")
		.run(fut)
}

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
			snapshot: None,
			offline_queue: VecDeque::new(),
			pending_will: None,
			parked: false,
			wake_pending: false,
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
fn wal_batch_tracks_suspend_offline_and_resume() {
	use crate::broker::session::PersistedSession;
	use std::collections::HashMap;

	let mut s = ShardState::default();
	s.enable_wal();
	let now = Instant::now();

	// Connect, subscribe, then suspend with a non-zero expiry → durable.
	let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
	let h = s.open_session("c1", tx, false);
	s.subscribe(
		"home/#",
		"c1",
		opts(QoS::AtLeastOnce, false, false, None, None),
	);
	assert!(s.close_session(
		"c1",
		h.generation,
		3600,
		SessionSnapshot::default(),
		VecDeque::new()
	));

	// A message routed to the now-suspended session queues offline (and re-dirties it).
	s.deliver_local(pubm("home/kitchen", QoS::AtLeastOnce, b"hot", false), None);
	assert_eq!(offline(&s, "c1").len(), 1);

	// The batch upserts c1; replaying it rebuilds the session with its queued message.
	let batch = s
		.take_wal_batch(now)
		.expect("a WAL batch after suspend + offline enqueue");
	let mut restored: HashMap<String, PersistedSession> = HashMap::new();
	crate::persistence::wal::apply(&batch, &mut restored);
	let ps = restored.get("c1").expect("c1 upserted into the WAL");
	assert_eq!(ps.session.offline.len(), 1, "queued message captured");
	assert_eq!(ps.session.subscriptions.len(), 1, "subscription captured");

	// Reconnecting resumes it → a tombstone; replaying that removes the durable copy.
	let (tx2, _rx2) = local_channel::new_unbounded::<Delivery>();
	s.open_session("c1", tx2, false);
	let batch = s.take_wal_batch(now).expect("a WAL batch after resume");
	crate::persistence::wal::apply(&batch, &mut restored);
	assert!(
		!restored.contains_key("c1"),
		"resume tombstoned the durable session"
	);
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
	s.sessions.get_mut("willed").unwrap().pending_will = Some(Box::new((
		pubm("will/topic", QoS::AtLeastOnce, b"bye", false),
		Instant::now(),
	)));

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
			snapshot: Some(Box::new(SessionSnapshot {
				inflight: HashMap::from([(
					5,
					InflightMessage {
						publish: pubm("out/5", QoS::AtLeastOnce, b"i5", false),
						state: InflightState::Qos1,
					},
				)]),
				incoming_qos2: HashMap::from([(9, pubm("in/9", QoS::ExactlyOnce, b"i9", false))]),
				next_pkid: 42,
			})),
			offline_queue: VecDeque::new(),
			pending_will: None,
			parked: false,
			wake_pending: false,
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

	// Durable QoS state round-trips exactly (boxed while suspended).
	let snapshot = session
		.snapshot
		.as_ref()
		.expect("suspended snapshot present");
	assert_eq!(snapshot.next_pkid, 42);
	assert!(matches!(snapshot.inflight[&5].state, InflightState::Qos1));
	assert_eq!(snapshot.inflight[&5].publish.topic, "out/5");
	assert_eq!(snapshot.incoming_qos2[&9].topic, "in/9");

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

#[test]
fn gc_indexes_reclaims_stale_shared_cursor() {
	let mut s = ShardState::default();
	arm(&mut s, "c1");
	s.subscribe(
		"t",
		"c1",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);
	// Routing to the purely-local group populates its round-robin cursor.
	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"x", false), None);
	assert!(
		s.shared_cursor.contains_key("g"),
		"cursor created for the group"
	);

	// The member unsubscribes; the group is now dead, but the cursor lingers until GC.
	s.unsubscribe("t", "c1", Some("g"));
	s.gc_indexes();
	assert!(
		!s.shared_cursor.contains_key("g"),
		"gc reclaims the cursor once no local member holds the group"
	);
}

#[test]
fn shared_events_maintain_the_remote_view() {
	use crate::broker::messages::SharedEvent;
	let mut s = ShardState::default();
	s.apply_shared_event(SharedEvent::Join { group: "g".into(), client_id: "r1".into() });
	s.apply_shared_event(SharedEvent::Join { group: "g".into(), client_id: "r2".into() });
	assert_eq!(s.shared_remote["g"].len(), 2);

	// Idempotent join, then leaves; the empty set is dropped entirely.
	s.apply_shared_event(SharedEvent::Join { group: "g".into(), client_id: "r1".into() });
	assert_eq!(s.shared_remote["g"].len(), 2);
	s.apply_shared_event(SharedEvent::Leave { group: "g".into(), client_id: "r1".into() });
	s.apply_shared_event(SharedEvent::Leave { group: "g".into(), client_id: "r2".into() });
	assert!(!s.shared_remote.contains_key("g"));
}

#[test]
fn shared_global_pick_skips_local_suspended_when_remote_members_exist() {
	use crate::broker::messages::SharedEvent;
	let mut s = ShardState::default();
	// A suspended local member of group g...
	arm(&mut s, "local");
	s.subscribe(
		"t",
		"local",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);
	// ...and a connected member on another shard.
	s.apply_shared_event(SharedEvent::Join { group: "g".into(), client_id: "remote".into() });

	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"x", false), None);

	// Globally, only connected members are candidates: the remote shard owns the
	// pick, so nothing is queued locally (the old per-shard behavior would have
	// parked the message in the suspended member's offline queue).
	assert_eq!(offline(&s, "local").len(), 0);
}

// --- parking ----------------------------------------------------------------

/// Opens a live session and parks it, returning its generation.
fn park(state: &mut ShardState, client: &str) -> u64 {
	let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
	let h = state.open_session(client, tx, false);
	assert!(
		state.park_session(client, h.generation, SessionSnapshot::default()),
		"parking a live session under its own generation must succeed"
	);
	h.generation
}

#[test]
fn park_session_flips_state_and_checks_generation() {
	let mut s = ShardState::default();
	let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
	let h = s.open_session("c1", tx, false);

	// A stale generation (taken-over connection) must not park.
	assert!(!s.park_session("c1", h.generation + 1, SessionSnapshot::default()));
	assert!(s.sessions["c1"].mailbox.is_some(), "session untouched");

	let snapshot = SessionSnapshot { next_pkid: 17, ..Default::default() };
	assert!(s.park_session("c1", h.generation, snapshot));
	let session = &s.sessions["c1"];
	assert!(session.parked);
	assert!(session.mailbox.is_none());
	assert!(
		session.expires_at.is_none(),
		"parked = connected, no expiry"
	);
	assert_eq!(
		session.snapshot.as_ref().unwrap().next_pkid,
		17,
		"durable state stored like a suspension (migration-ready)"
	);
	assert_eq!(session.generation, h.generation, "generation not bumped");

	// Double-park (no live mailbox any more) is refused.
	assert!(!s.park_session("c1", h.generation, SessionSnapshot::default()));
}

#[test]
fn deliver_to_parked_queues_all_qos_and_wakes_exactly_once() {
	block_on(async {
		let mut s = ShardState::default();
		let (cmd_tx, cmd_rx) = local_channel::new_unbounded::<UnparkCmd>();
		s.set_unpark_tx(cmd_tx);
		park(&mut s, "c1");
		s.subscribe("t", "c1", opts(QoS::AtLeastOnce, false, false, None, None));

		// QoS 0 queues too — a parked client is connected, not suspended.
		s.deliver_local(pubm("t", QoS::AtMostOnce, b"q0", false), None);
		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"q1", false), None);
		assert_eq!(offline(&s, "c1").len(), 2, "QoS 0 and QoS 1 both queued");

		// Exactly one Wake for the whole park episode.
		match cmd_rx.recv().await {
			Some(UnparkCmd::Wake { client_id }) => assert_eq!(client_id, "c1"),
			other => panic!("expected one Wake, got {other:?}"),
		}
		assert!(
			futures_lite::future::poll_once(cmd_rx.recv())
				.await
				.is_none(),
			"further deliveries must not send further Wakes"
		);
	});
}

#[test]
fn reattach_parked_returns_queue_in_order_and_clears_wake() {
	block_on(async {
		let mut s = ShardState::default();
		let (cmd_tx, _cmd_rx) = local_channel::new_unbounded::<UnparkCmd>();
		s.set_unpark_tx(cmd_tx);
		let generation = park(&mut s, "c1");
		s.subscribe("t", "c1", opts(QoS::AtLeastOnce, false, false, None, None));
		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"first", false), None);
		s.deliver_local(pubm("t", QoS::AtLeastOnce, b"second", false), None);

		// A stale generation must not reattach.
		let (m1, _r1) = local_channel::new_unbounded::<Delivery>();
		assert!(s.reattach_parked("c1", generation + 1, m1).is_none());

		let (m2, _r2) = local_channel::new_unbounded::<Delivery>();
		let (_snapshot, queued) = s
			.reattach_parked("c1", generation, m2)
			.expect("matching generation reattaches");
		let payloads: Vec<&[u8]> = queued.iter().map(|d| &d.publish.payload[..]).collect();
		assert_eq!(payloads, vec![&b"first"[..], &b"second"[..]], "in order");
		let session = &s.sessions["c1"];
		assert!(!session.parked);
		assert!(!session.wake_pending, "wake dedup reset for the next park");
		assert!(session.mailbox.is_some());

		// Reattaching again (no longer parked) is refused.
		let (m3, _r3) = local_channel::new_unbounded::<Delivery>();
		assert!(s.reattach_parked("c1", generation, m3).is_none());
	});
}

#[test]
fn takeover_of_parked_session_signals_close_with_old_generation() {
	block_on(async {
		let mut s = ShardState::default();
		let (cmd_tx, cmd_rx) = local_channel::new_unbounded::<UnparkCmd>();
		s.set_unpark_tx(cmd_tx);
		let old_generation = park(&mut s, "c1");

		// A new connection resumes the same client id: session takeover.
		let (tx2, _rx2) = local_channel::new_unbounded::<Delivery>();
		let h2 = s.open_session("c1", tx2, false);
		assert!(
			h2.resumed,
			"parked session state is resumed by the takeover"
		);
		assert!(!s.sessions["c1"].parked);

		match cmd_rx.recv().await {
			Some(UnparkCmd::Close { client_id, generation }) => {
				assert_eq!(client_id, "c1");
				assert_eq!(
					generation, old_generation,
					"Close carries the parked generation for the race check"
				);
			}
			other => panic!("expected Close, got {other:?}"),
		}
	});
}

#[test]
fn clean_start_over_parked_session_signals_close_and_discards() {
	block_on(async {
		let mut s = ShardState::default();
		let (cmd_tx, cmd_rx) = local_channel::new_unbounded::<UnparkCmd>();
		s.set_unpark_tx(cmd_tx);
		park(&mut s, "c1");

		let (tx2, _rx2) = local_channel::new_unbounded::<Delivery>();
		let h2 = s.open_session("c1", tx2, true);
		assert!(!h2.resumed, "Clean Start discards the parked session");
		assert!(matches!(cmd_rx.recv().await, Some(UnparkCmd::Close { .. })));
	});
}

#[test]
fn mesh_claim_of_parked_session_migrates_state_and_signals_close() {
	use crate::broker::messages::SessionControl;
	block_on(async {
		let mut s = ShardState::default();
		let (cmd_tx, cmd_rx) = local_channel::new_unbounded::<UnparkCmd>();
		s.set_unpark_tx(cmd_tx);
		let (tx, _rx) = local_channel::new_unbounded::<Delivery>();
		let h = s.open_session("c1", tx, false);
		let snapshot = SessionSnapshot { next_pkid: 23, ..Default::default() };
		assert!(s.park_session("c1", h.generation, snapshot));

		// Another shard claims the client (it reconnected there).
		s.on_control(SessionControl::Claim { client_id: "c1".into(), requester: 1, resume: true });

		assert!(
			!s.sessions.contains_key("c1"),
			"parked session migrated away (extract works because park stores the snapshot)"
		);
		assert!(
			matches!(cmd_rx.recv().await, Some(UnparkCmd::Close { .. })),
			"the orphaned parked fd is told to close"
		);
	});
}

#[test]
fn suspend_parked_sets_expiry_wal_and_destroys_on_zero() {
	let mut s = ShardState::default();
	s.enable_wal();
	let generation = park(&mut s, "c1");
	let _ = s.take_wal_batch(Instant::now()); // clear open_session's tombstone

	// Stale generation: no-op (taken over meanwhile — caller publishes no Will).
	assert!(!s.suspend_parked("c1", generation + 1, 3600));
	assert!(s.sessions["c1"].parked, "untouched on mismatch");

	assert!(s.suspend_parked("c1", generation, 3600));
	let session = &s.sessions["c1"];
	assert!(!session.parked);
	assert!(session.expires_at.is_some(), "finite expiry now armed");
	assert!(
		s.take_wal_batch(Instant::now()).is_some(),
		"now genuinely suspended: durable state enters the WAL"
	);

	// Expiry 0 destroys outright (the parked analogue of close_session).
	let generation = park(&mut s, "c2");
	assert!(s.suspend_parked("c2", generation, 0));
	assert!(!s.sessions.contains_key("c2"));
}

#[test]
fn persistence_skips_parked_sessions() {
	let mut s = ShardState::default();
	park(&mut s, "parked");
	arm(&mut s, "suspended");
	s.sessions.get_mut("suspended").unwrap().expires_at = Some(Instant::now() + Duration::from_secs(3600));

	let persisted = s.persist_sessions(Instant::now());
	assert_eq!(
		persisted.len(),
		1,
		"only the truly suspended session persists"
	);
	assert_eq!(persisted[0].client_id, "suspended");
}

#[test]
fn parked_queueing_does_not_dirty_the_wal() {
	let mut s = ShardState::default();
	s.enable_wal();
	park(&mut s, "c1");
	s.subscribe("t", "c1", opts(QoS::AtLeastOnce, false, false, None, None));
	let _ = s.take_wal_batch(Instant::now()); // clear open_session's tombstone

	s.deliver_local(pubm("t", QoS::AtLeastOnce, b"x", false), None);
	assert_eq!(offline(&s, "c1").len(), 1);
	assert!(
		s.take_wal_batch(Instant::now()).is_none(),
		"a parked session is a live connection, not durable state"
	);
}

#[test]
fn parked_shared_member_counts_as_online_for_the_group_pick() {
	let mut s = ShardState::default();
	// One parked member, one truly suspended member, same local group.
	park(&mut s, "parked");
	arm(&mut s, "suspended");
	s.subscribe(
		"t",
		"parked",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);
	s.subscribe(
		"t",
		"suspended",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);

	// Round-robin over ONLINE members only: the parked one is online, the
	// suspended one is not — so every pick lands on the parked member.
	for i in 0..4 {
		s.deliver_local(
			pubm("t", QoS::AtLeastOnce, format!("m{i}").as_bytes(), false),
			None,
		);
	}
	assert_eq!(
		offline(&s, "parked").len(),
		4,
		"all picks hit the parked member"
	);
	assert_eq!(offline(&s, "suspended").len(), 0);
}

/// Two shards' worth of state, cross-replicated views, one message stream:
/// every message must be delivered to exactly one member cluster-wide.
#[test]
fn shared_global_pick_delivers_exactly_once_across_shards() {
	use crate::broker::messages::SharedEvent;

	// Shard A owns alice; shard B owns bob. Each sees the other via its
	// replicated remote view — exactly what the mesh Join broadcasts build.
	let mut shard_a = ShardState::default();
	let (tx_a, _rx_a) = local_channel::new_unbounded::<Delivery>();
	shard_a.open_session("alice", tx_a, false);
	shard_a.subscribe(
		"t",
		"alice",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);
	shard_a.apply_shared_event(SharedEvent::Join { group: "g".into(), client_id: "bob".into() });

	let mut shard_b = ShardState::default();
	let (tx_b, _rx_b) = local_channel::new_unbounded::<Delivery>();
	shard_b.open_session("bob", tx_b, false);
	shard_b.subscribe(
		"t",
		"bob",
		opts(QoS::AtLeastOnce, false, false, Some("g"), None),
	);
	shard_b.apply_shared_event(SharedEvent::Join { group: "g".into(), client_id: "alice".into() });

	let queued = |s: &ShardState, id: &str| {
		s.sessions[id]
			.mailbox
			.as_ref()
			.expect("member is connected")
			.len()
	};

	let mut alice_total = 0;
	let mut bob_total = 0;
	for i in 0..24 {
		let payload = format!("message-{i}");
		// The same publish reaches both shards (origin fan-out + mesh broadcast).
		shard_a.deliver_local(pubm("t", QoS::AtLeastOnce, payload.as_bytes(), false), None);
		shard_b.deliver_local(pubm("t", QoS::AtLeastOnce, payload.as_bytes(), false), None);

		// Exactly one shard delivered this message.
		let total = queued(&shard_a, "alice") + queued(&shard_b, "bob");
		assert_eq!(
			total,
			alice_total + bob_total + 1,
			"message {i} must be delivered exactly once cluster-wide"
		);
		alice_total = queued(&shard_a, "alice");
		bob_total = queued(&shard_b, "bob");
	}
	// The content hash spreads work across both members (statistically certain
	// over 24 distinct payloads).
	assert!(alice_total > 0, "alice received a share of the messages");
	assert!(bob_total > 0, "bob received a share of the messages");
}
