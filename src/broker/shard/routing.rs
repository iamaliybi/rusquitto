//! Fan-out: turning one published message into the set of per-subscriber
//! deliveries, plus the retained-message table it consults.

use std::collections::HashMap;
use std::rc::Rc;

use mqttbytes::{QoS, v5::Publish};

use glommio::channels::local_channel::LocalSender;

use super::{Session, ShardState, WalPending};
use crate::broker::delivery::{Delivery, MAILBOX_LIMIT, OFFLINE_QUEUE_LIMIT, UnparkCmd};
use crate::broker::topics::filter_matches;
use crate::protocol::min_qos;

/// The chosen subscription for a client during routing: the options of its
/// highest-QoS matching filter, plus the identifiers of *all* its matching
/// subscriptions (MQTT 5 delivers every matching Subscription Identifier).
struct Match {
	qos: QoS,
	nolocal: bool,
	retain_as_published: bool,
	sub_ids: Vec<usize>,
}

/// Deterministic member index for a shared-group delivery.
///
/// Hashes the message content (topic + payload) so every shard — each seeing the
/// identical forwarded publish and the identical sorted member list — selects
/// the same member without any coordination. `DefaultHasher::new()` is
/// fixed-keyed, so the result is consistent across shards (and across processes
/// of the same build, for future clustering). Fairness is statistical rather
/// than round-robin, which distributed load-balancing tolerates by design.
fn shared_pick_index(topic: &str, payload: &[u8], members: usize) -> usize {
	use std::hash::{DefaultHasher, Hasher};
	let mut hasher = DefaultHasher::new();
	hasher.write(topic.as_bytes());
	hasher.write(payload);
	(hasher.finish() % members as u64) as usize
}

impl ShardState {
	/// Routes one publish on this shard: updates the retain table if the retain
	/// flag is set, then fans it out to local subscribers. Shared by the local
	/// publish path and the mesh drain task.
	///
	/// `publisher` is the client id that produced this message, when it is local
	/// to this shard (`None` for mesh-forwarded publishes and broker-internal
	/// ones); it drives the No Local subscription option.
	pub fn deliver_local(&mut self, mut publish: Publish, publisher: Option<&str>) {
		let was_retained = publish.retain;
		if was_retained {
			self.update_retain(&publish);
		}
		// Clear the wire retain flag; each delivery's flag is decided per subscriber
		// in `route` (kept only for Retain-As-Published subscribers).
		publish.retain = false;
		self.route(Rc::new(publish), publisher, was_retained);
	}

	/// Inserts or clears a retained message. A retained publish with an empty
	/// payload removes the stored message (MQTT spec). A new topic is refused once
	/// the shard's retained cap is reached (updates to existing topics still apply).
	fn update_retain(&mut self, publish: &Publish) {
		if publish.payload.is_empty() {
			self.retained.remove(&publish.topic);
		} else if self.retained_limit > 0
			&& self.retained.len() >= self.retained_limit
			&& !self.retained.contains_key(&publish.topic)
		{
			// At capacity and this is a new topic: drop it rather than grow unbounded.
		} else {
			self.retained.insert(publish.topic.clone(), publish.clone());
		}
	}

	/// Returns the retained messages whose topic matches a subscription `filter`,
	/// for replay to a newly-subscribed client.
	pub fn retained_matching(&self, filter: &str) -> Vec<Publish> {
		self.retained
			.values()
			.filter(|p| filter_matches(filter, &p.topic))
			.cloned()
			.collect()
	}

	/// Snapshots the whole retained set, for persistence.
	pub fn retained_messages(&self) -> Vec<Publish> {
		self.retained.values().cloned().collect()
	}

	/// Restores retained messages from a snapshot (startup), respecting the shard's
	/// retained cap. Every shard loads the same snapshot into its own table, so no
	/// cross-shard broadcast is needed.
	pub fn load_retained(&mut self, messages: Vec<Publish>) {
		for message in messages {
			// Empty-payload "clears" are never persisted, but skip them defensively.
			if message.payload.is_empty() {
				continue;
			}
			if self.retained_limit > 0
				&& self.retained.len() >= self.retained_limit
				&& !self.retained.contains_key(&message.topic)
			{
				continue;
			}
			self.retained.insert(message.topic.clone(), message);
		}
	}

	/// Fans a message out to the local subscribers whose filter matches the publish
	/// topic.
	///
	/// Wildcard-aware via the topic trie. An *ordinary* subscriber matching through
	/// several overlapping filters receives a single copy, using the options of its
	/// highest-QoS matching subscription (capped by the publish QoS). A *shared*
	/// subscription group (`$share/{group}/…`) instead delivers to exactly one of its
	/// members, chosen round-robin, so the group load-balances. Honours the No Local
	/// and Retain As Published options. Uses `try_send` so a slow or full consumer
	/// never blocks the publisher.
	fn route(&mut self, publish: Rc<Publish>, publisher: Option<&str>, was_retained: bool) {
		// Disjoint field borrows. The subscription matches borrow the trie immutably
		// for the whole fan-out, which lets `best`/`groups` key on the subscribers'
		// borrowed `&str` client ids instead of cloning an owned `String` per matched
		// subscriber — but only because delivery reaches `sessions`/`wal`/`unpark_tx`
		// through the free `deliver_to` rather than back through `self` (a `&mut self`
		// call would collide with the live trie borrow). On a wide fan-out this drops
		// one heap allocation per matched subscriber from the hot path.
		let Self {
			trie, sessions, shared_cursor, shared_remote, wal, unpark_tx, ..
		} = self;

		let mut matches = Vec::new();
		trie.matching(&publish.topic, &mut matches);

		// Collapse overlapping subscriptions to one Match per client, keeping the
		// options of the highest-QoS match. Ordinary subscribers go in `best` (each
		// gets a copy); shared subscribers are bucketed by group name in `groups`
		// (one member of each is picked below). Keys borrow the trie, so the trie
		// stays borrowed until the fan-out below completes.
		let mut best: HashMap<&str, Match> = HashMap::new();
		let mut groups: HashMap<&str, HashMap<&str, Match>> = HashMap::new();
		for sub in matches {
			let bucket = match &sub.share_group {
				None => &mut best,
				// No Local never applies here: it is a protocol error on a shared
				// subscription (MQTT 5 §3.8.3.1), rejected at SUBSCRIBE — which also
				// keeps every shard's view of the group's candidates identical.
				Some(group) => groups.entry(group.as_str()).or_default(),
			};
			let entry = bucket.entry(sub.client_id.as_str()).or_insert(Match {
				qos: sub.qos,
				nolocal: sub.nolocal,
				retain_as_published: sub.retain_as_published,
				sub_ids: Vec::new(),
			});
			if (sub.qos as u8) > (entry.qos as u8) {
				entry.qos = sub.qos;
				entry.nolocal = sub.nolocal;
				entry.retain_as_published = sub.retain_as_published;
			}
			// Every matching subscription's identifier is delivered (MQTT 5),
			// regardless of which one won the QoS tie-break.
			if let Some(id) = sub.sub_id
				&& !entry.sub_ids.contains(&id)
			{
				entry.sub_ids.push(id);
			}
		}

		// Ordinary subscribers: one copy each.
		for (client_id, m) in best {
			// No Local: never echo a message back to the client that published it.
			if m.nolocal && publisher == Some(client_id) {
				continue;
			}
			let qos = min_qos(publish.qos, m.qos);
			let retain = was_retained && m.retain_as_published;
			deliver_to(
				sessions, wal, unpark_tx, client_id, &publish, qos, retain, m.sub_ids,
			);
		}

		// Shared groups: exactly one member per group receives each message.
		//
		// When the group also has members on *other* shards (known via the
		// replicated membership view), the pick must be globally consistent:
		// every shard sees this same publish (mesh broadcast) and the same
		// sorted member list, and applies the same content hash to it — so all
		// shards agree on the one recipient, and only the shard owning that
		// member delivers. No coordination round-trip is needed. When the group
		// is purely local, the original round-robin is kept (better fairness,
		// and suspended members may still queue QoS > 0 messages).
		for (group, members) in groups {
			let mut ids: Vec<&str> = members.keys().copied().collect();
			if ids.is_empty() {
				continue;
			}
			ids.sort_unstable();
			// A parked member counts as online: it is still a connected client (a
			// delivery queues and wakes it), and — critically — it never announced a
			// shared-group Leave, so *remote* shards still count it in the global
			// pick. Excluding it locally would desync the deterministic choice
			// (double- or zero-delivery).
			let online: Vec<&str> = ids
				.iter()
				.copied()
				.filter(|id| {
					sessions
						.get(*id)
						.is_some_and(|s| s.mailbox.is_some() || s.parked)
				})
				.collect();

			let picked: Option<&str> = match shared_remote.get(group).filter(|r| !r.is_empty()) {
				Some(remote) => {
					// Global pick over the merged, sorted view of connected members
					// everywhere. Deterministic: same list + same hash on every shard.
					let mut all: Vec<&str> = online
						.iter()
						.copied()
						.chain(remote.iter().map(String::as_str))
						.collect();
					all.sort_unstable();
					all.dedup();
					let choice = all[shared_pick_index(&publish.topic, &publish.payload, all.len())];
					// Deliver only if the chosen member is ours; otherwise the shard
					// that owns it makes the same choice and delivers there.
					online.iter().copied().find(|id| *id == choice)
				}
				None => {
					// Purely local group: round-robin, preferring connected members
					// (a suspended member still queues QoS > 0 when it is all we have).
					let pool = if online.is_empty() {
						&ids
					} else {
						&online
					};
					// One owned key per shared-group publish (not per subscriber) to
					// index the persistent per-group cursor — negligible next to the
					// per-subscriber clone this refactor removed.
					let cursor = shared_cursor.entry(group.to_string()).or_insert(0);
					let picked = pool[*cursor % pool.len()];
					*cursor = cursor.wrapping_add(1);
					Some(picked)
				}
			};

			if let Some(client_id) = picked {
				let m = &members[client_id];
				let qos = min_qos(publish.qos, m.qos);
				let retain = was_retained && m.retain_as_published;
				deliver_to(
					sessions,
					wal,
					unpark_tx,
					client_id,
					&publish,
					qos,
					retain,
					m.sub_ids.clone(),
				);
			}
		}
	}
}

/// Delivers one message to a single client's session, taking the shard's session
/// map (and WAL / unpark handles) directly rather than through `&mut self`, so
/// [`ShardState::route`] can call it while the topic trie is still borrowed —
/// which is what lets its per-subscriber map keys stay borrowed `&str` (no owned
/// `String` cloned per matched subscriber).
///
/// Straight to the client's live mailbox if connected; queued (and the parking
/// task woken) if *parked*; otherwise buffered in its offline queue (QoS > 0
/// only; QoS 0 is dropped for a suspended session). `sub_ids` are the
/// Subscription Identifiers to echo on the delivered PUBLISH. (See `route` for
/// how shared-group deliveries pick their one recipient.)
#[allow(clippy::too_many_arguments)]
fn deliver_to(
	sessions: &mut HashMap<String, Session>,
	wal: &mut Option<WalPending>,
	unpark_tx: &Option<LocalSender<UnparkCmd>>,
	client_id: &str,
	publish: &Rc<Publish>,
	qos: QoS,
	retain: bool,
	sub_ids: Vec<usize>,
) {
	let Some(session) = sessions.get_mut(client_id) else {
		return;
	};
	let mut queued_offline = false;
	let mut wake = false;
	match &session.mailbox {
		Some(mailbox) => {
			// The mailbox channel is unbounded so an idle connection allocates
			// nothing; this length guard enforces the same drop-on-full bound a
			// bounded channel would (a consumer that stops reading its socket
			// stops draining, and unbounded growth would be a DoS).
			if mailbox.len() < MAILBOX_LIMIT {
				let _ = mailbox.try_send(Delivery { publish: publish.clone(), qos, retain, sub_ids });
			}
		}
		None if session.parked => {
			// Parked: the client is *connected*, just task-less — so QoS 0 is
			// queued too (a suspended session would drop it), bounded like the
			// offline queue. One deduplicated Wake resurrects the connection,
			// which drains everything queued here. No WAL write: a parked
			// session is a live connection, not durable state.
			if session.offline_queue.len() >= OFFLINE_QUEUE_LIMIT {
				session.offline_queue.pop_front();
			}
			session
				.offline_queue
				.push_back(Delivery { publish: publish.clone(), qos, retain, sub_ids });
			if !session.wake_pending {
				session.wake_pending = true;
				wake = true;
			}
		}
		None if qos != QoS::AtMostOnce => {
			if session.offline_queue.len() >= OFFLINE_QUEUE_LIMIT {
				session.offline_queue.pop_front();
			}
			session
				.offline_queue
				.push_back(Delivery { publish: publish.clone(), qos, retain, sub_ids });
			queued_offline = true;
		}
		None => {}
	}
	// The suspended session's durable offline queue grew: re-log it in the WAL
	// (inline `ShardState::wal_dirty`, which we cannot call without `self` here).
	if queued_offline && let Some(w) = wal.as_mut() {
		w.removed.remove(client_id);
		if !w.dirty.contains(client_id) {
			w.dirty.insert(client_id.to_string());
		}
	}
	if wake && let Some(tx) = unpark_tx {
		// Unbounded local channel: only errors at shard teardown, where the
		// parked fd is reclaimed by the shutdown drain instead.
		let _ = tx.try_send(UnparkCmd::Wake { client_id: client_id.to_string() });
	}
}
