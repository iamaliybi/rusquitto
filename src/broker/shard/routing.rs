//! Fan-out: turning one published message into the set of per-subscriber
//! deliveries, plus the retained-message table it consults.

use std::collections::HashMap;
use std::rc::Rc;

use mqttbytes::{QoS, v5::Publish};

use super::ShardState;
use crate::broker::session::{Delivery, MAILBOX_LIMIT, OFFLINE_QUEUE_LIMIT};
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
		let mut matches = Vec::new();
		self.trie.matching(&publish.topic, &mut matches);

		// Collapse overlapping subscriptions to one Match per client, keeping the
		// options of the highest-QoS match. Ordinary subscribers go in `best` (each
		// gets a copy); shared subscribers are bucketed by group name in `groups`
		// (one member of each is picked below). Owned keys so the trie borrow ends
		// before we touch `sessions`.
		let mut best: HashMap<String, Match> = HashMap::new();
		let mut groups: HashMap<String, HashMap<String, Match>> = HashMap::new();
		for sub in matches {
			let bucket = match &sub.share_group {
				None => &mut best,
				// No Local never applies here: it is a protocol error on a shared
				// subscription (MQTT 5 §3.8.3.1), rejected at SUBSCRIBE — which also
				// keeps every shard's view of the group's candidates identical.
				Some(group) => groups.entry(group.clone()).or_default(),
			};
			let entry = bucket.entry(sub.client_id.clone()).or_insert(Match {
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
			if m.nolocal && publisher == Some(client_id.as_str()) {
				continue;
			}
			let qos = min_qos(publish.qos, m.qos);
			let retain = was_retained && m.retain_as_published;
			self.deliver_to(&client_id, &publish, qos, retain, m.sub_ids);
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
			let mut ids: Vec<String> = members.keys().cloned().collect();
			if ids.is_empty() {
				continue;
			}
			ids.sort();
			let online: Vec<String> = ids
				.iter()
				.filter(|id| self.sessions.get(*id).is_some_and(|s| s.mailbox.is_some()))
				.cloned()
				.collect();

			let picked = match self.shared_remote.get(&group).filter(|r| !r.is_empty()) {
				Some(remote) => {
					// Global pick over the merged, sorted view of connected members
					// everywhere. Deterministic: same list + same hash on every shard.
					let mut all: Vec<&str> = online
						.iter()
						.map(String::as_str)
						.chain(remote.iter().map(String::as_str))
						.collect();
					all.sort_unstable();
					all.dedup();
					let choice = all[shared_pick_index(&publish.topic, &publish.payload, all.len())];
					// Deliver only if the chosen member is ours; otherwise the shard
					// that owns it makes the same choice and delivers there.
					online.iter().find(|id| id.as_str() == choice).cloned()
				}
				None => {
					// Purely local group: round-robin, preferring connected members
					// (a suspended member still queues QoS > 0 when it is all we have).
					let pool = if online.is_empty() {
						ids
					} else {
						online
					};
					let cursor = self.shared_cursor.entry(group).or_insert(0);
					let picked = pool[*cursor % pool.len()].clone();
					*cursor = cursor.wrapping_add(1);
					Some(picked)
				}
			};

			if let Some(client_id) = picked {
				let m = &members[&client_id];
				let qos = min_qos(publish.qos, m.qos);
				let retain = was_retained && m.retain_as_published;
				self.deliver_to(&client_id, &publish, qos, retain, m.sub_ids.clone());
			}
		}
	}

	/// Delivers one message to a single client's session: straight to its live
	/// mailbox if connected, otherwise buffered in its offline queue (QoS > 0 only;
	/// QoS 0 is dropped for a suspended session). `sub_ids` are the Subscription
	/// Identifiers to echo on the delivered PUBLISH.
	///
	/// (See `route` for how shared-group deliveries pick their one recipient.)
	fn deliver_to(&mut self, client_id: &str, publish: &Rc<Publish>, qos: QoS, retain: bool, sub_ids: Vec<usize>) {
		let Some(session) = self.sessions.get_mut(client_id) else {
			return;
		};
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
			None if qos != QoS::AtMostOnce => {
				if session.offline_queue.len() >= OFFLINE_QUEUE_LIMIT {
					session.offline_queue.pop_front();
				}
				session
					.offline_queue
					.push_back(Delivery { publish: publish.clone(), qos, retain, sub_ids });
			}
			None => {}
		}
	}
}
