use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalSender;
use mqttbytes::{v5::Publish, QoS};

use crate::broker::topic_trie::{filter_matches, TopicTrie};

/// Returns the lower of two QoS levels.
fn min_qos(a: QoS, b: QoS) -> QoS {
	if (a as u8) <= (b as u8) { a } else { b }
}

/// A message routed toward a connection for delivery to its client.
///
/// The `publish` is shared via `Rc` so one message fans out to many subscribers
/// on the same shard without re-allocating; `qos` is the effective QoS for this
/// particular subscriber (`min(publish QoS, granted QoS)`). The receiving
/// connection assigns its own packet id when `qos > 0`.
pub struct Delivery {
	pub publish: Rc<Publish>,
	pub qos: QoS,
}

/// Sender half of a connection's mailbox.
///
/// `LocalSender` is single-owner (not `Clone`), so each connection's sender is
/// stored exactly once — in [`ShardState::clients`] — and subscriptions refer to
/// the connection by `client_id` rather than holding their own sender.
pub type Mailbox = LocalSender<Delivery>;

/// Per-shard broker state.
///
/// Single-threaded and shared between every connection on the shard via
/// `Rc<RefCell<>>`. No locks are needed: in the thread-per-core model no other
/// core ever touches this memory.
#[derive(Default)]
pub struct ShardState {
	/// Connected clients on this shard -> their mailbox sender.
	clients: HashMap<String, Mailbox>,
	/// Subscription index: wildcard-aware topic trie keyed by filter.
	trie: TopicTrie,
	/// Last retained message per topic. Replicated on every shard (each retained
	/// publish is broadcast to all shards), so a new subscriber finds matches
	/// locally.
	retained: HashMap<String, Publish>,
	/// Senders to every other shard in the full channel mesh. `None` until the
	/// shard joins the mesh in `worker::init`.
	mesh: Option<Senders<Publish>>,
}

impl ShardState {
	/// Creates a fresh, shareable handle to this shard's state.
	pub fn new() -> Rc<RefCell<Self>> {
		Rc::new(RefCell::new(Self::default()))
	}

	/// Stores this shard's mesh senders so publishes can be forwarded to peers.
	pub fn set_mesh(&mut self, senders: Senders<Publish>) {
		self.mesh = Some(senders);
	}

	/// Forwards a publish to every *other* shard in the mesh. Each peer runs its
	/// own local `route`, so a remote subscriber receives it identically.
	///
	/// `try_send_to` is non-blocking (drop-on-full, matching QoS 0 semantics) and
	/// the publisher never stalls on a slow peer. Self is skipped — the local
	/// fan-out is handled directly by `route`.
	pub fn broadcast(&self, publish: &Publish) {
		let Some(senders) = &self.mesh else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			let _ = senders.try_send_to(idx, publish.clone());
		}
	}

	/// Records a connected client's mailbox. Called once at CONNECT.
	pub fn register(&mut self, client_id: String, mailbox: Mailbox) {
		self.clients.insert(client_id, mailbox);
	}

	/// Subscribes a client to a topic filter with a granted QoS. The filter may
	/// contain the `+` and `#` wildcards. Re-subscribing replaces the prior entry.
	pub fn subscribe(&mut self, filter: &str, client_id: &str, qos: QoS) {
		self.trie.insert(filter, client_id, qos);
	}

	/// Removes a single subscription (used by UNSUBSCRIBE).
	pub fn unsubscribe(&mut self, filter: &str, client_id: &str) {
		self.trie.remove(filter, client_id);
	}

	/// Routes one publish on this shard: updates the retain table if the retain
	/// flag is set, then fans it out to local subscribers. Shared by the local
	/// publish path and the mesh drain task.
	pub fn deliver_local(&mut self, mut publish: Publish) {
		if publish.retain {
			self.update_retain(&publish);
		}
		// The retain flag is only set when a message is delivered because of a new
		// subscription; on live fan-out it is always cleared.
		publish.retain = false;
		self.route(Rc::new(publish));
	}

	/// Inserts or clears a retained message. A retained publish with an empty
	/// payload removes the stored message (MQTT spec).
	fn update_retain(&mut self, publish: &Publish) {
		if publish.payload.is_empty() {
			self.retained.remove(&publish.topic);
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

	/// Fans a message out to every local subscriber whose filter matches the
	/// publish topic.
	///
	/// Wildcard-aware via the topic trie. A client matching through several
	/// overlapping filters receives a single copy, at the highest QoS it was
	/// granted across those filters (capped by the publish QoS). Uses `try_send`
	/// so a slow or full consumer never blocks the publisher.
	fn route(&self, publish: Rc<Publish>) {
		let mut matches = Vec::new();
		self.trie.matching(&publish.topic, &mut matches);

		// Collapse overlapping subscriptions to the best granted QoS per client.
		let mut best: HashMap<&str, QoS> = HashMap::new();
		for sub in matches {
			let entry = best.entry(sub.client_id.as_str()).or_insert(sub.qos);
			if (sub.qos as u8) > (*entry as u8) {
				*entry = sub.qos;
			}
		}

		for (client_id, granted) in best {
			if let Some(mailbox) = self.clients.get(client_id) {
				let qos = min_qos(publish.qos, granted);
				let _ = mailbox.try_send(Delivery {
					publish: publish.clone(),
					qos,
				});
			}
		}
	}

	/// Removes a client's mailbox and all of its subscriptions. Called on
	/// disconnect or EOF; dropping the mailbox closes the connection's channel.
	pub fn disconnect(&mut self, client_id: &str) {
		self.clients.remove(client_id);
		self.trie.remove_client(client_id);
	}
}
