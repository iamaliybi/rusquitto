use std::collections::HashMap;

use mqttbytes::QoS;

/// One client's subscription, with the QoS the broker granted.
pub struct Subscription {
	pub client_id: String,
	pub qos: QoS,
}

/// Tests whether a subscription `filter` (possibly containing `+`/`#`) matches a
/// concrete `topic`. Used to find retained messages for a new subscription — the
/// reverse direction of [`TopicTrie::matching`].
pub fn filter_matches(filter: &str, topic: &str) -> bool {
	// Wildcards never match a `$`-prefixed topic at the first level.
	if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
		return false;
	}

	let mut f = filter.split('/');
	let mut t = topic.split('/');

	loop {
		match (f.next(), t.next()) {
			(Some("#"), _) => return true, // matches this level and all below
			(Some("+"), Some(_)) => continue, // matches exactly one level
			(Some(fs), Some(ts)) if fs == ts => continue,
			(None, None) => return true,
			_ => return false,
		}
	}
}

/// A node in the topic tree. Each level of a filter (split on `/`) is an edge;
/// the wildcards `+` and `#` are stored as ordinary segment keys.
#[derive(Default)]
struct Node {
	children: HashMap<String, Node>,
	/// Subscribers whose filter terminates at this node.
	subscribers: Vec<Subscription>,
}

/// A topic trie for MQTT subscription matching.
///
/// Supports the two MQTT wildcards:
/// - `+` — matches exactly one topic level.
/// - `#` — matches the remaining levels (must be the final segment of a filter);
///   per spec it also matches the parent level (`sport/#` matches `sport`).
///
/// Wildcards never match a topic whose first level begins with `$`.
#[derive(Default)]
pub struct TopicTrie {
	root: Node,
}

impl TopicTrie {
	/// Inserts (or refreshes) a subscription for `filter`. Re-subscribing from
	/// the same client to the same filter replaces the prior entry.
	pub fn insert(&mut self, filter: &str, client_id: &str, qos: QoS) {
		let mut node = &mut self.root;
		for seg in filter.split('/') {
			node = node.children.entry(seg.to_string()).or_default();
		}
		node.subscribers.retain(|s| s.client_id != client_id);
		node.subscribers.push(Subscription {
			client_id: client_id.to_string(),
			qos,
		});
	}

	/// Removes a single subscription (used by UNSUBSCRIBE).
	pub fn remove(&mut self, filter: &str, client_id: &str) {
		let mut node = &mut self.root;
		for seg in filter.split('/') {
			match node.children.get_mut(seg) {
				Some(child) => node = child,
				None => return,
			}
		}
		node.subscribers.retain(|s| s.client_id != client_id);
	}

	/// Removes every subscription belonging to a client (used on disconnect).
	pub fn remove_client(&mut self, client_id: &str) {
		Self::remove_client_rec(&mut self.root, client_id);
	}

	fn remove_client_rec(node: &mut Node, client_id: &str) {
		node.subscribers.retain(|s| s.client_id != client_id);
		for child in node.children.values_mut() {
			Self::remove_client_rec(child, client_id);
		}
	}

	/// Collects every subscription whose filter matches the concrete `topic`.
	pub fn matching<'a>(&'a self, topic: &str, out: &mut Vec<&'a Subscription>) {
		let segments: Vec<&str> = topic.split('/').collect();
		let dollar_top = segments.first().is_some_and(|s| s.starts_with('$'));
		Self::match_rec(&self.root, &segments, 0, dollar_top, out);
	}

	fn match_rec<'a>(
		node: &'a Node,
		segments: &[&str],
		idx: usize,
		dollar_top: bool,
		out: &mut Vec<&'a Subscription>,
	) {
		if idx == segments.len() {
			out.extend(node.subscribers.iter());
			// `sport/#` also matches the exact topic `sport`.
			if let Some(hash) = node.children.get("#") {
				out.extend(hash.subscribers.iter());
			}
			return;
		}

		let seg = segments[idx];
		// `$`-prefixed topics are not matched by wildcards at the first level.
		let allow_wildcard = !(idx == 0 && dollar_top);

		if let Some(child) = node.children.get(seg) {
			Self::match_rec(child, segments, idx + 1, dollar_top, out);
		}
		if allow_wildcard {
			if let Some(child) = node.children.get("+") {
				Self::match_rec(child, segments, idx + 1, dollar_top, out);
			}
			// `#` consumes this level and all that follow.
			if let Some(child) = node.children.get("#") {
				out.extend(child.subscribers.iter());
			}
		}
	}
}
