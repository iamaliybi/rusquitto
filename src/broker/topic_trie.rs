use std::collections::HashMap;

use mqttbytes::QoS;

/// One client's subscription, with the QoS the broker granted and its options.
pub struct Subscription {
	pub client_id: String,
	pub qos: QoS,
	/// No Local: don't deliver a message to this subscriber if it was the one
	/// that published it.
	pub nolocal: bool,
	/// Retain As Published: forward the publisher's original retain flag rather
	/// than clearing it on live fan-out.
	pub retain_as_published: bool,
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
	/// the same client to the same filter replaces the prior entry. Returns
	/// `true` if this was a new subscription (the client was not already
	/// subscribed to this exact filter) — used for Retain Handling.
	pub fn insert(
		&mut self,
		filter: &str,
		client_id: &str,
		qos: QoS,
		nolocal: bool,
		retain_as_published: bool,
	) -> bool {
		let mut node = &mut self.root;
		for seg in filter.split('/') {
			node = node.children.entry(seg.to_string()).or_default();
		}
		let is_new = !node.subscribers.iter().any(|s| s.client_id == client_id);
		node.subscribers.retain(|s| s.client_id != client_id);
		node.subscribers.push(Subscription {
			client_id: client_id.to_string(),
			qos,
			nolocal,
			retain_as_published,
		});
		is_new
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

	/// Removes every subscription belonging to a client and returns them as
	/// `(filter, qos, nolocal, retain_as_published)` tuples, reconstructing each
	/// filter from its path through the trie. Used to migrate a session's
	/// subscriptions to another shard on cross-shard resume.
	pub fn take_client(&mut self, client_id: &str) -> Vec<(String, QoS, bool, bool)> {
		let mut out = Vec::new();
		let mut segments: Vec<String> = Vec::new();
		Self::take_client_rec(&mut self.root, client_id, &mut segments, &mut out);
		out
	}

	fn take_client_rec(
		node: &mut Node,
		client_id: &str,
		segments: &mut Vec<String>,
		out: &mut Vec<(String, QoS, bool, bool)>,
	) {
		node.subscribers.retain(|s| {
			if s.client_id == client_id {
				out.push((segments.join("/"), s.qos, s.nolocal, s.retain_as_published));
				false
			} else {
				true
			}
		});
		for (seg, child) in node.children.iter_mut() {
			segments.push(seg.clone());
			Self::take_client_rec(child, client_id, segments, out);
			segments.pop();
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

#[cfg(test)]
mod tests {
	use super::*;

	fn matches(trie: &TopicTrie, topic: &str) -> Vec<(String, bool, bool)> {
		let mut out = Vec::new();
		trie.matching(topic, &mut out);
		out.iter()
			.map(|s| (s.client_id.clone(), s.nolocal, s.retain_as_published))
			.collect()
	}

	#[test]
	fn insert_reports_new_then_existing() {
		let mut trie = TopicTrie::default();
		assert!(trie.insert("a/b", "c1", QoS::AtLeastOnce, false, false));
		// Re-subscribing the same client to the same filter is not new.
		assert!(!trie.insert("a/b", "c1", QoS::ExactlyOnce, false, false));
		// A different client on the same filter is new.
		assert!(trie.insert("a/b", "c2", QoS::AtMostOnce, false, false));
	}

	#[test]
	fn options_are_stored_and_matched() {
		let mut trie = TopicTrie::default();
		trie.insert("sensors/#", "c1", QoS::AtLeastOnce, true, true);
		let got = matches(&trie, "sensors/kitchen/temp");
		assert_eq!(got, vec![("c1".to_string(), true, true)]);
	}

	#[test]
	fn resubscribe_replaces_options() {
		let mut trie = TopicTrie::default();
		trie.insert("t", "c1", QoS::AtLeastOnce, true, true);
		trie.insert("t", "c1", QoS::AtLeastOnce, false, false);
		let got = matches(&trie, "t");
		assert_eq!(got, vec![("c1".to_string(), false, false)]);
	}

	#[test]
	fn take_client_removes_and_returns_filters() {
		let mut trie = TopicTrie::default();
		trie.insert("a/b", "c1", QoS::AtLeastOnce, true, false);
		trie.insert("x/+/z", "c1", QoS::ExactlyOnce, false, true);
		trie.insert("a/b", "c2", QoS::AtMostOnce, false, false);

		let mut taken = trie.take_client("c1");
		taken.sort_by(|a, b| a.0.cmp(&b.0));
		assert_eq!(
			taken,
			vec![
				("a/b".to_string(), QoS::AtLeastOnce, true, false),
				("x/+/z".to_string(), QoS::ExactlyOnce, false, true),
			]
		);

		// c1 is gone from both filters; c2 remains on a/b.
		assert_eq!(matches(&trie, "a/b"), vec![("c2".to_string(), false, false)]);
		assert!(matches(&trie, "x/y/z").is_empty());
		// Taking a client with no subscriptions yields nothing.
		assert!(trie.take_client("c1").is_empty());
	}
}
