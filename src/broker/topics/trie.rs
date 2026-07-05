use std::collections::HashMap;
use std::rc::Rc;

use mqttbytes::QoS;

use crate::broker::topics::interner::SegmentInterner;

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
	/// Shared Subscription group name (`$share/{group}/{filter}`), or `None` for an
	/// ordinary subscription. Members of the same group on the filter share the load:
	/// each matching message is delivered to just one of them.
	pub share_group: Option<String>,
	/// Subscription Identifier (MQTT 5), echoed on matching deliveries so the client
	/// can tell which subscription produced a message. `None` if the SUBSCRIBE set none.
	pub sub_id: Option<usize>,
}

/// A subscription flattened out of the trie, with its filter reconstructed from
/// the path — used to migrate a client's subscriptions to another shard.
pub struct FlatSub {
	pub filter: String,
	pub qos: QoS,
	pub nolocal: bool,
	pub retain_as_published: bool,
	pub share_group: Option<String>,
	pub sub_id: Option<usize>,
}

/// The options a SUBSCRIBE carries for one filter. Bundled into a struct so
/// callers name each field (the two adjacent `bool`s are easy to transpose
/// positionally) and the signatures stay small.
pub struct SubOptions<'a> {
	/// The QoS the broker granted for this filter.
	pub qos: QoS,
	/// No Local.
	pub nolocal: bool,
	/// Retain As Published.
	pub retain_as_published: bool,
	/// Shared Subscription group name (`$share/{group}/…`), or `None`.
	pub share_group: Option<&'a str>,
	/// Subscription Identifier, if the SUBSCRIBE set one.
	pub sub_id: Option<usize>,
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
			(Some("#"), _) => return true,    // matches this level and all below
			(Some("+"), Some(_)) => continue, // matches exactly one level
			(Some(fs), Some(ts)) if fs == ts => continue,
			(None, None) => return true,
			_ => return false,
		}
	}
}

/// A node in the topic tree. Each level of a filter (split on `/`) is an edge
/// keyed by an interned segment; the wildcards `+` and `#` are ordinary keys.
#[derive(Default)]
struct Node {
	children: HashMap<Rc<str>, Node>,
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
/// Wildcards never match a topic whose first level begins with `$`. Segment keys
/// are interned, so repeated names across the tree share one allocation.
#[derive(Default)]
pub struct TopicTrie {
	root: Node,
	interner: SegmentInterner,
}

impl TopicTrie {
	/// Inserts (or refreshes) a subscription for `filter`. Re-subscribing from
	/// the same client to the same filter (and same share group) replaces the prior
	/// entry. A client may hold both an ordinary subscription and a shared one on
	/// the same filter — they are distinct entries. Returns `true` if this was a
	/// new subscription — used for Retain Handling.
	pub fn insert(&mut self, filter: &str, client_id: &str, opts: SubOptions) -> bool {
		let Self { root, interner } = self;
		let mut node = root;
		for seg in filter.split('/') {
			let key = interner.intern(seg);
			node = node.children.entry(key).or_default();
		}
		let same = |s: &Subscription| s.client_id == client_id && s.share_group.as_deref() == opts.share_group;
		let is_new = !node.subscribers.iter().any(same);
		node.subscribers.retain(|s| !same(s));
		node.subscribers.push(Subscription {
			client_id: client_id.to_string(),
			qos: opts.qos,
			nolocal: opts.nolocal,
			retain_as_published: opts.retain_as_published,
			share_group: opts.share_group.map(str::to_string),
			sub_id: opts.sub_id,
		});
		is_new
	}

	/// Removes a single subscription (used by UNSUBSCRIBE). `share_group` selects
	/// the ordinary (`None`) or shared entry to remove. Returns whether an entry was
	/// actually removed.
	pub fn remove(&mut self, filter: &str, client_id: &str, share_group: Option<&str>) -> bool {
		let mut node = &mut self.root;
		for seg in filter.split('/') {
			match node.children.get_mut(seg) {
				Some(child) => node = child,
				None => return false,
			}
		}
		let before = node.subscribers.len();
		node.subscribers
			.retain(|s| !(s.client_id == client_id && s.share_group.as_deref() == share_group));
		node.subscribers.len() != before
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
	/// [`FlatSub`]s, reconstructing each filter from its path through the trie. Used
	/// to migrate a session's subscriptions to another shard on cross-shard resume.
	pub fn take_client(&mut self, client_id: &str) -> Vec<FlatSub> {
		let mut out = Vec::new();
		let mut segments: Vec<String> = Vec::new();
		Self::take_client_rec(&mut self.root, client_id, &mut segments, &mut out);
		out
	}

	/// Like [`take_client`](Self::take_client) but *non-destructive*: returns a
	/// client's subscriptions without removing them. Used to snapshot a session for
	/// persistence while it stays live in the trie.
	pub fn client_subscriptions(&self, client_id: &str) -> Vec<FlatSub> {
		let mut out = Vec::new();
		let mut segments: Vec<String> = Vec::new();
		Self::collect_client_rec(&self.root, client_id, &mut segments, &mut out);
		out
	}

	fn collect_client_rec(node: &Node, client_id: &str, segments: &mut Vec<String>, out: &mut Vec<FlatSub>) {
		for s in &node.subscribers {
			if s.client_id == client_id {
				out.push(FlatSub {
					filter: segments.join("/"),
					qos: s.qos,
					nolocal: s.nolocal,
					retain_as_published: s.retain_as_published,
					share_group: s.share_group.clone(),
					sub_id: s.sub_id,
				});
			}
		}
		for (seg, child) in node.children.iter() {
			segments.push(seg.to_string());
			Self::collect_client_rec(child, client_id, segments, out);
			segments.pop();
		}
	}

	fn take_client_rec(node: &mut Node, client_id: &str, segments: &mut Vec<String>, out: &mut Vec<FlatSub>) {
		node.subscribers.retain(|s| {
			if s.client_id == client_id {
				out.push(FlatSub {
					filter: segments.join("/"),
					qos: s.qos,
					nolocal: s.nolocal,
					retain_as_published: s.retain_as_published,
					share_group: s.share_group.clone(),
					sub_id: s.sub_id,
				});
				false
			} else {
				true
			}
		});
		for (seg, child) in node.children.iter_mut() {
			segments.push(seg.to_string());
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

	fn match_rec<'a>(node: &'a Node, segments: &[&str], idx: usize, dollar_top: bool, out: &mut Vec<&'a Subscription>) {
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

	/// Builds a `SubOptions` for the tests.
	fn opts(
		qos: QoS,
		nolocal: bool,
		retain_as_published: bool,
		share_group: Option<&str>,
		sub_id: Option<usize>,
	) -> SubOptions<'_> {
		SubOptions { qos, nolocal, retain_as_published, share_group, sub_id }
	}

	#[test]
	fn insert_reports_new_then_existing() {
		let mut trie = TopicTrie::default();
		assert!(trie.insert(
			"a/b",
			"c1",
			opts(QoS::AtLeastOnce, false, false, None, None)
		));
		// Re-subscribing the same client to the same filter is not new.
		assert!(!trie.insert(
			"a/b",
			"c1",
			opts(QoS::ExactlyOnce, false, false, None, None)
		));
		// A different client on the same filter is new.
		assert!(trie.insert("a/b", "c2", opts(QoS::AtMostOnce, false, false, None, None)));
	}

	#[test]
	fn options_are_stored_and_matched() {
		let mut trie = TopicTrie::default();
		trie.insert(
			"sensors/#",
			"c1",
			opts(QoS::AtLeastOnce, true, true, None, Some(7)),
		);
		let mut out = Vec::new();
		trie.matching("sensors/kitchen/temp", &mut out);
		assert_eq!(out.len(), 1);
		assert!(out[0].nolocal);
		assert!(out[0].retain_as_published);
		assert_eq!(out[0].sub_id, Some(7));
	}

	#[test]
	fn shared_and_regular_on_same_filter_coexist() {
		let mut trie = TopicTrie::default();
		// A regular sub and a shared sub from the same client on the same filter are
		// distinct entries (keyed by share group), so both match.
		assert!(trie.insert(
			"a/b",
			"c1",
			opts(QoS::AtLeastOnce, false, false, None, None)
		));
		assert!(trie.insert(
			"a/b",
			"c1",
			opts(QoS::AtLeastOnce, false, false, Some("g"), None)
		));
		let mut out = Vec::new();
		trie.matching("a/b", &mut out);
		assert_eq!(out.len(), 2);
		let groups: Vec<Option<&str>> = out.iter().map(|s| s.share_group.as_deref()).collect();
		assert!(groups.contains(&None));
		assert!(groups.contains(&Some("g")));

		// Removing the shared entry leaves the regular one intact.
		trie.remove("a/b", "c1", Some("g"));
		let mut out = Vec::new();
		trie.matching("a/b", &mut out);
		assert_eq!(out.len(), 1);
		assert_eq!(out[0].share_group, None);
	}

	#[test]
	fn resubscribe_replaces_options() {
		let mut trie = TopicTrie::default();
		trie.insert("t", "c1", opts(QoS::AtLeastOnce, true, true, None, None));
		trie.insert("t", "c1", opts(QoS::AtLeastOnce, false, false, None, None));
		let got = matches(&trie, "t");
		assert_eq!(got, vec![("c1".to_string(), false, false)]);
	}

	#[test]
	fn take_client_removes_and_returns_filters() {
		let mut trie = TopicTrie::default();
		trie.insert(
			"a/b",
			"c1",
			opts(QoS::AtLeastOnce, true, false, None, Some(5)),
		);
		trie.insert(
			"x/+/z",
			"c1",
			opts(QoS::ExactlyOnce, false, true, Some("g"), None),
		);
		trie.insert("a/b", "c2", opts(QoS::AtMostOnce, false, false, None, None));

		let mut taken = trie.take_client("c1");
		taken.sort_by(|a, b| a.filter.cmp(&b.filter));
		assert_eq!(taken.len(), 2);
		assert_eq!(taken[0].filter, "a/b");
		assert_eq!(taken[0].sub_id, Some(5));
		assert!(taken[0].nolocal);
		assert_eq!(taken[1].filter, "x/+/z");
		assert_eq!(taken[1].share_group, Some("g".to_string()));

		// c1 is gone from both filters; c2 remains on a/b.
		assert_eq!(
			matches(&trie, "a/b"),
			vec![("c2".to_string(), false, false)]
		);
		assert!(matches(&trie, "x/y/z").is_empty());
		// Taking a client with no subscriptions yields nothing.
		assert!(trie.take_client("c1").is_empty());
	}
}
