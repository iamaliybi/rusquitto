//! Pure MQTT helpers with no broker state.
//!
//! Small, side-effect-free functions shared across the broker and connection
//! layers: QoS arithmetic, topic/filter validation, and shared-subscription
//! parsing. Keeping them here (rather than on `Connection` or `ShardState`) makes
//! them trivially unit-testable and reusable.

use mqttbytes::{QoS, v5 as mqtt_v5};

/// Maximum number of levels (`/`-separated segments) allowed in a topic or filter.
///
/// A hard depth bound is a safety limit, not just a style rule: the subscription
/// trie is walked recursively (one stack frame per level in
/// [`TopicTrie::matching`](crate::broker::topics::TopicTrie)), and each SUBSCRIBE
/// level allocates a trie node. Without this cap a single ~64 KiB filter of the
/// form `a/a/a/…` yields ~32 000 levels — enough to overflow the executor stack on
/// a matching PUBLISH (an uncatchable abort) and to balloon trie memory. 128 is far
/// beyond any real topic hierarchy.
pub const MAX_TOPIC_LEVELS: usize = 128;

/// The lower of two QoS levels — used both for the granted QoS
/// (`min(requested, server max)`) and per-subscriber delivery
/// (`min(publish, granted)`).
pub fn min_qos(a: QoS, b: QoS) -> QoS {
	if (a as u8) <= (b as u8) {
		a
	} else {
		b
	}
}

/// Maps a granted QoS to its SubAck success reason code.
pub fn sub_reason_code(qos: QoS) -> mqtt_v5::SubscribeReasonCode {
	match qos {
		QoS::AtMostOnce => mqtt_v5::SubscribeReasonCode::QoS0,
		QoS::AtLeastOnce => mqtt_v5::SubscribeReasonCode::QoS1,
		QoS::ExactlyOnce => mqtt_v5::SubscribeReasonCode::QoS2,
	}
}

/// Splits a subscription filter into `(effective_filter, share_group)`.
///
/// A shared subscription is `$share/{ShareName}/{topic-filter}`: the group is
/// `ShareName` and the effective filter is `{topic-filter}`. An ordinary filter
/// returns itself with `None`. A malformed `$share/…` (missing/empty ShareName or
/// topic, or a wildcard in the ShareName) is `Err(())`.
#[allow(clippy::result_unit_err)]
pub fn parse_shared_filter(filter: &str) -> Result<(&str, Option<&str>), ()> {
	let Some(rest) = filter.strip_prefix("$share/") else {
		return Ok((filter, None));
	};
	match rest.split_once('/') {
		Some((group, topic))
			if !group.is_empty() && !topic.is_empty() && !group.contains('+') && !group.contains('#') =>
		{
			Ok((topic, Some(group)))
		}
		_ => Err(()),
	}
}

/// Whether a concrete publish `topic` is well-formed: non-empty, no wildcards, no
/// embedded NUL, and within the MQTT length limit. Clients must not publish to
/// `$`-prefixed topics (reserved for the broker), so those are rejected too.
pub fn valid_publish_topic(topic: &str) -> bool {
	!topic.is_empty()
		&& topic.len() <= u16::MAX as usize
		&& topic.split('/').count() <= MAX_TOPIC_LEVELS
		&& !topic.starts_with('$')
		&& !topic.contains(['+', '#', '\0'])
}

/// Whether a subscription `filter` is syntactically valid per MQTT: non-empty, no
/// NUL, each `+` occupies a whole level, and `#` is the final level only.
pub fn valid_subscribe_filter(filter: &str) -> bool {
	if filter.is_empty()
		|| filter.len() > u16::MAX as usize
		|| filter.contains('\0')
		|| filter.split('/').count() > MAX_TOPIC_LEVELS
	{
		return false;
	}
	let mut levels = filter.split('/').peekable();
	while let Some(level) = levels.next() {
		let last = levels.peek().is_none();
		match level {
			"#" if !last => return false,
			_ if level.len() > 1 && level.contains(['+', '#']) => return false,
			_ => {}
		}
	}
	true
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn qos_min() {
		assert_eq!(
			min_qos(QoS::ExactlyOnce, QoS::AtLeastOnce),
			QoS::AtLeastOnce
		);
		assert_eq!(min_qos(QoS::AtMostOnce, QoS::ExactlyOnce), QoS::AtMostOnce);
	}

	#[test]
	fn shared_filter_parsing() {
		assert_eq!(parse_shared_filter("a/b"), Ok(("a/b", None)));
		assert_eq!(parse_shared_filter("$share/g/a/b"), Ok(("a/b", Some("g"))));
		assert!(parse_shared_filter("$share/g/").is_err());
		assert!(parse_shared_filter("$share//a").is_err());
		assert!(parse_shared_filter("$share/g+/a").is_err());
	}

	#[test]
	fn publish_topic_rules() {
		assert!(valid_publish_topic("a/b/c"));
		assert!(!valid_publish_topic(""));
		assert!(!valid_publish_topic("a/+/c"));
		assert!(!valid_publish_topic("a/#"));
		assert!(!valid_publish_topic("$SYS/broker/x"));
		assert!(!valid_publish_topic("a\0b"));
	}

	#[test]
	fn subscribe_filter_rules() {
		assert!(valid_subscribe_filter("a/+/c"));
		assert!(valid_subscribe_filter("a/#"));
		assert!(valid_subscribe_filter("#"));
		assert!(!valid_subscribe_filter("a/#/c"));
		assert!(!valid_subscribe_filter("a/b+/c"));
		assert!(!valid_subscribe_filter(""));
	}

	#[test]
	fn rejects_excessive_topic_depth() {
		// A filter/topic at the cap is fine; one level deeper is rejected — this is
		// what bounds trie recursion depth and node growth.
		let ok = vec!["a"; MAX_TOPIC_LEVELS].join("/");
		let deep = vec!["a"; MAX_TOPIC_LEVELS + 1].join("/");
		assert!(valid_subscribe_filter(&ok));
		assert!(valid_publish_topic(&ok));
		assert!(!valid_subscribe_filter(&deep));
		assert!(!valid_publish_topic(&deep));
	}
}
