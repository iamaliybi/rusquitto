//! Per-shard string interner for topic segments.
//!
//! MQTT topic filters share segments heavily — `a` recurs across `a/b`, `a/c`,
//! `x/a`, and so on. Storing each occurrence as its own `String` wastes memory
//! and allocator churn. The interner hands out a single shared `Rc<str>` per
//! distinct segment, so the trie keys every level by a pointer-cheap handle and
//! identical segments across the whole tree share one allocation.
//!
//! It is shard-local (`Rc`, never crosses a core), so no synchronisation is
//! needed. Individual segments are not removed on unsubscribe (that hot path
//! stays allocation-free), but a periodic [`retain_live`](SegmentInterner::retain_live)
//! sweep reclaims any segment no longer referenced by a trie node, so a broker
//! whose topic namespace churns (e.g. per-client-id filters) does not grow the
//! interner without bound. Dropping the trie drops the interner entirely.

use std::collections::HashSet;
use std::rc::Rc;

/// Interns topic segments into shared `Rc<str>` handles.
#[derive(Default)]
pub struct SegmentInterner {
	seen: HashSet<Rc<str>>,
}

impl SegmentInterner {
	/// Returns the shared handle for `segment`, allocating it once on first sight.
	pub fn intern(&mut self, segment: &str) -> Rc<str> {
		if let Some(existing) = self.seen.get(segment) {
			return existing.clone();
		}
		let handle: Rc<str> = Rc::from(segment);
		self.seen.insert(handle.clone());
		handle
	}

	/// Evicts every segment the interner is the *sole* owner of — i.e. no trie
	/// node keys on it any more. A live segment is referenced by at least one
	/// `HashMap<Rc<str>, Node>` key in the trie *and* the interner's own set, so
	/// its strong count is ≥ 2; a dead one dropped by node pruning is back to 1.
	/// O(distinct segments); call periodically, not per-unsubscribe.
	pub fn retain_live(&mut self) {
		self.seen.retain(|handle| Rc::strong_count(handle) > 1);
	}

	/// Number of distinct interned segments (for diagnostics/tests).
	#[cfg(test)]
	pub fn distinct(&self) -> usize {
		self.seen.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn identical_segments_share_one_allocation() {
		let mut interner = SegmentInterner::default();
		let a1 = interner.intern("sensors");
		let a2 = interner.intern("sensors");
		let b = interner.intern("actuators");

		assert!(Rc::ptr_eq(&a1, &a2), "same segment returns the same Rc");
		assert!(!Rc::ptr_eq(&a1, &b));
		assert_eq!(interner.distinct(), 2, "one entry per distinct segment");
	}
}
