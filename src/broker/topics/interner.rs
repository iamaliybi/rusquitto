//! Per-shard string interner for topic segments.
//!
//! MQTT topic filters share segments heavily — `a` recurs across `a/b`, `a/c`,
//! `x/a`, and so on. Storing each occurrence as its own `String` wastes memory
//! and allocator churn. The interner hands out a single shared `Rc<str>` per
//! distinct segment, so the trie keys every level by a pointer-cheap handle and
//! identical segments across the whole tree share one allocation.
//!
//! It is shard-local (`Rc`, never crosses a core), so no synchronisation is
//! needed. Segments are never removed individually — the working set of topic
//! names in a broker is effectively bounded — but dropping the trie drops the
//! interner and reclaims everything.

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
