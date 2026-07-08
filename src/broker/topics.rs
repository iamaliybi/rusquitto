//! Subscription indexing: the wildcard-aware topic trie and its supporting types.

mod interner;
mod trie;

pub use interner::SegmentInterner;
pub use trie::{FlatSub, SubOptions, Subscription, TopicTrie, filter_matches, filter_subsumes};
