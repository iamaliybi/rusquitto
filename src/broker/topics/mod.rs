//! Subscription indexing: the wildcard-aware topic trie and its supporting types.

mod interner;
mod trie;

pub use interner::SegmentInterner;
pub use trie::{filter_matches, FlatSub, SubOptions, Subscription, TopicTrie};
