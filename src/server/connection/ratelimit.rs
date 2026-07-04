//! A per-connection token-bucket rate limiter.
//!
//! In the thread-per-core model a connection is pinned to the core that accepted
//! it, so a single client publishing at a huge rate can drive *that* core toward
//! saturation (each PUBLISH costs a trie match plus fan-out to every subscriber).
//! This bucket bounds that: a connection may burst up to one second's worth of
//! messages, then is paced to the configured sustained rate. Over-budget publishes
//! are *throttled* (the caller sleeps for the returned delay) rather than dropped,
//! which applies TCP backpressure to the noisy client while yielding the core to
//! its neighbours.
//!
//! The clock is passed in explicitly so the bucket is deterministic under test.

use std::time::{Duration, Instant};

pub(super) struct TokenBucket {
	/// Available tokens; may go transiently negative while a reserved token is
	/// waiting to be earned back.
	tokens: f64,
	/// Burst ceiling — one second's worth of tokens.
	capacity: f64,
	/// Refill rate, tokens (messages) per second.
	rate: f64,
	/// When `tokens` was last refilled.
	last: Instant,
}

impl TokenBucket {
	/// A bucket allowing `rate` messages per second with a burst capacity of one
	/// second's worth (at least 1). `rate` must be non-zero (callers only build a
	/// bucket when the limit is enabled).
	pub(super) fn per_second(rate: u32, now: Instant) -> Self {
		let rate = f64::from(rate.max(1));
		Self { tokens: rate, capacity: rate, rate, last: now }
	}

	/// Reserves one token, returning how long the caller must wait before it is
	/// available. `Duration::ZERO` means proceed immediately. Refills for elapsed
	/// time first, so calling it drives the bucket forward.
	pub(super) fn acquire(&mut self, now: Instant) -> Duration {
		let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
		self.last = now;
		self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
		self.tokens -= 1.0;
		if self.tokens >= 0.0 {
			Duration::ZERO
		} else {
			// Time for the deficit to refill back to zero.
			Duration::from_secs_f64(-self.tokens / self.rate)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn allows_a_burst_up_to_capacity_then_throttles() {
		let t0 = Instant::now();
		let mut bucket = TokenBucket::per_second(5, t0);

		// Five immediate messages fit the burst; the sixth (same instant) waits.
		for _ in 0..5 {
			assert_eq!(bucket.acquire(t0), Duration::ZERO);
		}
		let wait = bucket.acquire(t0);
		assert!(wait > Duration::ZERO, "sixth message is throttled");
		// One token at 5/s takes ~200ms.
		assert!((wait.as_secs_f64() - 0.2).abs() < 1e-6, "wait was {wait:?}");
	}

	#[test]
	fn refills_over_time() {
		let t0 = Instant::now();
		let mut bucket = TokenBucket::per_second(10, t0);
		// Drain exactly the burst (no over-draw, so no reserved-token debt).
		for _ in 0..10 {
			assert_eq!(bucket.acquire(t0), Duration::ZERO);
		}
		// One second later the bucket has refilled to capacity again.
		let t1 = t0 + Duration::from_secs(1);
		for _ in 0..10 {
			assert_eq!(bucket.acquire(t1), Duration::ZERO, "refilled after 1s");
		}
	}

	#[test]
	fn steady_state_paces_to_the_rate() {
		let t0 = Instant::now();
		let mut bucket = TokenBucket::per_second(4, t0);
		for _ in 0..4 {
			bucket.acquire(t0); // exhaust burst
		}
		// After earning exactly one token's worth of time, one message passes free.
		let quarter = t0 + Duration::from_millis(250);
		assert_eq!(bucket.acquire(quarter), Duration::ZERO);
		// A second message at the same instant must wait ~250ms (1 / 4 per second).
		let wait = bucket.acquire(quarter);
		assert!(
			(wait.as_secs_f64() - 0.25).abs() < 1e-6,
			"wait was {wait:?}"
		);
	}

	#[test]
	fn capacity_does_not_accumulate_beyond_one_second() {
		let t0 = Instant::now();
		let mut bucket = TokenBucket::per_second(3, t0);
		// Idle for a long time: tokens are capped at capacity, not hoarded.
		let later = t0 + Duration::from_secs(60);
		for _ in 0..3 {
			assert_eq!(bucket.acquire(later), Duration::ZERO);
		}
		assert!(
			bucket.acquire(later) > Duration::ZERO,
			"no hoarding past capacity"
		);
	}
}
