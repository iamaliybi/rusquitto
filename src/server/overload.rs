//! Per-shard overload detection.
//!
//! The signal is *scheduling delay*: how long a normal-priority task waits beyond
//! its due time because the reactor is busy. A probe task (in [`worker`](super::worker))
//! sleeps a fixed interval and measures the oversleep; on an idle core that is
//! near zero, on a saturated core it grows. [`LoadMonitor`] smooths those samples
//! into an EWMA so decisions react to *sustained* pressure, not a single spike.
//!
//! It is shard-local (`Rc`, never shared across cores) and feeds three levers:
//! the stall WARN, admission control, and load shedding.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

/// EWMA smoothing factor (0..1): the weight of each new sample. At the 100ms probe
/// interval this reaches ~90% of a step change in about a second.
const EWMA_ALPHA: f64 = 0.25;

#[derive(Default)]
pub struct LoadMonitor {
	/// Smoothed scheduling delay, in microseconds.
	delay_us: Cell<f64>,
}

impl LoadMonitor {
	pub fn new() -> Rc<Self> {
		Rc::new(Self::default())
	}

	/// Feeds one scheduling-delay sample into the EWMA.
	pub fn record(&self, sample: Duration) {
		let sample_us = sample.as_micros() as f64;
		let prev = self.delay_us.get();
		self.delay_us
			.set(prev * (1.0 - EWMA_ALPHA) + sample_us * EWMA_ALPHA);
	}

	/// The current smoothed scheduling delay.
	pub fn scheduling_delay(&self) -> Duration {
		Duration::from_micros(self.delay_us.get() as u64)
	}

	/// Whether the smoothed delay is at or above `threshold`. A zero threshold means
	/// "disabled" and never trips.
	pub fn exceeds(&self, threshold: Duration) -> bool {
		!threshold.is_zero() && self.scheduling_delay() >= threshold
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ewma_moves_toward_samples_but_smooths_a_single_spike() {
		let m = LoadMonitor::default();
		// A lone 100ms spike from rest lifts the average only partially (by alpha).
		m.record(Duration::from_millis(100));
		let after_one = m.scheduling_delay();
		assert!(
			after_one >= Duration::from_millis(20) && after_one <= Duration::from_millis(30),
			"one spike is smoothed, got {after_one:?}"
		);

		// Sustained high samples converge toward the true value.
		for _ in 0..50 {
			m.record(Duration::from_millis(100));
		}
		let converged = m.scheduling_delay();
		assert!(
			converged >= Duration::from_millis(95),
			"converges, got {converged:?}"
		);
	}

	#[test]
	fn exceeds_treats_zero_threshold_as_disabled() {
		let m = LoadMonitor::default();
		for _ in 0..50 {
			m.record(Duration::from_millis(500));
		}
		assert!(!m.exceeds(Duration::ZERO), "zero threshold never trips");
		assert!(m.exceeds(Duration::from_millis(100)));
		assert!(!m.exceeds(Duration::from_secs(10)));
	}

	#[test]
	fn idle_reactor_reads_near_zero() {
		let m = LoadMonitor::default();
		for _ in 0..20 {
			m.record(Duration::from_micros(50));
		}
		assert!(m.scheduling_delay() < Duration::from_millis(1));
	}
}
