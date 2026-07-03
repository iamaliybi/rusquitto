//! Broker metrics published under the `$SYS/` topic hierarchy.
//!
//! A single [`Metrics`] instance is shared across every shard via `Arc` and
//! updated with relaxed atomics on the hot path (connect/disconnect, each
//! PUBLISH in and out). One shard periodically snapshots it and publishes the
//! values as retained `$SYS/broker/...` messages (see `server::worker`), so any
//! client subscribed to `$SYS/#` can monitor the broker over MQTT itself.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Cross-shard broker counters. All fields use relaxed ordering: they are
/// monotonic (or a balanced inc/dec pair) and only ever read for reporting, so
/// no synchronisation beyond atomicity is required.
pub struct Metrics {
	start: Instant,
	/// Currently connected clients (incremented at CONNECT, decremented on close).
	clients_connected: AtomicU64,
	/// Cumulative successful connections since start.
	clients_total: AtomicU64,
	/// PUBLISH packets received from clients.
	messages_received: AtomicU64,
	/// PUBLISH packets sent to clients.
	messages_sent: AtomicU64,
	/// Publish payload bytes received from clients.
	bytes_received: AtomicU64,
	/// Publish payload bytes sent to clients.
	bytes_sent: AtomicU64,
}

impl Default for Metrics {
	fn default() -> Self {
		Self {
			start: Instant::now(),
			clients_connected: AtomicU64::new(0),
			clients_total: AtomicU64::new(0),
			messages_received: AtomicU64::new(0),
			messages_sent: AtomicU64::new(0),
			bytes_received: AtomicU64::new(0),
			bytes_sent: AtomicU64::new(0),
		}
	}
}

impl Metrics {
	/// Records a newly connected client (after a successful CONNECT).
	pub fn client_connected(&self) {
		self.clients_connected.fetch_add(1, Ordering::Relaxed);
		self.clients_total.fetch_add(1, Ordering::Relaxed);
	}

	/// Records a client disconnecting. Pairs with [`client_connected`].
	pub fn client_disconnected(&self) {
		self.clients_connected.fetch_sub(1, Ordering::Relaxed);
	}

	/// Records a PUBLISH received from a client, with its payload size.
	pub fn message_received(&self, payload_len: usize) {
		self.messages_received.fetch_add(1, Ordering::Relaxed);
		self.bytes_received
			.fetch_add(payload_len as u64, Ordering::Relaxed);
	}

	/// Records a PUBLISH sent to a client, with its payload size.
	pub fn message_sent(&self, payload_len: usize) {
		self.messages_sent.fetch_add(1, Ordering::Relaxed);
		self.bytes_sent
			.fetch_add(payload_len as u64, Ordering::Relaxed);
	}

	/// Snapshots the counters for publishing to `$SYS`.
	pub fn snapshot(&self) -> MetricsSnapshot {
		MetricsSnapshot {
			uptime_secs: self.start.elapsed().as_secs(),
			clients_connected: self.clients_connected.load(Ordering::Relaxed),
			clients_total: self.clients_total.load(Ordering::Relaxed),
			messages_received: self.messages_received.load(Ordering::Relaxed),
			messages_sent: self.messages_sent.load(Ordering::Relaxed),
			bytes_received: self.bytes_received.load(Ordering::Relaxed),
			bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
		}
	}
}

/// A point-in-time view of [`Metrics`], rendered into `$SYS` topic/value pairs.
pub struct MetricsSnapshot {
	pub uptime_secs: u64,
	pub clients_connected: u64,
	pub clients_total: u64,
	pub messages_received: u64,
	pub messages_sent: u64,
	pub bytes_received: u64,
	pub bytes_sent: u64,
}

impl MetricsSnapshot {
	/// The `$SYS/broker/...` topic/value pairs to publish for this snapshot.
	pub fn topics(&self) -> Vec<(&'static str, String)> {
		vec![
			(
				"$SYS/broker/version",
				concat!("rusquitto ", env!("CARGO_PKG_VERSION")).to_string(),
			),
			(
				"$SYS/broker/uptime",
				format!("{} seconds", self.uptime_secs),
			),
			(
				"$SYS/broker/clients/connected",
				self.clients_connected.to_string(),
			),
			("$SYS/broker/clients/total", self.clients_total.to_string()),
			(
				"$SYS/broker/messages/received",
				self.messages_received.to_string(),
			),
			("$SYS/broker/messages/sent", self.messages_sent.to_string()),
			(
				"$SYS/broker/bytes/received",
				self.bytes_received.to_string(),
			),
			("$SYS/broker/bytes/sent", self.bytes_sent.to_string()),
		]
	}
}
