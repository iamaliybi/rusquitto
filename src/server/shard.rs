//! The per-shard runtime: everything that happens on one pinned core.
//!
//! Each shard is one glommio `LocalExecutor`, on one OS thread, pinned to one
//! core. This module owns that lifecycle — join the channel mesh, bind the
//! shard's `SO_REUSEPORT` listeners, build the shard-local
//! [`ShardState`](crate::broker::shard::ShardState), restore persisted state,
//! spawn the background housekeeping, run the accept loop, and drain cleanly on
//! shutdown. The work is split by concern across sibling modules:
//!
//! - [`accept`] — the accept loop, per-shard connection accounting, and
//!   admission control.
//! - [`serve`] — building the transport stack (TCP / WS / TLS / WSS) for one
//!   accepted socket and driving its MQTT session.
//! - [`maintenance`] — persistence restore/snapshot, the inbound-mesh drain, the
//!   load probe, the session-expiry sweep, and load shedding.
//!
//! This is the shard's *runtime*; its *data* lives in
//! [`broker::shard::ShardState`](crate::broker::shard::ShardState) — two facets
//! of one shard, kept in separate modules on purpose.

mod accept;
mod maintenance;
mod serve;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use futures_rustls::TlsAcceptor;
use futures_rustls::rustls::ServerConfig;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{Latency, Shares};

use crate::auth::Authenticator;
use crate::broker::messages::MeshMsg;
use crate::broker::shard::ShardState;
use crate::config::{Config, LimitsConfig};
use crate::server::overload::LoadMonitor;
use crate::telemetry::metrics::Metrics;

use accept::{ConnCounts, Listeners};

/// How often each shard reclaims suspended sessions past their expiry deadline.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Ask glibc to return freed arena pages to the kernel once per this many
/// session sweeps (i.e. every 30 s with the 1 s sweep interval). Cheap when
/// there is nothing to release; bounds post-burst RSS retention.
const MALLOC_TRIM_EVERY: u32 = 30;

/// How often the accept loop wakes to check the shutdown flag while `accept()` is
/// otherwise blocked. Bounds shutdown latency; coarse so it barely touches the hot path.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Longest a shard waits for its connections to drain during shutdown.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// How often the drain loop re-checks the live-connection count.
const SHUTDOWN_DRAIN_POLL: Duration = Duration::from_millis(25);

/// How often the load probe samples the reactor's scheduling delay. Short enough
/// to react within a second, long enough to be negligible overhead.
const LOAD_PROBE_INTERVAL: Duration = Duration::from_millis(100);

/// How often the load-shedding task re-evaluates whether to shed connections.
const SHED_INTERVAL: Duration = Duration::from_secs(1);

/// CPU shares for the background maintenance task queue (sweep, `$SYS`, shedding),
/// relative to the default queue's 1000. Low, so housekeeping yields to
/// client-serving work when a shard is busy — the scheduling-group idea from
/// Seastar/ScyllaDB, where background work is starved to protect foreground latency.
const MAINTENANCE_SHARES: usize = 200;

/// Which listener an accepted socket arrived on, deciding the transport stack the
/// MQTT session runs over.
#[derive(Clone, Copy)]
enum Transport {
	/// Plain MQTT over TCP (`mqtt://`).
	Plain,
	/// MQTT over WebSocket (`ws://`).
	WebSocket,
	/// MQTT over TLS (`mqtts://`).
	Mqtts,
	/// MQTT over WebSocket over TLS (`wss://`).
	Wss,
}

impl Transport {
	/// `(is_tls, is_websocket)` for connection-span fields.
	fn flags(self) -> (bool, bool) {
		match self {
			Transport::Plain => (false, false),
			Transport::WebSocket => (false, true),
			Transport::Mqtts => (true, false),
			Transport::Wss => (true, true),
		}
	}
}

/// The clonable per-connection context — the shard-local and cross-shard handles
/// every accepted connection needs. Bundling them collapses what were seven
/// positional arguments threaded through `serve`/`run_stream`/`boxed_*` into one,
/// and each clone is cheap (`Rc`/`Arc` refcount bumps, a `Copy` config, a
/// `usize`). The `Rc` vs `Arc` split is the thread-per-core signal: `shard`/`auth`
/// are shard-local (`Rc`), `metrics`/`shutdown` are the only broker-wide shared
/// state (`Arc`).
#[derive(Clone)]
struct ConnCtx {
	shard_id: usize,
	shard: Rc<RefCell<ShardState>>,
	limits: LimitsConfig,
	auth: Rc<Authenticator>,
	metrics: Arc<Metrics>,
	shutdown: Arc<AtomicBool>,
	/// Whether to map a verified client certificate's CN onto the MQTT username
	/// (`[tls] cert_cn_as_username`), decided once at startup.
	map_cert_cn: bool,
}

/// Runs one shard to completion: the body of every `LocalExecutor` in the pool.
///
/// Returns when the shutdown flag is set and this shard's connections have
/// drained — which lets the executor pool unwind and `lib::run` return, flushing
/// the log guards cleanly.
pub async fn run_shard(
	mesh: MeshBuilder<MeshMsg, Full>,
	config: Arc<Config>,
	shutdown: Arc<AtomicBool>,
	metrics: Arc<Metrics>,
	tls_config: Option<Arc<ServerConfig>>,
) {
	let shard_id: usize = glommio::executor().id();

	// Join the full mesh. This rendezvous blocks until every shard has joined.
	let (senders, receivers) = match mesh.join().await {
		Ok(pair) => pair,
		Err(_) => {
			tracing::error!(shard = shard_id, "failed to join the channel mesh");
			return;
		}
	};

	// Bind this shard's SO_REUSEPORT listeners (TCP always; WS/mqtts/wss opt-in).
	let Some(listeners) = Listeners::bind(&config, shard_id) else {
		return; // a configured-but-unbindable port is a fatal misconfiguration
	};
	// Shard-local, swappable acceptor: the reload task replaces it in place when the
	// certificate files change, so a rotated cert reaches new handshakes without a
	// restart. Single-threaded `RefCell` — no cross-core sharing, no lock.
	let tls_acceptor = Rc::new(RefCell::new(tls_config.map(TlsAcceptor::from)));

	// Mesh peer id is 0-based and unique per shard (glommio executor ids are
	// 1-based). Peer 0 owns broker-wide duties: `$SYS`, the retained snapshot.
	let mesh_peer_id = senders.peer_id();
	let shard_count = senders.nr_consumers();

	// A low-share, latency-insensitive task queue for background housekeeping
	// (session sweep, `$SYS`, shedding). Under load the scheduler starves it in
	// favour of the default queue that serves clients — the scheduling-group
	// pattern from Seastar/ScyllaDB.
	let tq_maintenance = glommio::executor().create_task_queue(
		Shares::Static(MAINTENANCE_SHARES),
		Latency::NotImportant,
		"maintenance",
	);

	// Per-shard load signal (reactor scheduling delay), driving the stall WARN,
	// admission control, and shedding.
	let load = LoadMonitor::new();

	// Shard-local broker state, shared by `Rc` between every connection on this shard.
	let state = ShardState::new();
	{
		let mut s = state.borrow_mut();
		s.set_mesh(senders);
		s.set_retained_limit(config.limits.max_retained_messages);
	}

	// Restore retained messages and this shard's sessions before accepting, then
	// spawn the inbound-mesh drain and all periodic housekeeping tasks.
	let ids = maintenance::ShardIds { executor: shard_id, peer: mesh_peer_id };
	maintenance::restore_from_disk(&state, &config, ids, shard_count).await;
	maintenance::spawn_background(
		&state,
		&config,
		&metrics,
		&load,
		ids,
		tq_maintenance,
		receivers,
	);
	maintenance::spawn_tls_reload(&tls_acceptor, &config, tq_maintenance, shard_id);

	tracing::info!(
		shard = shard_id,
		websocket = config.server.websocket,
		mqtts = listeners.mqtts.is_some(),
		wss = listeners.wss.is_some(),
		"shard ready, accepting connections"
	);

	// Shard-local credential store, shared by every connection on this shard.
	let auth = Rc::new(Authenticator::from_config(&config.auth));
	if mesh_peer_id == 0 {
		tracing::info!(
			enforced = !auth.is_open(),
			users = config.auth.users.len(),
			allow_anonymous = config.auth.allow_anonymous,
			"authentication configured"
		);
		// Warn if idle-connection protection is off: with no server keep-alive and a
		// client that also sends keep-alive 0, an idle/stalled connection is never reaped.
		if config.limits.keep_alive == 0 {
			tracing::warn!(
				"limits.keep_alive = 0 disables the server keep-alive override; idle \
				 connections are only reaped when the client sets its own keep-alive. \
				 Set keep_alive > 0 to guarantee idle/slow connections are dropped."
			);
		}
	}

	let ctx = ConnCtx {
		shard_id,
		shard: state.clone(),
		limits: config.limits,
		auth,
		metrics: metrics.clone(),
		shutdown: shutdown.clone(),
		map_cert_cn: config.tls.cert_cn_as_username,
	};
	let counts = Rc::new(ConnCounts::default());

	// The accept loop runs until the shutdown flag is set.
	accept::accept_loop(&ctx, &listeners, &tls_acceptor, &load, &config, &counts).await;

	// Drain: wake every live connection so it sends DISCONNECT and cleans up, then
	// wait (bounded) for them to finish before returning.
	tracing::info!(
		shard = shard_id,
		connections = counts.live(),
		"shutdown signal received, draining connections"
	);
	state.borrow_mut().shutdown_connections();

	let deadline = Instant::now() + SHUTDOWN_GRACE;
	while counts.live() > 0 && Instant::now() < deadline {
		glommio::timer::sleep(SHUTDOWN_DRAIN_POLL).await;
	}

	// Final snapshots on graceful shutdown, capturing anything since the last periodic
	// write so a clean stop loses nothing. By now every connection has drained and its
	// durable session is suspended in the shard state, so this captures them all.
	maintenance::write_final_snapshots(&state, &config, shard_id, mesh_peer_id).await;

	tracing::info!(shard = shard_id, remaining = counts.live(), "shard stopped");
}
