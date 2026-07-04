//! rusquitto — a thread-per-core MQTT 5 broker built on glommio.
//!
//! The crate is split into cohesive layers, each depending only on those beneath
//! it:
//!
//! - [`config`] — CLI parsing and the validated configuration tree.
//! - [`protocol`] — pure MQTT helpers (QoS, topic/filter validation).
//! - [`transport`] — the [`ByteStream`](transport::ByteStream) abstraction and its
//!   TCP, WebSocket, and TLS implementations (stackable into `wss://`).
//! - [`auth`] — authentication and per-topic authorization.
//! - [`broker`] — subscription routing, session lifecycle, and cross-shard mesh.
//! - [`server`] — the per-shard accept loop and the per-connection state machine.
//! - [`telemetry`] — logging and `$SYS` metrics.
//!
//! [`run`] wires them together into the running broker.

pub mod auth;
pub mod broker;
pub mod config;
pub mod persistence;
pub mod protocol;
pub mod server;
pub mod telemetry;
pub mod transport;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{CpuSet, LocalExecutorPoolBuilder, PoolPlacement};

use crate::broker::mesh::MeshMsg;
use crate::config::{Config, LogFormat, Placement};

/// Boots the broker from a validated [`Config`]: initialises logging, binds one
/// glommio executor per shard, and blocks until every shard's accept loop exits
/// (on SIGTERM/SIGINT). Returns once the pool has fully unwound.
pub fn run(config: Config) -> std::io::Result<()> {
	// Initialise logging first; keep `_log_guards` alive for the whole run so the
	// non-blocking background writers are not torn down early.
	let stdout_format = match config.logging.format {
		LogFormat::Pretty => telemetry::logging::Format::Pretty,
		LogFormat::Json => telemetry::logging::Format::Json,
	};
	let _log_guards = telemetry::logging::init(telemetry::logging::Config {
		dir: &config.logging.dir,
		log_file: &config.logging.file,
		error_file: &config.logging.error_file,
		default_filter: &config.logging.level,
		enable_terminal: config.logging.enable_terminal,
		stdout_format,
	})?;

	let all_cpus = CpuSet::online()?;
	let total_cores = all_cpus.len();
	let shards = config.resolved_shards(total_cores);

	// A request for more cores than exist is honoured by clamping down; warn so the
	// operator knows their config wasn't taken literally.
	if let Some(requested) = config.runtime.cores
		&& requested > total_cores
	{
		tracing::warn!(
			requested,
			total_cores,
			"runtime.cores exceeds online cores; using all online cores"
		);
	}

	let placement = match config.runtime.placement {
		Placement::MaxSpread => PoolPlacement::MaxSpread(shards, Some(all_cpus)),
		Placement::MaxPack => PoolPlacement::MaxPack(shards, Some(all_cpus)),
		Placement::Unbound => PoolPlacement::Unbound(shards),
	};

	tracing::info!(
		total_cores,
		shards,
		placement = ?config.runtime.placement,
		bind = %config.server.bind,
		port = config.server.port,
		websocket = ?config.server.websocket_port(),
		mqtts = ?config.tls.mqtts_port(),
		wss = ?config.tls.wss_port(),
		"starting rusquitto broker"
	);

	// Build the shared TLS config once, before spawning shards, so a bad
	// certificate/key fails startup immediately with a clear error rather than
	// per-shard. rustls `ServerConfig` is immutable and `Send + Sync`, so a single
	// `Arc` is shared read-only across every core (it holds no per-shard state).
	let tls_config = match (&config.tls.cert_file, &config.tls.key_file) {
		(Some(cert), Some(key)) if config.tls.enabled => Some(transport::tls::load_server_config(cert, key)?),
		_ => None,
	};

	// Full mesh connecting all shards, carrying forwarded publishes and cross-shard
	// session-control messages. Cloned into each shard, which then joins.
	let mesh: MeshBuilder<MeshMsg, Full> = MeshBuilder::full(shards, config.runtime.mesh_capacity);

	let config = Arc::new(config);

	// Shared shutdown flag flipped by SIGTERM/SIGINT. Each shard polls it and stops
	// accepting, so the executor pool unwinds and this function returns — letting the
	// log guards flush on the way out instead of dying mid-write.
	let shutdown = Arc::new(AtomicBool::new(false));
	for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
		signal_hook::flag::register(signal, Arc::clone(&shutdown))?;
	}

	// Cross-shard broker counters, published to `$SYS` by one shard. Sized to the
	// shard count so each shard has a slot for its per-shard load gauge.
	let metrics = Arc::new(telemetry::metrics::Metrics::with_shards(shards));

	LocalExecutorPoolBuilder::new(placement)
		.on_all_shards(move || {
			let mesh = mesh.clone();
			let config = Arc::clone(&config);
			let shutdown = Arc::clone(&shutdown);
			let metrics = Arc::clone(&metrics);
			let tls_config = tls_config.clone();
			async move { server::worker::init(mesh, config, shutdown, metrics, tls_config).await }
		})
		.expect("failed to spawn local executor")
		.join_all();

	tracing::info!("broker shut down");
	Ok(())
}
