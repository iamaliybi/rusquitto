mod auth;
mod broker;
mod config;
mod logger;
mod metrics;
mod net;
mod server;

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{CpuSet, LocalExecutorPoolBuilder, PoolPlacement};

use crate::broker::engine::MeshMsg;
use crate::config::{Cli, Config, LogFormat, Placement};

fn main() -> ExitCode {
	let cli = Cli::parse_args();

	let config = match Config::load(&cli.config) {
		Ok(config) => config,
		Err(e) => {
			eprintln!("rusquitto: {e}");
			return ExitCode::FAILURE;
		}
	};

	match run(config) {
		Ok(()) => ExitCode::SUCCESS,
		Err(e) => {
			eprintln!("rusquitto: fatal: {e}");
			ExitCode::FAILURE
		}
	}
}

fn run(config: Config) -> std::io::Result<()> {
	// Initialise logging first; keep `_log_guards` alive for the whole run so the
	// non-blocking background writers are not torn down early.
	let stdout_format = match config.logging.format {
		LogFormat::Pretty => logger::Format::Pretty,
		LogFormat::Json => logger::Format::Json,
	};
	let _log_guards = logger::init(logger::Config {
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

	// A request for more cores than exist is honoured by clamping down to what is
	// available; warn so the operator knows their config wasn't taken literally.
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
		"starting rusquitto broker"
	);

	// Full mesh connecting all shards, carrying forwarded publishes and cross-shard
	// session-control messages. Cloned into each shard, which then joins.
	let mesh: MeshBuilder<MeshMsg, Full> =
		MeshBuilder::full(shards, config.runtime.mesh_capacity);

	// Shared, read-only config handed to every shard.
	let config = Arc::new(config);

	// Shared shutdown flag flipped by SIGTERM/SIGINT. Each shard polls it and
	// stops accepting, so the executor pool unwinds and this function returns —
	// letting the log guards flush on the way out instead of dying mid-write.
	let shutdown = Arc::new(AtomicBool::new(false));
	for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
		signal_hook::flag::register(signal, Arc::clone(&shutdown))?;
	}

	// Cross-shard broker counters, published to `$SYS` by one shard.
	let metrics = Arc::new(metrics::Metrics::default());

	LocalExecutorPoolBuilder::new(placement)
		.on_all_shards(move || {
			let mesh = mesh.clone();
			let config = Arc::clone(&config);
			let shutdown = Arc::clone(&shutdown);
			let metrics = Arc::clone(&metrics);
			async move { server::worker::init(mesh, config, shutdown, metrics).await }
		})
		.expect("failed to spawn local executor")
		.join_all();

	tracing::info!("broker shut down");
	Ok(())
}
