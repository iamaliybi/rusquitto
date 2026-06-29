mod broker;
mod logger;
mod net;
mod server;

use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{CpuSet, LocalExecutorPoolBuilder, PoolPlacement};
use mqttbytes::v5::Publish;
use std::{cmp, io::Result, path::Path};

/// Per-link capacity of the inter-shard channel mesh (messages buffered before
/// `try_send_to` drops).
const MESH_CHANNEL_SIZE: usize = 1024;

fn main() -> Result<()> {
	// Initialise logging first; keep `_log_guards` alive for the whole run so the
	// non-blocking background writers are not torn down early.
	let stdout_format = if cfg!(debug_assertions) {
		logger::Format::Pretty
	} else {
		logger::Format::Json
	};
	let _log_guards = logger::init(logger::Config {
		dir: Path::new("logs"),
		default_filter: "info,rusquitto=debug",
		stdout_format,
	})?;

	let all_cpus = CpuSet::online()?;
	let total_cores = all_cpus.len();

	let glommio_cores = cmp::max((total_cores * 3) / 4, 1);
	tracing::info!(
		total_cores,
		shards = glommio_cores,
		"starting rusquitto broker"
	);
	let placement = PoolPlacement::MaxSpread(glommio_cores, Some(all_cpus));

	// Full mesh connecting all shards. Cloned into each shard, which then joins.
	let mesh: MeshBuilder<Publish, Full> = MeshBuilder::full(glommio_cores, MESH_CHANNEL_SIZE);

	LocalExecutorPoolBuilder::new(placement)
		.on_all_shards(move || {
			let mesh = mesh.clone();
			async move { server::worker::init(mesh).await }
		})
		.expect("failed to spawn local executor")
		.join_all();

	Ok(())
}
