mod net;
mod server;
mod broker;

use glommio::{CpuSet, LocalExecutorPoolBuilder, PoolPlacement};
use std::{cmp, io::Result};

fn main() -> Result<()> {
	let all_cpus = CpuSet::online()?;
	let total_cores = all_cpus.len();

	let glommio_cores = cmp::max((total_cores * 3) / 4, 1);
	let placement = PoolPlacement::MaxSpread(glommio_cores, Some(all_cpus));

	LocalExecutorPoolBuilder::new(placement)
		.on_all_shards(move || async move { server::worker::init().await })
		.expect("failed to spawn local executor")
		.join_all();

	Ok(())
}
