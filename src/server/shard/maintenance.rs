//! Per-shard background housekeeping: persistence restore/snapshot, the
//! inbound-mesh drain, the reactor load probe, the session-expiry sweep, and
//! load shedding.
//!
//! [`restore_from_disk`] and [`write_final_snapshots`] run inline in the shard's
//! startup/shutdown; [`spawn_background`] launches the periodic tasks, most onto
//! the low-priority maintenance queue so they yield to client-serving work.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use glommio::TaskQueueHandle;
use glommio::channels::channel_mesh::Receivers;
use mqttbytes::{QoS, v5::Publish};

use super::{LOAD_PROBE_INTERVAL, MALLOC_TRIM_EVERY, SESSION_SWEEP_INTERVAL, SHED_INTERVAL};
use crate::broker::messages::MeshMsg;
use crate::broker::shard::ShardState;
use crate::config::Config;
use crate::persistence;
use crate::server::overload::LoadMonitor;
use crate::telemetry::metrics::Metrics;

/// A shard-local handle to the broker state, as shared between the background
/// tasks (each clones it, so a `borrow_mut` is only ever held transiently).
type Shard = Rc<RefCell<ShardState>>;

/// The two identities every shard task needs: the glommio `executor` id (1-based,
/// used only in logs) and the mesh `peer` id (0-based; peer 0 owns broker-wide
/// duties like `$SYS` and the retained snapshot). Bundled so the spawn
/// orchestration doesn't thread two bare `usize`s everywhere.
#[derive(Clone, Copy)]
pub(super) struct ShardIds {
	pub(super) executor: usize,
	pub(super) peer: usize,
}

/// Session snapshot files orphaned by a decrease in shard count — `sessions-<n>.mqtt`
/// for `n >= shard_count`. Peer 0 loads these at startup so no durable session is
/// lost when `cores` shrinks between runs.
fn orphan_session_files(dir: &Path, shard_count: usize) -> Vec<PathBuf> {
	let mut out = Vec::new();
	let Ok(entries) = std::fs::read_dir(dir) else {
		return out;
	};
	for entry in entries.flatten() {
		let path = entry.path();
		if let Some(name) = path.file_name().and_then(|n| n.to_str())
			&& let Some(idx) = name
				.strip_prefix("sessions-")
				.and_then(|s| s.strip_suffix(".mqtt"))
				.and_then(|n| n.parse::<usize>().ok())
			&& idx >= shard_count
		{
			out.push(path);
		}
	}
	out
}

/// Restores retained messages and this shard's persisted sessions from disk,
/// before the shard starts accepting. A no-op when persistence is disabled.
pub(super) async fn restore_from_disk(state: &Shard, config: &Config, ids: ShardIds, shard_count: usize) {
	let (shard_id, mesh_peer_id) = (ids.executor, ids.peer);
	if !config.persistence.enabled {
		return;
	}

	// Retained set: every shard loads the same snapshot into its own table
	// (retained is replicated across shards), so no cross-shard coordination.
	let path = config.persistence.retained_path();
	match persistence::load_retained(&path).await {
		Ok(messages) => {
			let restored = messages.len();
			state.borrow_mut().load_retained(messages);
			if mesh_peer_id == 0 && restored > 0 {
				tracing::info!(
					shard = shard_id,
					retained = restored,
					"restored retained messages from disk"
				);
			}
		}
		Err(e) => {
			tracing::error!(shard = shard_id, error = %e, path = %path.display(), "failed to load retained snapshot");
		}
	}

	// Sessions are shard-local, so each shard loads its own file; peer 0
	// additionally loads any files orphaned by a decrease in `cores`, so no
	// durable session is lost when the shard count shrinks. Loaded sessions are
	// suspended — a reconnecting client resumes one directly, or the cross-shard
	// `Claim`/`Handoff` migrates it to wherever the client lands.
	let _ = std::fs::create_dir_all(&config.persistence.dir);
	let now = Instant::now();
	let mut paths = vec![config.persistence.session_path(mesh_peer_id)];
	if mesh_peer_id == 0 {
		paths.extend(orphan_session_files(&config.persistence.dir, shard_count));
	}
	let mut restored = 0;
	for path in paths {
		match persistence::load_sessions(&path).await {
			Ok(sessions) => {
				restored += sessions.len();
				state.borrow_mut().load_sessions(sessions, now);
			}
			Err(e) => {
				tracing::error!(shard = shard_id, error = %e, path = %path.display(), "failed to load session snapshot");
			}
		}
	}
	if restored > 0 {
		tracing::info!(
			shard = shard_id,
			sessions = restored,
			"restored sessions from disk"
		);
	}
}

/// Spawns every periodic background task for this shard: the inbound-mesh drain
/// (foreground), plus — on the low-priority maintenance queue — the session
/// sweep + `malloc_trim`, load probe, shedding, and the persistence and `$SYS`
/// snapshotters. Each captures its own clone of the shard state.
pub(super) fn spawn_background(
	state: &Shard,
	config: &Config,
	metrics: &Arc<Metrics>,
	load: &Rc<LoadMonitor>,
	ids: ShardIds,
	tq_maintenance: TaskQueueHandle,
	mut receivers: Receivers<MeshMsg>,
) {
	let (shard_id, mesh_peer_id) = (ids.executor, ids.peer);
	// Drain inbound cross-shard messages into local fan-out / migration handling.
	for (_producer, receiver) in receivers.streams() {
		let state = state.clone();
		glommio::spawn_local(async move {
			while let Some(msg) = receiver.recv().await {
				match msg {
					MeshMsg::Publish(publish) => state.borrow_mut().deliver_local(publish, None),
					MeshMsg::Control(control) => state.borrow_mut().on_control(*control),
					MeshMsg::Shared(event) => state.borrow_mut().apply_shared_event(event),
				}
			}
		})
		.detach();
	}

	spawn_session_sweep(state, tq_maintenance, mesh_peer_id);
	spawn_load_probe(metrics, load, config, shard_id, mesh_peer_id);
	spawn_load_shedding(state, metrics, load, config, tq_maintenance, shard_id);
	spawn_snapshotters(state, config, tq_maintenance, mesh_peer_id);
	spawn_sys_metrics(state, config, metrics, tq_maintenance, mesh_peer_id);
}

/// Periodically reclaim suspended sessions whose expiry has lapsed and publish
/// any delayed wills that have come due. Every `MALLOC_TRIM_EVERY` sweeps, peer 0
/// also asks glibc to return freed arena pages to the kernel: a burst's small
/// allocations otherwise stay resident in the arena indefinitely (measured
/// ~50 MB retained after a 2000-connection burst fully disconnected), which reads
/// as a leak on memory-tight hosts. `malloc_trim` walks all arenas, so one shard
/// suffices.
fn spawn_session_sweep(state: &Shard, tq: TaskQueueHandle, mesh_peer_id: usize) {
	let state = state.clone();
	glommio::spawn_local_into(
		async move {
			let mut sweeps: u32 = 0;
			loop {
				glommio::timer::sleep(SESSION_SWEEP_INTERVAL).await;
				let wills = state.borrow_mut().sweep_expired();
				for will in wills {
					let mut shard_state = state.borrow_mut();
					shard_state.broadcast(&will);
					shard_state.deliver_local(will, None);
				}
				sweeps = sweeps.wrapping_add(1);
				if mesh_peer_id == 0 && sweeps.is_multiple_of(MALLOC_TRIM_EVERY) {
					// SAFETY: glibc `malloc_trim` is thread-safe and touches no
					// Rust-visible state; it only releases free arena memory.
					unsafe { libc::malloc_trim(0) };
				}
			}
		},
		tq,
	)
	.expect("spawn session-sweep task")
	.detach();
}

/// Measures how far the reactor's scheduling of a normal-priority task slips past
/// its due time — near zero when idle, growing under saturation — and feeds it
/// into the shard's load monitor and the `$SYS` gauge. Runs on the default
/// (foreground) queue so it observes the delay client-serving work actually sees.
fn spawn_load_probe(
	metrics: &Arc<Metrics>,
	load: &Rc<LoadMonitor>,
	config: &Config,
	shard_id: usize,
	mesh_peer_id: usize,
) {
	let load = Rc::clone(load);
	let metrics = metrics.clone();
	let stall_warn = Duration::from_millis(u64::from(config.overload.stall_warn_ms));
	glommio::spawn_local(async move {
		let mut warning = false;
		loop {
			let started = Instant::now();
			glommio::timer::sleep(LOAD_PROBE_INTERVAL).await;
			let delay = started.elapsed().saturating_sub(LOAD_PROBE_INTERVAL);
			load.record(delay);
			let smoothed = load.scheduling_delay();
			metrics.record_shard_delay(mesh_peer_id, smoothed);
			// Stall detector with hysteresis: warn on crossing up, clear at half.
			if !stall_warn.is_zero() {
				if !warning && smoothed >= stall_warn {
					warning = true;
					tracing::warn!(
						shard = shard_id,
						delay_ms = smoothed.as_millis(),
						"shard overloaded: reactor scheduling delay is high"
					);
				} else if warning && smoothed < stall_warn / 2 {
					warning = false;
					tracing::info!(shard = shard_id, "shard load recovered");
				}
			}
		}
	})
	.detach();
}

/// While a shard stays overloaded, close a small batch of its connections each
/// interval so they reconnect and `SO_REUSEPORT` rehashes them onto (probably)
/// cooler cores. Opt-in and disruptive, so only spawned when enabled.
fn spawn_load_shedding(
	state: &Shard,
	metrics: &Arc<Metrics>,
	load: &Rc<LoadMonitor>,
	config: &Config,
	tq: TaskQueueHandle,
	shard_id: usize,
) {
	if config.overload.shed_delay_ms == 0 {
		return;
	}
	let load = Rc::clone(load);
	let state = state.clone();
	let metrics = metrics.clone();
	let threshold = Duration::from_millis(u64::from(config.overload.shed_delay_ms));
	let batch = config.overload.shed_batch;
	glommio::spawn_local_into(
		async move {
			loop {
				glommio::timer::sleep(SHED_INTERVAL).await;
				if load.exceeds(threshold) {
					let shed = state.borrow_mut().shed_connections(batch);
					if shed > 0 {
						metrics.record_connections_shed(shed as u64);
						tracing::warn!(
							shard = shard_id,
							shed,
							"sustained overload: shedding connections to rebalance"
						);
					}
				}
			}
		},
		tq,
	)
	.expect("spawn load-shedding task")
	.detach();
}

/// Periodic persistence snapshots: peer 0 writes the (replicated) retained set;
/// every shard writes its own (shard-local) sessions. No-ops when persistence is
/// disabled or `snapshot_interval` is 0 (snapshot only on graceful shutdown).
fn spawn_snapshotters(state: &Shard, config: &Config, tq: TaskQueueHandle, mesh_peer_id: usize) {
	if !config.persistence.enabled {
		return;
	}
	if mesh_peer_id == 0
		&& let Err(e) = std::fs::create_dir_all(&config.persistence.dir)
	{
		tracing::error!(error = %e, dir = %config.persistence.dir.display(), "failed to create persistence dir");
	}
	let snapshot_secs = config.persistence.snapshot_interval;
	if snapshot_secs == 0 {
		return;
	}

	// Retained snapshot (peer 0 only — it is replicated across shards).
	if mesh_peer_id == 0 {
		let state = state.clone();
		let path = config.persistence.retained_path();
		glommio::spawn_local_into(
			async move {
				loop {
					glommio::timer::sleep(Duration::from_secs(snapshot_secs)).await;
					let messages = state.borrow().retained_messages();
					match persistence::save_retained(&path, &messages).await {
						Ok(()) => tracing::debug!(retained = messages.len(), "retained snapshot written"),
						Err(e) => tracing::error!(error = %e, "retained snapshot failed"),
					}
				}
			},
			tq,
		)
		.expect("spawn retained-snapshot task")
		.detach();
	}

	// Session snapshot (every shard writes its own file).
	let state = state.clone();
	let path = config.persistence.session_path(mesh_peer_id);
	glommio::spawn_local_into(
		async move {
			loop {
				glommio::timer::sleep(Duration::from_secs(snapshot_secs)).await;
				let sessions = state.borrow().persist_sessions(Instant::now());
				match persistence::save_sessions(&path, &sessions).await {
					Ok(()) => tracing::debug!(sessions = sessions.len(), "session snapshot written"),
					Err(e) => tracing::error!(error = %e, "session snapshot failed"),
				}
			}
		},
		tq,
	)
	.expect("spawn session-snapshot task")
	.detach();
}

/// Peer 0 publishes broker-wide `$SYS` metrics as retained messages, broadcast to
/// every shard so any `$SYS/#` subscriber sees them.
fn spawn_sys_metrics(state: &Shard, config: &Config, metrics: &Arc<Metrics>, tq: TaskQueueHandle, mesh_peer_id: usize) {
	if mesh_peer_id != 0 || !config.sys.enabled {
		return;
	}
	let state = state.clone();
	let metrics = metrics.clone();
	let interval = Duration::from_secs(config.sys.interval);
	glommio::spawn_local_into(
		async move {
			loop {
				glommio::timer::sleep(interval).await;
				let topics = metrics.snapshot().topics();
				let mut shard_state = state.borrow_mut();
				for (topic, value) in topics {
					let mut publish = Publish::new(topic, QoS::AtMostOnce, value.into_bytes());
					publish.retain = true;
					shard_state.broadcast(&publish);
					shard_state.deliver_local(publish, None);
				}
			}
		},
		tq,
	)
	.expect("spawn $SYS metrics task")
	.detach();
}

/// Writes the final persistence snapshots on graceful shutdown, capturing
/// anything since the last periodic write. By the time this runs every connection
/// has drained and its durable session is suspended, so this captures them all.
pub(super) async fn write_final_snapshots(state: &Shard, config: &Config, shard_id: usize, mesh_peer_id: usize) {
	if !config.persistence.enabled {
		return;
	}
	// Retained set (peer 0 only — it is replicated across shards).
	if mesh_peer_id == 0 {
		let messages = state.borrow().retained_messages();
		match persistence::save_retained(&config.persistence.retained_path(), &messages).await {
			Ok(()) => tracing::info!(
				shard = shard_id,
				retained = messages.len(),
				"wrote final retained snapshot"
			),
			Err(e) => tracing::error!(shard = shard_id, error = %e, "final retained snapshot failed"),
		}
	}
	// This shard's sessions (every shard writes its own).
	let sessions = state.borrow().persist_sessions(Instant::now());
	match persistence::save_sessions(&config.persistence.session_path(mesh_peer_id), &sessions).await {
		Ok(()) => tracing::info!(
			shard = shard_id,
			sessions = sessions.len(),
			"wrote final session snapshot"
		),
		Err(e) => tracing::error!(shard = shard_id, error = %e, "final session snapshot failed"),
	}
}
