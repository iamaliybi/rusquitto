//! Per-shard background housekeeping: persistence restore/snapshot, the
//! inbound-mesh drain, the reactor load probe, the session-expiry sweep, and
//! load shedding.
//!
//! [`restore_from_disk`] and [`write_final_snapshots`] run inline in the shard's
//! startup/shutdown; [`spawn_background`] launches the periodic tasks, most onto
//! the low-priority maintenance queue so they yield to client-serving work.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_rustls::TlsAcceptor;
use glommio::TaskQueueHandle;
use glommio::channels::channel_mesh::Receivers;
use glommio::channels::local_channel;
use mqttbytes::{QoS, v5::Publish};

use super::{LOAD_PROBE_INTERVAL, MALLOC_TRIM_EVERY, SESSION_SWEEP_INTERVAL, SHED_INTERVAL, Shard};
use crate::broker::messages::MeshMsg;
use crate::broker::session::PersistedSession;
use crate::config::Config;
use crate::persistence;
use crate::server::overload::LoadMonitor;
use crate::telemetry::metrics::Metrics;

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

	// Start tracking session mutations for the WAL before the shard serves, so no
	// suspension between now and the first flush is missed.
	if config.persistence.wal_enabled() {
		state.borrow_mut().enable_wal();
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
	let wal_on = config.persistence.wal_enabled();
	// Merge every snapshot (+ its sibling WAL) into one client-keyed view so a WAL
	// record supersedes the snapshot entry it updates. The WAL is replayed *after*
	// its snapshot, so its records — newer by construction — win.
	let mut merged: HashMap<String, PersistedSession> = HashMap::new();
	for path in paths {
		match persistence::load_sessions(&path).await {
			Ok(sessions) => {
				for ps in sessions {
					merged.insert(ps.client_id.clone(), ps);
				}
			}
			Err(e) => {
				tracing::error!(shard = shard_id, error = %e, path = %path.display(), "failed to load session snapshot");
			}
		}
		if wal_on {
			let wal_path = path.with_extension("wal");
			match persistence::wal::replay(&wal_path, &mut merged).await {
				Ok(n) if n > 0 => tracing::info!(shard = shard_id, records = n, "replayed session WAL"),
				Ok(_) => {}
				Err(e) => {
					tracing::error!(shard = shard_id, error = %e, path = %wal_path.display(), "failed to replay session WAL");
				}
			}
		}
	}
	let restored = merged.len();
	if restored > 0 {
		state
			.borrow_mut()
			.load_sessions(merged.into_values().collect(), now);
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
	// After a blocking `recv` wakes, drain every message already queued without
	// yielding between them (via `poll_once`), so a burst forwarded from a peer is
	// handled in a single wake instead of one reactor reschedule per message —
	// cutting cross-shard scheduling latency and CPU under load. `poll_once` on a
	// local-channel `recv` is cancel-safe: a pending poll takes no message.
	for (_producer, receiver) in receivers.streams() {
		let state = state.clone();
		glommio::spawn_local(async move {
			while let Some(msg) = receiver.recv().await {
				handle_mesh_msg(&state, msg);
				while let Some(Some(msg)) = futures_lite::future::poll_once(receiver.recv()).await {
					handle_mesh_msg(&state, msg);
				}
			}
		})
		.detach();
	}

	spawn_control_drain(state);
	spawn_session_sweep(state, tq_maintenance, mesh_peer_id);
	spawn_load_probe(metrics, load, config, shard_id, mesh_peer_id);
	spawn_load_shedding(state, metrics, load, config, tq_maintenance, shard_id);
	spawn_snapshotters(state, config, tq_maintenance, mesh_peer_id);
	spawn_sys_metrics(state, config, metrics, tq_maintenance, mesh_peer_id);
}

/// Applies one inbound mesh message to the shard: a forwarded publish fans out
/// locally, a session control message drives migration, a shared-sub event
/// updates the replicated membership view. Each takes the borrow transiently.
fn handle_mesh_msg(state: &Shard, msg: MeshMsg) {
	match msg {
		MeshMsg::Publish(publish) => state.borrow_mut().deliver_local(publish, None),
		MeshMsg::Control(control) => state.borrow_mut().on_control(*control),
		MeshMsg::Shared(event) => state.borrow_mut().apply_shared_event(event),
	}
}

/// Spawns the reliable control-plane drain (foreground): it awaits the shard's
/// control outbox and forwards each message with the awaiting `send_to` (mesh
/// backpressure), so session `Claim`/`Handoff` and shared-subscription
/// `Join`/`Leave` are never dropped under load — unlike the best-effort data
/// plane. FIFO, so a `Join` is never reordered past a later `Leave`. A no-op on a
/// single-shard broker (no peers).
fn spawn_control_drain(state: &Shard) {
	let senders = {
		let s = state.borrow();
		if s.mesh_peers() == 0 {
			return;
		}
		s.mesh_senders()
	};
	let Some(senders) = senders else {
		return;
	};
	let (tx, rx) = local_channel::new_unbounded::<(usize, MeshMsg)>();
	state.borrow_mut().set_control_tx(tx);
	glommio::spawn_local(async move {
		while let Some((peer, msg)) = rx.recv().await {
			let _ = senders.send_to(peer, msg).await;
		}
	})
	.detach();
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

/// Checkpoint cadence for the session WAL when periodic snapshots are off
/// (`snapshot_interval = 0`): a full snapshot + WAL truncate still runs this
/// often so the log can't grow without bound over a long run.
const WAL_CHECKPOINT_FALLBACK: Duration = Duration::from_secs(60);

/// Spawns the persistence tasks: peer 0's periodic retained snapshot, and each
/// shard's session persistence — the WAL flush/checkpoint loop when the WAL is
/// enabled, otherwise the plain periodic session snapshot. No-ops when
/// persistence is disabled; with both `snapshot_interval = 0` and the WAL off,
/// sessions are snapshotted only on graceful shutdown.
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

	// Retained snapshot (peer 0 only — it is replicated across shards). Retained is
	// snapshot-only, so this needs a non-zero interval.
	if mesh_peer_id == 0 && snapshot_secs > 0 {
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

	// Session persistence (every shard writes its own file).
	if config.persistence.wal_enabled() {
		spawn_session_wal(state, config, tq, mesh_peer_id);
	} else if snapshot_secs > 0 {
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
}

/// The session WAL task: every `wal_flush_ms` it appends the batch of session
/// mutations since the last flush and `fdatasync`s it; every `snapshot_interval`
/// (or [`WAL_CHECKPOINT_FALLBACK`] when that is 0) it writes a full session
/// snapshot and truncates the now-subsumed log.
fn spawn_session_wal(state: &Shard, config: &Config, tq: TaskQueueHandle, mesh_peer_id: usize) {
	let state = state.clone();
	let session_path = config.persistence.session_path(mesh_peer_id);
	let wal_path = config.persistence.wal_path(mesh_peer_id);
	let flush = Duration::from_millis(config.persistence.wal_flush_ms);
	let checkpoint = if config.persistence.snapshot_interval > 0 {
		Duration::from_secs(config.persistence.snapshot_interval)
	} else {
		WAL_CHECKPOINT_FALLBACK
	};
	glommio::spawn_local_into(
		async move {
			let mut wal = match persistence::Wal::open(&wal_path).await {
				Ok(w) => w,
				Err(e) => {
					tracing::error!(error = %e, path = %wal_path.display(), "failed to open session WAL; sessions persist on checkpoint/shutdown only");
					return;
				}
			};
			let mut since_checkpoint = Duration::ZERO;
			loop {
				glommio::timer::sleep(flush).await;
				// 1. Flush the batch of session mutations since the last flush. Bind
				// the batch in a `let` first so the shard borrow is released before the
				// append `.await` (never hold a `RefCell` borrow across a yield).
				let batch = state.borrow_mut().take_wal_batch(Instant::now());
				if let Some(batch) = batch
					&& let Err(e) = wal.append(batch).await
				{
					tracing::error!(error = %e, "session WAL append failed");
				}
				// 2. Periodic checkpoint: a full snapshot subsumes the log, so truncate it.
				since_checkpoint += flush;
				if since_checkpoint >= checkpoint {
					since_checkpoint = Duration::ZERO;
					let sessions = state.borrow().persist_sessions(Instant::now());
					match persistence::save_sessions(&session_path, &sessions).await {
						Ok(()) => {
							if let Err(e) = wal.truncate().await {
								tracing::error!(error = %e, "session WAL truncate failed");
							}
							tracing::debug!(sessions = sessions.len(), "session checkpoint written, WAL truncated");
						}
						Err(e) => tracing::error!(error = %e, "session checkpoint failed"),
					}
				}
			}
		},
		tq,
	)
	.expect("spawn session WAL task")
	.detach();
}

/// Watches the TLS certificate / key / client-CA files and hot-reloads them into
/// this shard's acceptor when any changes, so a rotated certificate reaches new
/// handshakes without a restart. Connections already established keep the
/// certificate they handshook with. Shard-local: each shard reloads its own
/// acceptor, so no cross-core coordination is involved. A no-op when TLS is off or
/// `reload_interval` is 0. If the new files fail to load, the previous certificate
/// is kept and the reload is retried next tick.
pub(super) fn spawn_tls_reload(
	acceptor: &Rc<RefCell<Option<TlsAcceptor>>>,
	config: &Config,
	tq: TaskQueueHandle,
	shard_id: usize,
) {
	if !config.tls.enabled || config.tls.reload_interval == 0 {
		return;
	}
	let (Some(cert_path), Some(key_path)) = (config.tls.cert_file.clone(), config.tls.key_file.clone()) else {
		return;
	};
	let ca_path = config.tls.client_ca_file.clone();
	let require = config.tls.require_client_cert;
	let interval = Duration::from_secs(config.tls.reload_interval);
	let acceptor = acceptor.clone();
	glommio::spawn_local_into(
		async move {
			let mut seen = file_mtimes(&cert_path, &key_path, ca_path.as_deref());
			loop {
				glommio::timer::sleep(interval).await;
				let current = file_mtimes(&cert_path, &key_path, ca_path.as_deref());
				if current == seen {
					continue;
				}
				match crate::transport::tls::load_server_config(&cert_path, &key_path, ca_path.as_deref(), require) {
					Ok(new_config) => {
						*acceptor.borrow_mut() = Some(TlsAcceptor::from(new_config));
						seen = current;
						tracing::info!(shard = shard_id, "reloaded TLS certificate");
					}
					// Keep the old acceptor and don't advance `seen`, so a corrected file
					// is picked up on a later tick.
					Err(e) => tracing::error!(
						shard = shard_id,
						error = %e,
						"TLS certificate reload failed; keeping the previous certificate"
					),
				}
			}
		},
		tq,
	)
	.expect("spawn TLS reload task")
	.detach();
}

/// Last-modified times of the certificate, key, and (optional) client-CA files —
/// the change signal the reload watcher compares tick to tick.
fn file_mtimes(cert: &Path, key: &Path, ca: Option<&Path>) -> [Option<std::time::SystemTime>; 3] {
	let mtime = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
	[mtime(cert), mtime(key), ca.and_then(mtime)]
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
