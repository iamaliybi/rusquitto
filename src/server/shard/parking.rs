//! The parked-connection registry and its per-shard readiness ring.
//!
//! A *parked* connection is an idle plain-TCP client whose per-connection task
//! and io_uring read `Source` (~a few KiB) have been torn down: only its fd and a
//! boxed [`ResumeState`] (~0.2 KiB) remain here, with the fd armed as a oneshot
//! `IORING_OP_POLL_ADD(POLLIN)` on a small **raw** io_uring owned by this module
//! — raw because glommio does not expose its own `POLL_ADD`, and one shared ring
//! per shard is the whole point (no per-connection reactor state).
//!
//! One long-lived glommio task per shard drives the registry, wakes on either of
//! two signals and re-checks the world:
//!
//! - **egress** — the broker routed a delivery to a parked client and sent
//!   [`UnparkCmd::Wake`] over the shard-local command channel (immediate: the
//!   task blocks on this channel);
//! - **ingress** — the client sent bytes, the kernel posted a CQE. Reaping the
//!   completion queue is a shared-memory read (zero syscalls when empty), paid
//!   for by an adaptive timer tick: ~1 ms while recently busy, decaying to
//!   ~10 ms, and no tick at all while nothing is parked. Worst-case ingress
//!   wake latency is therefore one tick — invisible on a connection that has
//!   already been silent for the whole parking grace.
//!
//! Every removal path is generation-guarded twice: the io_uring `user_data`
//! carries a slab generation tag (a stale CQE from a previous park of the same
//! slot is ignored), and the broker session's own generation is checked at
//! reattach (a takeover that raced the wake resolves as a quiet close).
//!
//! The task runs on the **default** (foreground) queue, deliberately not the
//! low-share maintenance queue: parked clients are still connected clients, and
//! their ingress must not be starved exactly when overload churn makes the
//! shard busy.
//!
//! Fallback note: if `RLIMIT_MEMLOCK` is ever too tight for even this small
//! ring (the ring is created once per shard at startup and parking is disabled
//! with a warning if that fails), an `epoll` fd with `EPOLLONESHOT` is a
//! drop-in alternative — same token scheme, no locked memory, one
//! `epoll_wait(timeout=0)` syscall per tick.

use std::collections::{HashMap, VecDeque};
use std::os::fd::RawFd;
use std::time::{Duration, Instant};

use futures_lite::FutureExt;
use glommio::channels::local_channel::LocalReceiver;
use io_uring::{IoUring, opcode, squeue, types};

use super::accept::ConnSlot;
use super::serve;
use super::{ConnCtx, Shard};
use crate::broker::delivery::UnparkCmd;
use crate::server::connection::ResumeState;

/// Submission-queue depth of the per-shard parking ring. Small on purpose — the
/// SQ only stages `POLL_ADD`/`POLL_REMOVE` entries between submits, and every
/// push falls back to [`Parking::pending_sqe`] when it is momentarily full, so
/// depth bounds locked memory (~100 KiB per shard with the kernel's 2× CQ),
/// not capacity. The completion side relies on `IORING_FEAT_NODROP` (Linux
/// 5.5+, well below glommio's own floor) for overflow.
const RING_ENTRIES: u32 = 1024;

/// How often the parking task checks parked keep-alive deadlines.
const DEADLINE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Completion-reap tick while the ring saw a wake/CQE within the last second.
const TICK_BUSY: Duration = Duration::from_millis(1);

/// Completion-reap tick once the ring has been quiet for a while.
const TICK_IDLE: Duration = Duration::from_millis(10);

/// Heartbeat while nothing is parked at all: just often enough to notice the
/// first park promptly (connections park themselves into the registry without
/// signalling this task).
const TICK_EMPTY: Duration = Duration::from_millis(25);

/// A parked connection: the dup'd socket fd, the state to rebuild its
/// [`Connection`](crate::server::connection::Connection), and its RAII
/// connection-accounting slot — parked clients still occupy their per-shard and
/// per-IP slots (they are real connections) and still gate the shutdown drain.
pub(super) struct ParkedConn {
	pub(super) fd: RawFd,
	pub(super) resume: Box<ResumeState>,
	pub(super) slot: ConnSlot,
}

impl ParkedConn {
	/// Closes the socket. Consumes the entry; the `ConnSlot` drop rebalances the
	/// connection counts. (Not a `Drop` impl on purpose: the resume path must be
	/// able to move the fd out without a close racing it.)
	pub(super) fn close(self) {
		// SAFETY: `fd` is an open socket fd owned exclusively by this entry.
		unsafe { libc::close(self.fd) };
	}
}

/// The per-shard parked-connection registry: a generation-tagged slab (indices
/// are io_uring `user_data` tokens) plus a client-id index for the egress wake
/// and takeover paths.
pub(super) struct Parking {
	ring: IoUring,
	/// Slab of parked connections; `user_data = index << 32 | generation tag`.
	slots: Vec<Option<ParkedConn>>,
	/// Per-slot generation tags, bumped on every removal, so a stale CQE from an
	/// earlier occupancy of the slot can never resurrect the wrong connection.
	gen_tags: Vec<u32>,
	free: Vec<u32>,
	/// Client id → slot index, for egress wakes and takeover closes.
	by_client: HashMap<String, u32>,
	/// Overflow staging for submission entries pushed while the SQ was full;
	/// drained ahead of every submit.
	pending_sqe: VecDeque<squeue::Entry>,
}

/// `user_data` for a `POLL_REMOVE`'s *own* completion — carries no slot, always
/// ignored by the reaper. (`index << 32` would collide with slot 0 otherwise.)
const CANCEL_TOKEN: u64 = u64::MAX;

fn token(index: u32, tag: u32) -> u64 {
	(u64::from(index) << 32) | u64::from(tag)
}

impl Parking {
	/// Creates the registry and its ring. Failing to create the ring (e.g. a
	/// tight `RLIMIT_MEMLOCK`) disables parking for the shard — the caller warns
	/// and runs without it; connections then simply never park.
	pub(super) fn new() -> std::io::Result<Self> {
		Ok(Self {
			ring: IoUring::new(RING_ENTRIES)?,
			slots: Vec::new(),
			gen_tags: Vec::new(),
			free: Vec::new(),
			by_client: HashMap::new(),
			pending_sqe: VecDeque::new(),
		})
	}

	/// The number of connections currently parked.
	pub(super) fn len(&self) -> usize {
		self.slots.len() - self.free.len()
	}

	pub(super) fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// Registers a parked connection and arms its fd on the ring. Synchronous —
	/// the caller runs it in the same no-await block as
	/// [`park_session`](crate::broker::shard::ShardState::park_session), so no
	/// wake can be issued for an entry that isn't here yet.
	pub(super) fn park(&mut self, conn: ParkedConn) {
		let fd = conn.fd;
		let index = match self.free.pop() {
			Some(index) => index,
			None => {
				self.slots.push(None);
				self.gen_tags.push(0);
				(self.slots.len() - 1) as u32
			}
		};
		self.by_client
			.insert(conn.resume.client_id().to_string(), index);
		self.slots[index as usize] = Some(conn);

		// Oneshot POLL_ADD: level-triggered, so bytes that raced the park (or are
		// already buffered in the kernel) complete immediately on submit.
		let poll = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
			.build()
			.user_data(token(index, self.gen_tags[index as usize]));
		self.push_sqe(poll);
		self.submit();
	}

	/// Removes the parked connection for `client_id`, if present. With
	/// `generation = Some(g)` the entry is only taken if it parked under that
	/// session generation (the [`UnparkCmd::Close`] race guard); `None` takes it
	/// unconditionally (egress wake — the reattach re-checks the session
	/// generation anyway).
	pub(super) fn take_by_client(&mut self, client_id: &str, generation: Option<u64>) -> Option<ParkedConn> {
		let index = *self.by_client.get(client_id)?;
		if let Some(g) = generation {
			let entry = self.slots[index as usize].as_ref()?;
			if entry.resume.generation() != g {
				return None;
			}
		}
		self.remove(index)
	}

	/// Resolves a completion token to its parked connection. A stale token —
	/// the slot was re-used or the entry already taken — resolves to `None` and
	/// the completion is ignored.
	pub(super) fn take_by_token(&mut self, user_data: u64) -> Option<ParkedConn> {
		if user_data == CANCEL_TOKEN {
			return None;
		}
		let index = (user_data >> 32) as u32;
		let tag = user_data as u32;
		if self.gen_tags.get(index as usize) != Some(&tag) {
			return None;
		}
		// The CQE consumed the oneshot poll: the removal must not push a
		// POLL_REMOVE for it (there is nothing left to cancel).
		self.remove_without_cancel(index)
	}

	/// Takes every parked connection past its keep-alive deadline (frozen at
	/// park time — any traffic would have unparked it and refreshed it).
	pub(super) fn take_expired(&mut self, now: Instant) -> Vec<ParkedConn> {
		let expired: Vec<u32> = self
			.slots
			.iter()
			.enumerate()
			.filter_map(|(index, entry)| {
				let deadline = entry.as_ref()?.resume.deadline()?;
				(deadline <= now).then_some(index as u32)
			})
			.collect();
		expired
			.into_iter()
			.filter_map(|index| self.remove(index))
			.collect()
	}

	/// Takes everything — the shutdown drain.
	pub(super) fn drain_all(&mut self) -> Vec<ParkedConn> {
		let occupied: Vec<u32> = (0..self.slots.len() as u32)
			.filter(|&i| self.slots[i as usize].is_some())
			.collect();
		occupied
			.into_iter()
			.filter_map(|index| self.remove(index))
			.collect()
	}

	/// Removes a slot, cancelling its still-armed poll. The cancellation is
	/// best-effort: if it loses the race with the poll's own completion, the
	/// resulting CQE carries a now-stale generation tag and is ignored.
	fn remove(&mut self, index: u32) -> Option<ParkedConn> {
		let stale = token(index, self.gen_tags[index as usize]);
		let entry = self.remove_without_cancel(index)?;
		let cancel = opcode::PollRemove::new(stale)
			.build()
			.user_data(CANCEL_TOKEN);
		self.push_sqe(cancel);
		self.submit();
		Some(entry)
	}

	/// Removes a slot without touching the ring (the poll already completed).
	fn remove_without_cancel(&mut self, index: u32) -> Option<ParkedConn> {
		let entry = self.slots.get_mut(index as usize)?.take()?;
		self.gen_tags[index as usize] = self.gen_tags[index as usize].wrapping_add(1);
		self.free.push(index);
		self.by_client.remove(entry.resume.client_id());
		Some(entry)
	}

	/// Stages one submission entry, overflowing to the pending queue when the SQ
	/// is momentarily full.
	fn push_sqe(&mut self, entry: squeue::Entry) {
		// SAFETY: every entry is a POLL_ADD/POLL_REMOVE built above over an fd
		// this registry owns; neither references caller memory after push.
		if unsafe { self.ring.submission().push(&entry) }.is_err() {
			self.pending_sqe.push_back(entry);
		}
	}

	/// Drains the overflow queue into the SQ as space allows and submits. The
	/// `submit` syscall is non-blocking (no `min_complete`).
	pub(super) fn submit(&mut self) {
		while let Some(entry) = self.pending_sqe.pop_front() {
			// SAFETY: as in `push_sqe`.
			if unsafe { self.ring.submission().push(&entry) }.is_err() {
				self.pending_sqe.push_front(entry);
				break;
			}
		}
		if let Err(e) = self.ring.submit() {
			// EINTR/EAGAIN are retried on the next tick; anything else is logged
			// once per occurrence (the polls stay staged, nothing is lost).
			tracing::debug!(error = %e, "parking ring submit failed, will retry");
		}
	}

	/// Reaps every available completion token (shared-memory read, no syscall).
	fn reap_tokens(&mut self) -> Vec<u64> {
		self.ring.completion().map(|cqe| cqe.user_data()).collect()
	}
}

/// One turn of the parking task: a broker command, or the adaptive tick.
enum Turn {
	Cmd(Option<UnparkCmd>),
	Tick,
}

/// Spawns the per-shard parking task on the **default** queue (see the module
/// docs for why not the maintenance queue).
pub(super) fn spawn_parking(ctx: ConnCtx, state: Shard, wake_rx: LocalReceiver<UnparkCmd>) {
	glommio::spawn_local(async move {
		run_parking(ctx, state, wake_rx).await;
	})
	.detach();
}

async fn run_parking(ctx: ConnCtx, state: Shard, wake_rx: LocalReceiver<UnparkCmd>) {
	let parking = ctx
		.parking
		.clone()
		.expect("parking task spawned only when the registry exists");
	let mut last_event = Instant::now();
	let mut last_sweep = Instant::now();

	loop {
		if ctx.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
			return; // run_shard's drain owns the remaining entries
		}

		// Block: a Wake/Close command wakes immediately; the tick paces CQE
		// reaping and deadline sweeps. Connections park themselves *into* the
		// registry without signalling this task (the transition is synchronous
		// inside their own task), so even the empty state needs a slow heartbeat
		// to notice the first park — after which the tick tightens. The empty
		// tick is deliberately coarse: it only bounds the reap latency of the
		// very first ingress after the first park, and ~40 no-op checks/s cost
		// nothing measurable.
		let tick = if parking.borrow().is_empty() {
			TICK_EMPTY
		} else if last_event.elapsed() < Duration::from_secs(1) {
			TICK_BUSY
		} else {
			TICK_IDLE
		};
		let turn = {
			let cmd = async { Turn::Cmd(wake_rx.recv().await) };
			let tick = async {
				glommio::timer::sleep(tick).await;
				Turn::Tick
			};
			cmd.or(tick).await
		};

		// Drain every command already queued (the first may have come from the
		// blocking turn above).
		let mut first = match turn {
			Turn::Cmd(None) => return, // channel closed: shard teardown
			Turn::Cmd(Some(cmd)) => Some(cmd),
			Turn::Tick => None,
		};
		loop {
			let cmd = match first.take() {
				Some(cmd) => cmd,
				None => match futures_lite::future::poll_once(wake_rx.recv()).await {
					Some(Some(cmd)) => cmd,
					Some(None) => return,
					None => break,
				},
			};
			last_event = Instant::now();
			match cmd {
				UnparkCmd::Wake { client_id } => {
					// The reattach re-checks the session generation, so a Wake that
					// raced a takeover resolves as a quiet close in the resumed task.
					let taken = parking.borrow_mut().take_by_client(&client_id, None);
					if let Some(parked) = taken {
						ctx.metrics.client_unparked();
						serve::spawn_resume(ctx.clone(), parked);
					}
				}
				UnparkCmd::Close { client_id, generation } => {
					// Takeover / Clean Start / mesh claim: close the dormant fd, no
					// resume, no Will. The generation guard makes a Close that lost a
					// race against an unpark (entry gone or re-parked newer) a no-op.
					let taken = parking
						.borrow_mut()
						.take_by_client(&client_id, Some(generation));
					if let Some(parked) = taken {
						ctx.metrics.client_unparked();
						ctx.metrics.client_disconnected();
						parked.close();
					}
				}
			}
		}

		// Ingress: reap readiness completions and resurrect their connections.
		// POLLHUP/POLLERR resurrect too — the resumed read observes EOF and runs
		// the normal disconnect path (which fires the Will, as an abrupt close
		// must).
		let woken: Vec<ParkedConn> = {
			let mut p = parking.borrow_mut();
			p.reap_tokens()
				.into_iter()
				.filter_map(|t| p.take_by_token(t))
				.collect()
		};
		if !woken.is_empty() {
			last_event = Instant::now();
		}
		for parked in woken {
			ctx.metrics.client_unparked();
			serve::spawn_resume(ctx.clone(), parked);
		}

		// Keep-alive: reap parked connections past their frozen deadline. The
		// session suspends exactly as a live keep-alive timeout would, Will
		// included (delay-armed on the suspended session when non-zero).
		if last_sweep.elapsed() >= DEADLINE_SWEEP_INTERVAL {
			last_sweep = Instant::now();
			let expired = parking.borrow_mut().take_expired(Instant::now());
			for mut parked in expired {
				ctx.metrics.client_unparked();
				ctx.metrics.client_disconnected();
				let client_id = parked.resume.client_id().to_string();
				let generation = parked.resume.generation();
				let expiry = parked.resume.session_expiry();
				tracing::warn!(client_id = %client_id, "keep-alive timeout on parked connection, closing");
				let owned = state
					.borrow_mut()
					.suspend_parked(&client_id, generation, expiry);
				if owned && let Some((will, delay)) = parked.resume.take_will() {
					let delay = delay.min(expiry);
					if delay == 0 {
						let mut s = state.borrow_mut();
						s.broadcast(&will);
						s.deliver_local(will, None);
					} else {
						state
							.borrow_mut()
							.arm_will(&client_id, generation, will, delay);
					}
				}
				parked.close();
			}
			// Anything the sweep staged (poll cancellations) goes out with the
			// removals' own submits; this extra submit drains overflow, if any.
			parking.borrow_mut().submit();
		}
	}
}
