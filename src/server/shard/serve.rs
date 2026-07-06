//! Building the transport stack for one accepted socket, then driving its MQTT
//! session to completion.
//!
//! `serve` picks the transport the socket arrived on and hands off to
//! `run_stream`, which is generic over the transport (the payoff of
//! [`ByteStream`](crate::transport::ByteStream): one MQTT engine over TCP / WS /
//! TLS / WSS). Every branch is boxed through a plain-`fn` seam — see the comment
//! on [`serve`] — to keep the long-lived task future tiny.

use std::future::Future;
use std::os::fd::{AsRawFd, FromRawFd};
use std::pin::Pin;
use std::time::Duration;

use futures_rustls::TlsAcceptor;
use glommio::net::TcpStream;
use tracing::Instrument;

use super::accept::ConnSlot;
use super::parking::ParkedConn;
use super::{ConnCtx, Transport};
use crate::server::connection::{Connection, Flow};
use crate::transport::{ByteStream, tls, websocket::WsStream};

/// Serves one accepted socket, building the transport stack it arrived on and then
/// driving the MQTT session over the resulting [`ByteStream`]. TLS and WebSocket
/// each run a handshake first, bounded by the connect timeout so a stalled
/// handshake can't hold the connection open; `wss` stacks both (TLS, then the
/// WebSocket upgrade over the encrypted channel).
///
/// `slot` is the connection's RAII accounting guard. Non-TCP branches simply hold
/// it for the connection's lifetime; the plain-TCP branch may move it into the
/// parking registry when the connection parks.
pub(super) async fn serve(
	ctx: ConnCtx,
	stream: TcpStream,
	transport: Transport,
	tls_acceptor: Option<TlsAcceptor>,
	slot: ConnSlot,
) {
	let timeout = Duration::from_secs(u64::from(ctx.limits.connect_timeout));
	let max_frame = ctx.limits.max_payload_size;

	// Every connection future is boxed *via a plain `fn`* below. Without this, the
	// spawned task's state machine reserves space for the connection future of
	// every transport branch at once — and because temporaries in a statement
	// containing `.await` live across the suspension, even `Box::pin(fut).await`
	// written inline keeps the unboxed future's slot in the frame. Measured:
	// ~13 KiB per connection task, even for plain TCP with TLS disabled.
	// Constructing the box inside a separate non-async function builds the future
	// on an ordinary stack frame, so the task holds only the 8-byte pointer and
	// the heap carries one allocation sized to the transport actually in use
	// (~4.5 KiB for plain TCP).
	match transport {
		// With parking disabled, plain TCP takes the same minimal path as the
		// other transports — no park-capable driver, no context held across the
		// connection's lifetime — so switching parking off costs nothing.
		Transport::Plain if ctx.parking.is_none() => boxed_run_plain(ctx, stream, slot).await,
		Transport::Plain => boxed_run_tcp(ctx, stream, slot).await,
		Transport::WebSocket => boxed_serve_ws(ctx, stream, max_frame, timeout, slot).await,
		Transport::Mqtts => {
			let Some(acceptor) = tls_acceptor else {
				return; // Unreachable: an mqtts listener only exists with an acceptor.
			};
			boxed_serve_tls(ctx, acceptor, stream, timeout, slot).await
		}
		Transport::Wss => {
			let Some(acceptor) = tls_acceptor else {
				return;
			};
			boxed_serve_wss(ctx, acceptor, stream, max_frame, timeout, slot).await
		}
	}
}

/// Boxes the plain-TCP connection future — the only transport that can park —
/// on a plain (non-async) stack frame.
///
/// Deliberately not `async` and deliberately a separate function: see the
/// comment in [`serve`]. The multi-KiB state machine must never exist as a
/// temporary — or a moved-from binding — inside an async frame, or it gets a
/// permanent slot in the enclosing task's allocation. The `Connection` is
/// constructed *here*, on this plain frame, and moved into [`drive_tcp`] as an
/// argument — so the boxed future holds exactly one `Connection`-sized slot
/// (constructing it inside an inner `async` block would add a second).
fn boxed_run_tcp(ctx: ConnCtx, stream: TcpStream, slot: ConnSlot) -> Pin<Box<impl Future<Output = ()>>> {
	let mut conn = Connection::new(
		stream,
		ctx.shard_id,
		ctx.shard.clone(),
		ctx.limits,
		ctx.auth.clone(),
		ctx.metrics.clone(),
		ctx.shutdown.clone(),
		tls::TlsIdentity::None,
	);
	conn.set_parkable(ctx.park_grace);
	Box::pin(drive_tcp(ctx, conn, slot))
}

/// Plain TCP with parking disabled: the same minimal shape as the other
/// transports (see [`boxed_run_tcp`] for the parking-capable variant).
fn boxed_run_plain(ctx: ConnCtx, stream: TcpStream, slot: ConnSlot) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(async move {
		let _slot = slot; // held for the connection's lifetime
		run_stream(ctx, stream, tls::TlsIdentity::None).await
	})
}

/// Drives a plain-TCP connection to completion: `run()` either ends the
/// connection (normal teardown already done) or asks to park, which hands the
/// connection to [`complete_park`] and ends this task.
///
/// This future lives for the connection's whole life, so it stays lean: one
/// `Connection` slot plus the `run()` machine. The park transition's locals
/// live in `complete_park`'s plain stack frame, and its rare can't-park
/// fallback spawns a fresh task rather than returning a second
/// `Connection`-sized value through this frame.
async fn drive_tcp(ctx: ConnCtx, mut conn: Connection<TcpStream>, slot: ConnSlot) {
	if let Ok(Flow::Park) = conn.run().await {
		complete_park(ctx, conn, slot);
	}
	// Closed (or errored): `run()` already performed the full teardown; the
	// slot drops here, releasing the connection's accounting.
}

/// The park transition, in one **synchronous** function: session flip, fd
/// extraction, registry insert. Not `async` on purpose, twice over — its locals
/// live on the plain stack instead of occupying permanent slots in the
/// long-lived connection future, and a plain fn makes the
/// no-`.await`-in-the-transition invariant structural: on a single-threaded
/// shard nothing can interleave, so no delivery can slip between the final
/// mailbox drain and the parked arm of `deliver_to` taking over.
fn complete_park(ctx: ConnCtx, conn: Connection<TcpStream>, slot: ConnSlot) {
	let Some(park) = ctx.parking.clone() else {
		return; // unreachable: parking is set whenever set_parkable was
	};
	let (stream, resume) = conn.into_parts();

	// Flip the broker session first. A `false` means a newer connection took
	// the client id over during our very last turn: we are displaced — close
	// without a Will, balancing the gauge ourselves (a live displaced
	// connection would do the same in its cleanup).
	let flipped = ctx.shard.borrow_mut().park_session(
		resume.client_id(),
		resume.generation(),
		resume.session_snapshot(),
	);
	if !flipped {
		ctx.metrics.client_disconnected();
		return; // dropping `stream` closes the socket
	}

	// Extract the fd: duplicate it, then drop the glommio stream. The drop
	// cancels glommio's registered read source and closes the *original* fd
	// through the normal path; the dup shares the open socket (and its
	// options). glommio's TcpStream has no `IntoRawFd`, and forgetting it
	// would leak its reactor source — this is the clean seam.
	//
	// SAFETY: `stream` is open; F_DUPFD_CLOEXEC allocates a fresh fd.
	let fd = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
	if fd < 0 {
		// fd pressure: can't park. Un-flip by resuming in a fresh task — the
		// reattach restores the session exactly as an unpark would.
		tracing::warn!("fd duplication failed; keeping idle connection live");
		let conn = resume_connection(&ctx, stream, *resume);
		glommio::spawn_local(drive_tcp(ctx, conn, slot)).detach();
		return;
	}
	drop(stream);

	park.borrow_mut().park(ParkedConn { fd, resume, slot });
	ctx.metrics.client_parked();
}

/// Rebuilds a `Connection` around a live stream from its resume state (shared by
/// the unpark path and the failed-park fallback).
fn resume_connection(
	ctx: &ConnCtx,
	stream: TcpStream,
	resume: crate::server::connection::ResumeState,
) -> Connection<TcpStream> {
	Connection::resume(
		stream,
		resume,
		ctx.shard_id,
		ctx.shard.clone(),
		ctx.limits,
		ctx.auth.clone(),
		ctx.metrics.clone(),
		ctx.shutdown.clone(),
		ctx.park_grace,
	)
}

/// Spawns the resurrection task for a parked connection that woke (ingress
/// readiness or an egress delivery). Called by the parking task; the spawned
/// task re-attaches the session, replays what queued, and re-enters the normal
/// serve loop — including parking again later.
pub(super) fn spawn_resume(ctx: ConnCtx, parked: ParkedConn) {
	let span = tracing::info_span!(
		"connection",
		shard = ctx.shard_id,
		tls = false,
		websocket = false,
		client_id = %parked.resume.client_id(),
	);
	glommio::spawn_local(
		async move {
			boxed_resume(ctx, parked).await;
		}
		.instrument(span),
	)
	.detach();
}

/// Boxes the resume pipeline on a plain stack frame (same rationale as
/// [`boxed_run_tcp`]: the `Connection` is built here and moved in as an
/// argument, so the future holds exactly one slot for it).
fn boxed_resume(ctx: ConnCtx, parked: ParkedConn) -> Pin<Box<impl Future<Output = ()>>> {
	let ParkedConn { fd, resume, slot } = parked;
	// SAFETY: `fd` is the open, exclusively-owned socket fd extracted at park
	// time; ownership transfers to the glommio stream (closed on drop).
	let stream = unsafe { TcpStream::from_raw_fd(fd) };
	let conn = resume_connection(&ctx, stream, *resume);
	Box::pin(drive_tcp(ctx, conn, slot))
}

/// The whole WebSocket pipeline (handshake, then the connection) in one box —
/// same rationale as [`boxed_run`]: intermediate stream values (`WsStream`,
/// `TlsStream`) are multi-KiB and must not occupy slots in `serve`'s frame.
fn boxed_serve_ws(
	ctx: ConnCtx,
	stream: TcpStream,
	max_frame: usize,
	timeout: Duration,
	slot: ConnSlot,
) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(async move {
		let _slot = slot; // held for the connection's lifetime (WS never parks)
		match WsStream::accept(stream, max_frame, timeout).await {
			Ok(ws) => run_stream(ctx, ws, tls::TlsIdentity::None).await,
			Err(e) => tracing::warn!(error = %e, "WebSocket handshake failed"),
		}
	})
}

/// The whole `mqtts` pipeline (TLS handshake, then the connection) in one box.
fn boxed_serve_tls(
	ctx: ConnCtx,
	acceptor: TlsAcceptor,
	stream: TcpStream,
	timeout: Duration,
	slot: ConnSlot,
) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(async move {
		let _slot = slot; // held for the connection's lifetime (TLS never parks)
		match tls::accept(&acceptor, stream, timeout).await {
			Ok(tls) => {
				let identity = tls::client_tls_identity(&tls, ctx.map_cert_cn);
				run_stream(ctx, tls, identity).await
			}
			Err(e) => tracing::warn!(error = %e, "TLS handshake failed"),
		}
	})
}

/// The whole `wss` pipeline (TLS, then the WebSocket upgrade over it, then the
/// connection) in one box.
fn boxed_serve_wss(
	ctx: ConnCtx,
	acceptor: TlsAcceptor,
	stream: TcpStream,
	max_frame: usize,
	timeout: Duration,
	slot: ConnSlot,
) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(async move {
		let _slot = slot; // held for the connection's lifetime (WSS never parks)
		match tls::accept(&acceptor, stream, timeout).await {
			Ok(tls) => {
				let identity = tls::client_tls_identity(&tls, ctx.map_cert_cn);
				match WsStream::accept(tls, max_frame, timeout).await {
					Ok(ws) => run_stream(ctx, ws, identity).await,
					Err(e) => tracing::warn!(error = %e, "WebSocket handshake over TLS failed"),
				}
			}
			Err(e) => tracing::warn!(error = %e, "TLS handshake failed"),
		}
	})
}

/// Drives the MQTT state machine over an established stream to completion. Generic
/// over the transport (the payoff of [`ByteStream`]): one implementation serves
/// plain TCP, WebSocket, TLS, and WebSocket-over-TLS alike.
async fn run_stream<S: ByteStream>(ctx: ConnCtx, stream: S, tls_identity: tls::TlsIdentity) {
	let mut conn = Connection::new(
		stream,
		ctx.shard_id,
		ctx.shard,
		ctx.limits,
		ctx.auth,
		ctx.metrics,
		ctx.shutdown,
		tls_identity,
	);
	let _ = conn.run().await;
}
