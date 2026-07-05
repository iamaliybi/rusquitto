//! Building the transport stack for one accepted socket, then driving its MQTT
//! session to completion.
//!
//! `serve` picks the transport the socket arrived on and hands off to
//! `run_stream`, which is generic over the transport (the payoff of
//! [`ByteStream`](crate::transport::ByteStream): one MQTT engine over TCP / WS /
//! TLS / WSS). Every branch is boxed through a plain-`fn` seam — see the comment
//! on [`serve`] — to keep the long-lived task future tiny.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_rustls::TlsAcceptor;
use glommio::net::TcpStream;

use super::{ConnCtx, Transport};
use crate::server::connection::Connection;
use crate::transport::{ByteStream, tls, websocket::WsStream};

/// Serves one accepted socket, building the transport stack it arrived on and then
/// driving the MQTT session over the resulting [`ByteStream`]. TLS and WebSocket
/// each run a handshake first, bounded by the connect timeout so a stalled
/// handshake can't hold the connection open; `wss` stacks both (TLS, then the
/// WebSocket upgrade over the encrypted channel).
pub(super) async fn serve(ctx: ConnCtx, stream: TcpStream, transport: Transport, tls_acceptor: Option<TlsAcceptor>) {
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
		Transport::Plain => boxed_run(ctx, stream).await,
		Transport::WebSocket => boxed_serve_ws(ctx, stream, max_frame, timeout).await,
		Transport::Mqtts => {
			let Some(acceptor) = tls_acceptor else {
				return; // Unreachable: an mqtts listener only exists with an acceptor.
			};
			boxed_serve_tls(ctx, acceptor, stream, timeout).await
		}
		Transport::Wss => {
			let Some(acceptor) = tls_acceptor else {
				return;
			};
			boxed_serve_wss(ctx, acceptor, stream, max_frame, timeout).await
		}
	}
}

/// Boxes a transport's connection future on a plain (non-async) stack frame.
///
/// Deliberately not `async` and deliberately a separate function: see the
/// comment in [`serve`]. The multi-KiB `run_stream` state machine must never
/// exist as a temporary — or a moved-from binding — inside an async frame, or
/// it gets a permanent slot in the enclosing task's allocation.
fn boxed_run<S: ByteStream>(ctx: ConnCtx, stream: S) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(run_stream(ctx, stream, false))
}

/// The whole WebSocket pipeline (handshake, then the connection) in one box —
/// same rationale as [`boxed_run`]: intermediate stream values (`WsStream`,
/// `TlsStream`) are multi-KiB and must not occupy slots in `serve`'s frame.
fn boxed_serve_ws(
	ctx: ConnCtx,
	stream: TcpStream,
	max_frame: usize,
	timeout: Duration,
) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(async move {
		match WsStream::accept(stream, max_frame, timeout).await {
			Ok(ws) => run_stream(ctx, ws, false).await,
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
) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(async move {
		match tls::accept(&acceptor, stream, timeout).await {
			Ok(tls) => {
				let verified = tls::client_cert_present(&tls);
				run_stream(ctx, tls, verified).await
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
) -> Pin<Box<impl Future<Output = ()>>> {
	Box::pin(async move {
		match tls::accept(&acceptor, stream, timeout).await {
			Ok(tls) => {
				let verified = tls::client_cert_present(&tls);
				match WsStream::accept(tls, max_frame, timeout).await {
					Ok(ws) => run_stream(ctx, ws, verified).await,
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
async fn run_stream<S: ByteStream>(ctx: ConnCtx, stream: S, tls_verified: bool) {
	let mut conn = Connection::new(
		stream,
		ctx.shard_id,
		ctx.shard,
		ctx.limits,
		ctx.auth,
		ctx.metrics,
		ctx.shutdown,
		tls_verified,
	);
	let _ = conn.run().await;
}
