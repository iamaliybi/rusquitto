//! The accept loop: binding the shard's listeners, admission control, per-shard
//! connection accounting, and spawning one task per accepted socket.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_lite::FutureExt;
use futures_rustls::TlsAcceptor;
use glommio::net::{TcpListener, TcpStream};
use tracing::Instrument;

use super::serve::serve;
use super::{ConnCtx, SHUTDOWN_POLL_INTERVAL, Transport};
use crate::config::Config;
use crate::server::overload::LoadMonitor;
use crate::transport::tcp::bind_listener;

/// The shard's `SO_REUSEPORT` listeners: TCP is always present, the rest are
/// opt-in. Every shard binds its own set (nothing is shared but the read-only
/// TLS config), which is how the kernel load-balances connections across cores.
pub(super) struct Listeners {
	tcp: TcpListener,
	ws: Option<TcpListener>,
	pub(super) mqtts: Option<TcpListener>,
	pub(super) wss: Option<TcpListener>,
}

impl Listeners {
	/// Binds every configured listener for this shard, or `None` if a
	/// configured-but-unbindable port aborts the shard (a fatal misconfiguration).
	pub(super) fn bind(config: &Config, shard_id: usize) -> Option<Self> {
		let (recv_buf, send_buf) = (
			config.server.socket_recv_buffer,
			config.server.socket_send_buffer,
		);
		let bind_one = |port: u16, what: &str| -> Option<TcpListener> {
			let addr = SocketAddr::new(config.server.bind, port);
			match bind_listener(addr, config.server.listen_backlog, recv_buf, send_buf) {
				Ok(l) => Some(l),
				Err(e) => {
					tracing::error!(shard = shard_id, error = %e, "failed to bind {what} listener");
					None
				}
			}
		};
		let bind_optional = |port: Option<u16>, what: &str| -> Result<Option<TcpListener>, ()> {
			match port {
				None => Ok(None),
				Some(p) => bind_one(p, what).map(Some).ok_or(()),
			}
		};

		let tcp = bind_one(config.server.port, "TCP")?;
		let (ws, mqtts, wss) = match (
			bind_optional(config.server.websocket_port(), "WebSocket"),
			bind_optional(config.tls.mqtts_port(), "mqtts"),
			bind_optional(config.tls.wss_port(), "wss"),
		) {
			(Ok(ws), Ok(mqtts), Ok(wss)) => (ws, mqtts, wss),
			_ => return None,
		};
		Some(Self { tcp, ws, mqtts, wss })
	}
}

/// Per-shard live-connection accounting: the total count plus a per-IP tally.
/// Single-threaded (shard-local), so plain `Cell`/`RefCell` suffice — no locks.
#[derive(Default)]
pub(super) struct ConnCounts {
	total: Cell<usize>,
	per_ip: RefCell<HashMap<IpAddr, usize>>,
}

impl ConnCounts {
	/// The number of live connections on this shard.
	pub(super) fn live(&self) -> usize {
		self.total.get()
	}
}

/// RAII counter for a live connection slot. Incremented on [`acquire`](Self::acquire)
/// and decremented on drop, so both the total and per-IP counts stay balanced even
/// if the connection task panics and unwinds.
struct ConnSlot {
	counts: Rc<ConnCounts>,
	ip: Option<IpAddr>,
}

impl ConnSlot {
	fn acquire(counts: Rc<ConnCounts>, ip: Option<IpAddr>) -> Self {
		counts.total.set(counts.total.get() + 1);
		if let Some(ip) = ip {
			*counts.per_ip.borrow_mut().entry(ip).or_insert(0) += 1;
		}
		Self { counts, ip }
	}
}

impl Drop for ConnSlot {
	fn drop(&mut self) {
		self.counts
			.total
			.set(self.counts.total.get().saturating_sub(1));
		if let Some(ip) = self.ip {
			let mut map = self.counts.per_ip.borrow_mut();
			if let Some(n) = map.get_mut(&ip) {
				*n -= 1;
				if *n == 0 {
					map.remove(&ip); // keep the map bounded by live IPs only
				}
			}
		}
	}
}

/// One turn of the accept loop: a connection on one of the listeners, or a
/// periodic tick that lets the loop re-check the shutdown flag.
enum AcceptTurn {
	Conn(TcpStream, Transport),
	Failed,
	Tick,
}

/// Accepts one connection from an optional listener, tagged with its transport. An
/// absent listener yields a future that never resolves, so it simply drops out of
/// the `or` race between the listeners.
async fn accept_on(listener: Option<&TcpListener>, transport: Transport, shard_id: usize) -> AcceptTurn {
	match listener {
		Some(l) => match l.accept().await {
			Ok(stream) => AcceptTurn::Conn(stream, transport),
			Err(e) => {
				tracing::warn!(shard = shard_id, error = %e, "accept failed");
				AcceptTurn::Failed
			}
		},
		None => std::future::pending().await,
	}
}

/// The shard's accept loop: races every listener against a shutdown-poll tick,
/// applies admission control and the connection caps, and spawns one task per
/// admitted socket. Returns when the shutdown flag is set.
pub(super) async fn accept_loop(
	ctx: &ConnCtx,
	listeners: &Listeners,
	tls_acceptor: &Rc<RefCell<Option<TlsAcceptor>>>,
	load: &Rc<LoadMonitor>,
	config: &Config,
	counts: &Rc<ConnCounts>,
) {
	let shard_id = ctx.shard_id;
	let max_conns = config.limits.max_connections_per_shard;
	let max_conns_per_ip = config.limits.max_connections_per_ip;
	// Admission control: reject new connections while this shard is overloaded.
	let admission_delay = Duration::from_millis(u64::from(config.overload.admission_delay_ms));

	while !ctx.shutdown.load(Ordering::Relaxed) {
		// Race every listener against a periodic tick so a shutdown signal is noticed
		// even while no client is connecting. `.or` polls in order, so a ready
		// connection is never lost to the tick; absent listeners never resolve.
		let tick = async {
			glommio::timer::sleep(SHUTDOWN_POLL_INTERVAL).await;
			AcceptTurn::Tick
		};
		let turn = accept_on(Some(&listeners.tcp), Transport::Plain, shard_id)
			.or(accept_on(
				listeners.ws.as_ref(),
				Transport::WebSocket,
				shard_id,
			))
			.or(accept_on(
				listeners.mqtts.as_ref(),
				Transport::Mqtts,
				shard_id,
			))
			.or(accept_on(listeners.wss.as_ref(), Transport::Wss, shard_id))
			.or(tick)
			.await;

		let (stream, transport) = match turn {
			AcceptTurn::Conn(stream, transport) => (stream, transport),
			AcceptTurn::Failed | AcceptTurn::Tick => continue,
		};

		// Disable Nagle on the accepted socket *explicitly*. The listener sets
		// TCP_NODELAY (`transport::tcp`) and Linux happens to inherit it, but MQTT is
		// request/response — a coalesced small PUBACK/PUBLISH costs a round-trip of
		// latency — so guarantee it per-connection rather than lean on kernel
		// inheritance that isn't contractual across platforms/versions.
		let _ = stream.set_nodelay(true);

		let peer_ip = stream.peer_addr().ok().map(|a| a.ip());

		// Admission control: while the shard's scheduling delay is over budget, shed
		// load at the door. The rejected client retries — from a new source port, so
		// SO_REUSEPORT may hash it onto a cooler shard. Existing connections are left
		// alone, so overload doesn't cascade into dropping healthy sessions.
		if load.exceeds(admission_delay) {
			ctx.metrics.record_admission_rejected();
			tracing::debug!(shard = shard_id, "overloaded, rejecting new connection");
			drop(stream);
			continue;
		}

		if counts.total.get() >= max_conns {
			tracing::warn!(
				shard = shard_id,
				max = max_conns,
				"max connections per shard reached, rejecting"
			);
			drop(stream); // closes the socket
			continue;
		}

		// Per-source connection cap: bound how many concurrent connections one client
		// IP may hold on this shard, so a single host can't monopolise the slots. Off
		// when 0. Note it is per-shard (SO_REUSEPORT spreads a source across shards) and
		// only meaningful when clients connect directly — behind a reverse proxy every
		// connection shares the proxy's IP, so rely on the proxy/network layer there.
		if max_conns_per_ip > 0
			&& let Some(ip) = peer_ip
			&& counts.per_ip.borrow().get(&ip).copied().unwrap_or(0) >= max_conns_per_ip
		{
			tracing::warn!(
				shard = shard_id,
				%ip,
				max = max_conns_per_ip,
				"per-IP connection limit reached, rejecting"
			);
			drop(stream);
			continue;
		}

		// Acquire the slot via an RAII guard: it decrements the total and per-IP counts
		// on drop, so slots are reclaimed on *every* task exit — including a panic
		// unwind. A manual decrement after `.await` would be skipped on panic, slowly
		// leaking slots until the shard stops accepting (a slot-exhaustion DoS).
		let slot = ConnSlot::acquire(counts.clone(), peer_ip);

		let ctx = ctx.clone();
		// Snapshot the acceptor current at accept time; a concurrent hot-reload swaps
		// the shared cell but never disturbs a connection already handed its clone.
		let tls_acceptor = tls_acceptor.borrow().clone();
		let (is_tls, is_websocket) = transport.flags();
		let span = tracing::info_span!(
			"connection",
			shard = shard_id,
			tls = is_tls,
			websocket = is_websocket,
			client_id = tracing::field::Empty,
		);
		// NOTE: keep this task future small — it lives for the whole connection.
		// The heavy per-transport state machines are boxed inside `serve` (see
		// the comments there); this wrapper measures ~600 bytes.
		glommio::spawn_local(
			async move {
				let _slot = slot;
				serve(ctx, stream, transport, tls_acceptor).await;
			}
			.instrument(span),
		)
		.detach();
	}
}
