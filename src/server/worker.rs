use crate::auth::Authenticator;
use crate::broker::mesh::MeshMsg;
use crate::broker::shard::ShardState;
use crate::config::Config;
use crate::server::connection::Connection;
use crate::telemetry::metrics::Metrics;
use crate::transport::ByteStream;
use crate::transport::tcp::bind_listener;
use crate::transport::tls;
use crate::transport::websocket::WsStream;
use futures_lite::FutureExt;
use futures_rustls::TlsAcceptor;
use futures_rustls::rustls::ServerConfig;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::net::{TcpListener, TcpStream};
use mqttbytes::{QoS, v5::Publish};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::Instrument;

/// How often each shard reclaims suspended sessions past their expiry deadline.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How often the accept loop wakes to check the shutdown flag while `accept()` is
/// otherwise blocked. Bounds shutdown latency; coarse so it barely touches the hot path.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Longest a shard waits for its connections to drain during shutdown.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// How often the drain loop re-checks the live-connection count.
const SHUTDOWN_DRAIN_POLL: Duration = Duration::from_millis(25);

/// Which listener an accepted socket arrived on, deciding the transport stack the
/// MQTT session runs over.
#[derive(Clone, Copy)]
enum Transport {
	/// Plain MQTT over TCP (`mqtt://`).
	Plain,
	/// MQTT over WebSocket (`ws://`).
	WebSocket,
	/// MQTT over TLS (`mqtts://`).
	Mqtts,
	/// MQTT over WebSocket over TLS (`wss://`).
	Wss,
}

impl Transport {
	/// `(is_tls, is_websocket)` for connection-span fields.
	fn flags(self) -> (bool, bool) {
		match self {
			Transport::Plain => (false, false),
			Transport::WebSocket => (false, true),
			Transport::Mqtts => (true, false),
			Transport::Wss => (true, true),
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

/// Per-shard live-connection accounting: the total count plus a per-IP tally.
/// Single-threaded (shard-local), so plain `Cell`/`RefCell` suffice — no locks.
#[derive(Default)]
struct ConnCounts {
	total: Cell<usize>,
	per_ip: RefCell<HashMap<IpAddr, usize>>,
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

pub async fn init(
	mesh: MeshBuilder<MeshMsg, Full>,
	config: Arc<Config>,
	shutdown: Arc<AtomicBool>,
	metrics: Arc<Metrics>,
	tls_config: Option<Arc<ServerConfig>>,
) {
	let shard_id: usize = glommio::executor().id();

	// Join the full mesh. This rendezvous blocks until every shard has joined.
	let (senders, mut receivers) = match mesh.join().await {
		Ok(pair) => pair,
		Err(_) => {
			tracing::error!(shard = shard_id, "failed to join the channel mesh");
			return;
		}
	};

	let tcp_addr = SocketAddr::new(config.server.bind, config.server.port);
	let tcp_listener = match bind_listener(tcp_addr, config.server.listen_backlog) {
		Ok(l) => l,
		Err(e) => {
			tracing::error!(shard = shard_id, error = %e, "failed to bind TCP listener");
			return;
		}
	};

	// Binds an optional listener on `config.server.bind:port`, aborting the shard on
	// failure (a configured-but-unbindable port is a fatal misconfiguration).
	let bind_optional = |port: Option<u16>, what: &str| -> std::result::Result<Option<TcpListener>, ()> {
		let Some(port) = port else {
			return Ok(None);
		};
		let addr = SocketAddr::new(config.server.bind, port);
		match bind_listener(addr, config.server.listen_backlog) {
			Ok(l) => Ok(Some(l)),
			Err(e) => {
				tracing::error!(shard = shard_id, error = %e, "failed to bind {what} listener");
				Err(())
			}
		}
	};

	// Optional WebSocket listener for browser clients, plus the TLS listeners
	// (mqtts / wss). The acceptor wraps the shared, immutable rustls config; each
	// shard still binds its own SO_REUSEPORT sockets, so nothing is shared but the
	// read-only config.
	let (ws_listener, mqtts_listener, wss_listener) = match (
		bind_optional(config.server.websocket_port(), "WebSocket"),
		bind_optional(config.tls.mqtts_port(), "mqtts"),
		bind_optional(config.tls.wss_port(), "wss"),
	) {
		(Ok(ws), Ok(mqtts), Ok(wss)) => (ws, mqtts, wss),
		_ => return,
	};
	let tls_acceptor = tls_config.map(TlsAcceptor::from);

	// Mesh peer id is 0-based and unique per shard (glommio executor ids are
	// 1-based). Peer 0 publishes broker-wide `$SYS` metrics.
	let mesh_peer_id = senders.peer_id();

	// Shard-local broker state, shared by Rc between every connection on this shard.
	let state = ShardState::new();
	{
		let mut s = state.borrow_mut();
		s.set_mesh(senders);
		s.set_retained_limit(config.limits.max_retained_messages);
	}

	// Drain inbound cross-shard messages into local fan-out / migration handling.
	for (_producer, receiver) in receivers.streams() {
		let state = state.clone();
		glommio::spawn_local(async move {
			while let Some(msg) = receiver.recv().await {
				match msg {
					MeshMsg::Publish(publish) => state.borrow_mut().deliver_local(publish, None),
					MeshMsg::Control(control) => state.borrow_mut().on_control(*control),
				}
			}
		})
		.detach();
	}

	// Periodically reclaim suspended sessions whose expiry has lapsed and publish any
	// delayed wills that have now come due (best-effort mesh forward, like `$SYS`).
	{
		let state = state.clone();
		glommio::spawn_local(async move {
			loop {
				glommio::timer::sleep(SESSION_SWEEP_INTERVAL).await;
				let wills = state.borrow_mut().sweep_expired();
				for will in wills {
					let mut shard_state = state.borrow_mut();
					shard_state.broadcast(&will);
					shard_state.deliver_local(will, None);
				}
			}
		})
		.detach();
	}

	// One shard owns publishing `$SYS` metrics (broker-wide totals). Messages are
	// retained and broadcast to every shard, so any `$SYS/#` subscriber sees them.
	if mesh_peer_id == 0 && config.sys.enabled {
		let state = state.clone();
		let metrics = metrics.clone();
		let interval = Duration::from_secs(config.sys.interval);
		glommio::spawn_local(async move {
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
		})
		.detach();
	}

	tracing::info!(
		shard = shard_id,
		websocket = config.server.websocket,
		mqtts = mqtts_listener.is_some(),
		wss = wss_listener.is_some(),
		"shard ready, accepting connections"
	);

	let limits = config.limits;
	let max_conns = limits.max_connections_per_shard;
	let max_conns_per_ip = limits.max_connections_per_ip;
	// Shard-local credential store, shared by every connection on this shard.
	let auth = Rc::new(Authenticator::from_config(&config.auth));
	if mesh_peer_id == 0 {
		tracing::info!(
			enforced = !auth.is_open(),
			users = config.auth.users.len(),
			allow_anonymous = config.auth.allow_anonymous,
			"authentication configured"
		);
		// Warn if idle-connection protection is off: with no server keep-alive and a
		// client that also sends keep-alive 0, an idle/stalled connection is never reaped.
		if limits.keep_alive == 0 {
			tracing::warn!(
				"limits.keep_alive = 0 disables the server keep-alive override; idle \
				 connections are only reaped when the client sets its own keep-alive. \
				 Set keep_alive > 0 to guarantee idle/slow connections are dropped."
			);
		}
	}
	let counts = Rc::new(ConnCounts::default());

	while !shutdown.load(Ordering::Relaxed) {
		// Race every listener against a periodic tick so a shutdown signal is noticed
		// even while no client is connecting. `.or` polls in order, so a ready
		// connection is never lost to the tick; absent listeners never resolve.
		let tick = async {
			glommio::timer::sleep(SHUTDOWN_POLL_INTERVAL).await;
			AcceptTurn::Tick
		};
		let turn = accept_on(Some(&tcp_listener), Transport::Plain, shard_id)
			.or(accept_on(
				ws_listener.as_ref(),
				Transport::WebSocket,
				shard_id,
			))
			.or(accept_on(
				mqtts_listener.as_ref(),
				Transport::Mqtts,
				shard_id,
			))
			.or(accept_on(wss_listener.as_ref(), Transport::Wss, shard_id))
			.or(tick)
			.await;

		let (stream, transport) = match turn {
			AcceptTurn::Conn(stream, transport) => (stream, transport),
			AcceptTurn::Failed | AcceptTurn::Tick => continue,
		};

		let peer_ip = stream.peer_addr().ok().map(|a| a.ip());

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

		let state = state.clone();
		let auth = auth.clone();
		let metrics = metrics.clone();
		let shutdown = shutdown.clone();
		let tls_acceptor = tls_acceptor.clone();
		let (is_tls, is_websocket) = transport.flags();
		let span = tracing::info_span!(
			"connection",
			shard = shard_id,
			tls = is_tls,
			websocket = is_websocket,
			client_id = tracing::field::Empty,
		);
		glommio::spawn_local(
			async move {
				let _slot = slot;
				serve(
					stream,
					transport,
					tls_acceptor,
					shard_id,
					state,
					limits,
					auth,
					metrics,
					shutdown,
				)
				.await;
			}
			.instrument(span),
		)
		.detach();
	}

	// Drain: wake every live connection so it sends DISCONNECT and cleans up, then
	// wait (bounded) for them to finish before returning.
	let live = counts.total.get();
	tracing::info!(
		shard = shard_id,
		connections = live,
		"shutdown signal received, draining connections"
	);
	state.borrow_mut().shutdown_connections();

	let deadline = Instant::now() + SHUTDOWN_GRACE;
	while counts.total.get() > 0 && Instant::now() < deadline {
		glommio::timer::sleep(SHUTDOWN_DRAIN_POLL).await;
	}
	tracing::info!(
		shard = shard_id,
		remaining = counts.total.get(),
		"shard stopped"
	);
}

/// Serves one accepted socket, building the transport stack it arrived on and then
/// driving the MQTT session over the resulting [`ByteStream`]. TLS and WebSocket
/// each run a handshake first, bounded by the connect timeout so a stalled
/// handshake can't hold the connection open; `wss` stacks both (TLS, then the
/// WebSocket upgrade over the encrypted channel).
#[allow(clippy::too_many_arguments)]
async fn serve(
	stream: TcpStream,
	transport: Transport,
	tls_acceptor: Option<TlsAcceptor>,
	shard_id: usize,
	state: Rc<std::cell::RefCell<ShardState>>,
	limits: crate::config::LimitsConfig,
	auth: Rc<Authenticator>,
	metrics: Arc<Metrics>,
	shutdown: Arc<AtomicBool>,
) {
	let timeout = Duration::from_secs(u64::from(limits.connect_timeout));
	let max_frame = limits.max_payload_size;

	match transport {
		Transport::Plain => run_stream(stream, shard_id, state, limits, auth, metrics, shutdown).await,
		Transport::WebSocket => match WsStream::accept(stream, max_frame, timeout).await {
			Ok(ws) => run_stream(ws, shard_id, state, limits, auth, metrics, shutdown).await,
			Err(e) => tracing::warn!(error = %e, "WebSocket handshake failed"),
		},
		Transport::Mqtts => {
			let Some(acceptor) = tls_acceptor else {
				return; // Unreachable: an mqtts listener only exists with an acceptor.
			};
			match tls::accept(&acceptor, stream, timeout).await {
				Ok(tls) => run_stream(tls, shard_id, state, limits, auth, metrics, shutdown).await,
				Err(e) => tracing::warn!(error = %e, "TLS handshake failed"),
			}
		}
		Transport::Wss => {
			let Some(acceptor) = tls_acceptor else {
				return;
			};
			match tls::accept(&acceptor, stream, timeout).await {
				Ok(tls) => match WsStream::accept(tls, max_frame, timeout).await {
					Ok(ws) => run_stream(ws, shard_id, state, limits, auth, metrics, shutdown).await,
					Err(e) => tracing::warn!(error = %e, "WebSocket handshake over TLS failed"),
				},
				Err(e) => tracing::warn!(error = %e, "TLS handshake failed"),
			}
		}
	}
}

/// Drives the MQTT state machine over an established stream to completion. Generic
/// over the transport (the payoff of [`ByteStream`]): one implementation serves
/// plain TCP, WebSocket, TLS, and WebSocket-over-TLS alike.
async fn run_stream<S: ByteStream>(
	stream: S,
	shard_id: usize,
	state: Rc<std::cell::RefCell<ShardState>>,
	limits: crate::config::LimitsConfig,
	auth: Rc<Authenticator>,
	metrics: Arc<Metrics>,
	shutdown: Arc<AtomicBool>,
) {
	let mut conn = Connection::new(stream, shard_id, state, limits, auth, metrics, shutdown);
	let _ = conn.run().await;
}
