use crate::auth::Authenticator;
use crate::broker::mesh::MeshMsg;
use crate::broker::shard::ShardState;
use crate::config::Config;
use crate::persistence;
use crate::server::connection::Connection;
use crate::server::overload::LoadMonitor;
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
use glommio::{Latency, Shares};
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

/// How often the load probe samples the reactor's scheduling delay. Short enough
/// to react within a second, long enough to be negligible overhead.
const LOAD_PROBE_INTERVAL: Duration = Duration::from_millis(100);

/// How often the load-shedding task re-evaluates whether to shed connections.
const SHED_INTERVAL: Duration = Duration::from_secs(1);

/// CPU shares for the background maintenance task queue (sweep, `$SYS`, shedding),
/// relative to the default queue's 1000. Low, so housekeeping yields to
/// client-serving work when a shard is busy — the scheduling-group idea from
/// Seastar/ScyllaDB, where background work is starved to protect foreground latency.
const MAINTENANCE_SHARES: usize = 200;

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

	// A low-share, latency-insensitive task queue for background housekeeping
	// (session sweep, `$SYS`, shedding). Under load the scheduler starves it in
	// favour of the default queue that serves clients — the scheduling-group
	// pattern from Seastar/ScyllaDB.
	let tq_maintenance = glommio::executor().create_task_queue(
		Shares::Static(MAINTENANCE_SHARES),
		Latency::NotImportant,
		"maintenance",
	);

	// Per-shard load signal (reactor scheduling delay), driving the stall WARN,
	// admission control, and shedding.
	let load = LoadMonitor::new();

	// Shard-local broker state, shared by Rc between every connection on this shard.
	let state = ShardState::new();
	{
		let mut s = state.borrow_mut();
		s.set_mesh(senders);
		s.set_retained_limit(config.limits.max_retained_messages);
	}

	// Restore persisted retained messages before accepting, so early subscribers see
	// them. Every shard loads the same snapshot into its own table (retained is
	// replicated across shards), so no cross-shard coordination is needed.
	if config.persistence.enabled {
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
		glommio::spawn_local_into(
			async move {
				loop {
					glommio::timer::sleep(SESSION_SWEEP_INTERVAL).await;
					let wills = state.borrow_mut().sweep_expired();
					for will in wills {
						let mut shard_state = state.borrow_mut();
						shard_state.broadcast(&will);
						shard_state.deliver_local(will, None);
					}
				}
			},
			tq_maintenance,
		)
		.expect("spawn session-sweep task")
		.detach();
	}

	// Load probe: measure how far the reactor's scheduling of a normal-priority task
	// slips past its due time — near zero when idle, growing under saturation — and
	// feed it into the shard's load monitor and the `$SYS` gauge. Runs on the default
	// (foreground) queue so it observes the delay client-serving work actually sees.
	{
		let load = load.clone();
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

	// Load shedding: while a shard stays overloaded, close a small batch of its
	// connections each interval so they reconnect and SO_REUSEPORT rehashes them onto
	// (probably) cooler cores. Opt-in and disruptive, so only spawned when enabled.
	if config.overload.shed_delay_ms > 0 {
		let load = load.clone();
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
			tq_maintenance,
		)
		.expect("spawn load-shedding task")
		.detach();
	}

	// Peer 0 owns persisting the retained snapshot (all shards hold identical
	// copies). Periodic snapshots run on the background queue; a final one is written
	// on graceful shutdown at the end of this function.
	if config.persistence.enabled && mesh_peer_id == 0 {
		if let Err(e) = std::fs::create_dir_all(&config.persistence.dir) {
			tracing::error!(error = %e, dir = %config.persistence.dir.display(), "failed to create persistence dir");
		}
		let snapshot_secs = config.persistence.snapshot_interval;
		if snapshot_secs > 0 {
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
				tq_maintenance,
			)
			.expect("spawn persistence snapshot task")
			.detach();
		}
	}

	// One shard owns publishing `$SYS` metrics (broker-wide totals). Messages are
	// retained and broadcast to every shard, so any `$SYS/#` subscriber sees them.
	if mesh_peer_id == 0 && config.sys.enabled {
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
			tq_maintenance,
		)
		.expect("spawn $SYS metrics task")
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
	// Admission control: reject new connections while this shard is overloaded.
	let admission_delay = Duration::from_millis(u64::from(config.overload.admission_delay_ms));
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

		// Admission control: while the shard's scheduling delay is over budget, shed
		// load at the door. The rejected client retries — from a new source port, so
		// SO_REUSEPORT may hash it onto a cooler shard. Existing connections are left
		// alone, so overload doesn't cascade into dropping healthy sessions.
		if load.exceeds(admission_delay) {
			metrics.record_admission_rejected();
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

	// Final retained snapshot on graceful shutdown (peer 0), capturing anything since
	// the last periodic write so a clean stop loses nothing.
	if config.persistence.enabled && mesh_peer_id == 0 {
		let messages = state.borrow().retained_messages();
		match persistence::save_retained(&config.persistence.retained_path(), &messages).await {
			Ok(()) => {
				tracing::info!(
					shard = shard_id,
					retained = messages.len(),
					"wrote final retained snapshot"
				)
			}
			Err(e) => tracing::error!(shard = shard_id, error = %e, "final retained snapshot failed"),
		}
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
