use crate::auth::Authenticator;
use crate::broker::mesh::MeshMsg;
use crate::broker::shard::ShardState;
use crate::config::Config;
use crate::server::connection::Connection;
use crate::telemetry::metrics::Metrics;
use crate::transport::tcp::bind_listener;
use crate::transport::websocket::WsStream;
use futures_lite::FutureExt;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::net::{TcpListener, TcpStream};
use mqttbytes::{QoS, v5::Publish};
use std::cell::Cell;
use std::net::SocketAddr;
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

/// One turn of the accept loop: a connection on one of the listeners, or a
/// periodic tick that lets the loop re-check the shutdown flag.
enum AcceptTurn {
	Tcp(TcpStream),
	WebSocket(TcpStream),
	Failed,
	Tick,
}

pub async fn init(
	mesh: MeshBuilder<MeshMsg, Full>,
	config: Arc<Config>,
	shutdown: Arc<AtomicBool>,
	metrics: Arc<Metrics>,
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

	// Optional WebSocket listener for browser clients.
	let ws_listener: Option<TcpListener> = match config.server.websocket_port() {
		Some(port) => {
			let addr = SocketAddr::new(config.server.bind, port);
			match bind_listener(addr, config.server.listen_backlog) {
				Ok(l) => Some(l),
				Err(e) => {
					tracing::error!(shard = shard_id, error = %e, "failed to bind WebSocket listener");
					return;
				}
			}
		}
		None => None,
	};

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
		"shard ready, accepting connections"
	);

	let limits = config.limits;
	let max_conns = limits.max_connections_per_shard;
	// Shard-local credential store, shared by every connection on this shard.
	let auth = Rc::new(Authenticator::from_config(&config.auth));
	if mesh_peer_id == 0 {
		tracing::info!(
			enforced = !auth.is_open(),
			users = config.auth.users.len(),
			allow_anonymous = config.auth.allow_anonymous,
			"authentication configured"
		);
	}
	let conn_count = Rc::new(Cell::new(0usize));

	while !shutdown.load(Ordering::Relaxed) {
		// Race the accept(s) against a periodic tick so a shutdown signal is noticed
		// even while no client is connecting. `.or` polls in order, so a ready
		// connection is never lost to the tick.
		let turn = {
			let accept_tcp = async {
				match tcp_listener.accept().await {
					Ok(stream) => AcceptTurn::Tcp(stream),
					Err(e) => {
						tracing::warn!(shard = shard_id, error = %e, "TCP accept failed");
						AcceptTurn::Failed
					}
				}
			};
			let tick = async {
				glommio::timer::sleep(SHUTDOWN_POLL_INTERVAL).await;
				AcceptTurn::Tick
			};
			match &ws_listener {
				Some(ws) => {
					let accept_ws = async {
						match ws.accept().await {
							Ok(stream) => AcceptTurn::WebSocket(stream),
							Err(e) => {
								tracing::warn!(shard = shard_id, error = %e, "WebSocket accept failed");
								AcceptTurn::Failed
							}
						}
					};
					accept_tcp.or(accept_ws).or(tick).await
				}
				None => accept_tcp.or(tick).await,
			}
		};

		let (stream, is_websocket) = match turn {
			AcceptTurn::Tcp(stream) => (stream, false),
			AcceptTurn::WebSocket(stream) => (stream, true),
			AcceptTurn::Failed | AcceptTurn::Tick => continue,
		};

		if conn_count.get() >= max_conns {
			tracing::warn!(
				shard = shard_id,
				max = max_conns,
				"max connections per shard reached, rejecting"
			);
			drop(stream); // closes the socket
			continue;
		}
		conn_count.set(conn_count.get() + 1);

		let state = state.clone();
		let counter = conn_count.clone();
		let auth = auth.clone();
		let metrics = metrics.clone();
		let shutdown = shutdown.clone();
		let span = tracing::info_span!(
			"connection",
			shard = shard_id,
			websocket = is_websocket,
			client_id = tracing::field::Empty,
		);
		glommio::spawn_local(
			async move {
				serve(stream, is_websocket, shard_id, state, limits, auth, metrics, shutdown).await;
				counter.set(counter.get() - 1);
			}
			.instrument(span),
		)
		.detach();
	}

	// Drain: wake every live connection so it sends DISCONNECT and cleans up, then
	// wait (bounded) for them to finish before returning.
	let live = conn_count.get();
	tracing::info!(
		shard = shard_id,
		connections = live,
		"shutdown signal received, draining connections"
	);
	state.borrow_mut().shutdown_connections();

	let deadline = Instant::now() + SHUTDOWN_GRACE;
	while conn_count.get() > 0 && Instant::now() < deadline {
		glommio::timer::sleep(SHUTDOWN_DRAIN_POLL).await;
	}
	tracing::info!(shard = shard_id, remaining = conn_count.get(), "shard stopped");
}

/// Serves one accepted socket. A WebSocket socket first completes the RFC 6455
/// handshake (yielding a framed `ByteStream`); either way the same `Connection`
/// state machine drives the MQTT session over the resulting stream.
#[allow(clippy::too_many_arguments)]
async fn serve(
	stream: TcpStream,
	is_websocket: bool,
	shard_id: usize,
	state: Rc<std::cell::RefCell<ShardState>>,
	limits: crate::config::LimitsConfig,
	auth: Rc<Authenticator>,
	metrics: Arc<Metrics>,
	shutdown: Arc<AtomicBool>,
) {
	if is_websocket {
		match WsStream::accept(stream, limits.max_payload_size).await {
			Ok(ws) => {
				let mut conn =
					Connection::new(ws, shard_id, state, limits, auth, metrics, shutdown);
				let _ = conn.run().await;
			}
			Err(e) => tracing::warn!(error = %e, "WebSocket handshake failed"),
		}
	} else {
		let mut conn = Connection::new(stream, shard_id, state, limits, auth, metrics, shutdown);
		let _ = conn.run().await;
	}
}
