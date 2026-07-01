use crate::auth::Authenticator;
use crate::broker::engine::ShardState;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::net::socket::create_socket;
use crate::net::tcp_listener::create_tcp_listener;
use crate::server::connection::Connection;
use futures_lite::FutureExt;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::net::TcpStream;
use mqttbytes::{QoS, v5::Publish};
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::Instrument;

/// How often each shard reclaims suspended sessions past their expiry deadline.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How often the accept loop wakes to check the shutdown flag. Bounds the
/// shutdown latency; kept coarse so it barely touches the steady-state hot path.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// One turn of the accept loop: either a connection outcome, or a periodic tick
/// that lets the loop re-check the shutdown flag while `accept()` is blocked.
enum AcceptTurn {
	Accepted(TcpStream),
	Failed,
	Tick,
}

pub async fn init(
	mesh: MeshBuilder<Publish, Full>,
	config: Arc<Config>,
	shutdown: Arc<AtomicBool>,
	metrics: Arc<Metrics>,
) {
	let shard_id: usize = glommio::executor().id();
	let socket_addr = SocketAddr::new(config.server.bind, config.server.port);

	// Join the full mesh. This rendezvous blocks until every shard has joined.
	let (senders, mut receivers) = match mesh.join().await {
		Ok(pair) => pair,
		Err(_) => {
			tracing::error!(shard = shard_id, "failed to join the channel mesh");
			return;
		}
	};

	let socket = match create_socket(socket_addr, config.server.listen_backlog) {
		Ok(l) => l,
		Err(e) => {
			tracing::error!(shard = shard_id, error = %e, "failed to bind listener");
			return;
		}
	};

	let tcp_listener = create_tcp_listener(socket);

	// Mesh peer id is 0-based and unique per shard (glommio executor ids are
	// 1-based, so they can't be used to pick a single shard). Peer 0 is elected
	// to publish the broker-wide `$SYS` metrics.
	let mesh_peer_id = senders.peer_id();

	// Shard-local broker state. Shared by Rc between every connection on this
	// shard; never crosses the core boundary, so no locking is required.
	let state = ShardState::new();
	state.borrow_mut().set_mesh(senders);

	// Drain inbound cross-shard publishes: re-wrap each in Rc and fan it out to
	// this shard's local subscribers, exactly as a local publish would be.
	for (_producer, receiver) in receivers.streams() {
		let state = state.clone();
		glommio::spawn_local(async move {
			while let Some(publish) = receiver.recv().await {
				state.borrow_mut().deliver_local(publish);
			}
		})
		.detach();
	}

	// Periodically reclaim suspended sessions whose expiry has lapsed.
	{
		let state = state.clone();
		glommio::spawn_local(async move {
			loop {
				glommio::timer::sleep(SESSION_SWEEP_INTERVAL).await;
				state.borrow_mut().sweep_expired();
			}
		})
		.detach();
	}

	// One shard owns publishing `$SYS` metrics (they are broker-wide totals, so a
	// single publisher avoids duplicates). Messages are retained and broadcast to
	// every shard, so any `$SYS/#` subscriber — on any shard — sees them.
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
					shard_state.deliver_local(publish);
				}
			}
		})
		.detach();
	}

	tracing::info!(shard = shard_id, "shard ready, accepting connections");

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
	// Shard-local live-connection counter (single-threaded, so a plain Cell).
	let conn_count = Rc::new(Cell::new(0usize));

	while !shutdown.load(Ordering::Relaxed) {
		// Race the (otherwise unbounded) accept against a periodic tick so a
		// shutdown signal is noticed even while no client is connecting. `.or`
		// polls the accept first, so a ready connection is never lost to the tick.
		let turn = {
			let accept = async {
				match tcp_listener.accept().await {
					Ok(stream) => AcceptTurn::Accepted(stream),
					Err(e) => {
						tracing::warn!(shard = shard_id, error = %e, "accept failed");
						AcceptTurn::Failed
					}
				}
			};
			let tick = async {
				glommio::timer::sleep(SHUTDOWN_POLL_INTERVAL).await;
				AcceptTurn::Tick
			};
			accept.or(tick).await
		};

		let stream = match turn {
			AcceptTurn::Accepted(stream) => stream,
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
		// One span per connection. `client_id` is recorded later, once the
		// client sends CONNECT, so every log line for this connection — on
		// either side of an `.await` — automatically carries it.
		let span = tracing::info_span!(
			"connection",
			shard = shard_id,
			client_id = tracing::field::Empty,
		);
		glommio::spawn_local(
			async move {
				let mut connection =
					Connection::new(stream, shard_id, state, limits, auth, metrics);
				let _ = connection.run().await;
				counter.set(counter.get() - 1);
			}
			.instrument(span),
		)
		.detach();
	}

	tracing::info!(shard = shard_id, "shutdown signal received, stopping accept loop");
}
