use crate::broker::engine::ShardState;
use crate::config::Config;
use crate::net::socket::create_socket;
use crate::net::tcp_listener::create_tcp_listener;
use crate::server::connection::Connection;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use mqttbytes::v5::Publish;
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use tracing::Instrument;

pub async fn init(mesh: MeshBuilder<Publish, Full>, config: Arc<Config>) {
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

	tracing::info!(shard = shard_id, "shard ready, accepting connections");

	let limits = config.limits;
	let max_conns = limits.max_connections_per_shard;
	// Shard-local live-connection counter (single-threaded, so a plain Cell).
	let conn_count = Rc::new(Cell::new(0usize));

	loop {
		match tcp_listener.accept().await {
			Ok(stream) => {
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
						let mut connection = Connection::new(stream, shard_id, state, limits);
						let _ = connection.run().await;
						counter.set(counter.get() - 1);
					}
					.instrument(span),
				)
				.detach();
			}
			Err(e) => tracing::warn!(shard = shard_id, error = %e, "accept failed"),
		}
	}
}
