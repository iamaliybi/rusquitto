use crate::broker::engine::ShardState;
use crate::net::socket::create_socket;
use crate::net::tcp_listener::create_tcp_listener;
use crate::server::connection::Connection;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use mqttbytes::v5::Publish;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tracing::Instrument;

pub async fn init(mesh: MeshBuilder<Publish, Full>) {
	let shard_id: usize = glommio::executor().id();
	let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1883);

	// Join the full mesh. This rendezvous blocks until every shard has joined.
	let (senders, mut receivers) = match mesh.join().await {
		Ok(pair) => pair,
		Err(_) => {
			tracing::error!(shard = shard_id, "failed to join the channel mesh");
			return;
		}
	};

	let socket = match create_socket(socket_addr) {
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

	// Server Accept Loop: Continuously listens for and accepts new incoming TCP connections.
	loop {
		match tcp_listener.accept().await {
			Ok(stream) => {
				let state = state.clone();
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
						let mut connection = Connection::new(stream, shard_id, state);
						connection.run().await
					}
					.instrument(span),
				)
				.detach();
			}
			Err(e) => tracing::warn!(shard = shard_id, error = %e, "accept failed"),
		}
	}
}
