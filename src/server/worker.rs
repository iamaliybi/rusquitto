use crate::net::socket::create_socket;
use crate::net::tcp_listener::create_tcp_listener;
use crate::server::connection::Connection;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub async fn init() {
	let shard_id: usize = glommio::executor().id();
	let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1883);

	let socket = match create_socket(socket_addr) {
		Ok(l) => l,
		Err(e) => {
			eprintln!("Shard {} failed to bind: {}", shard_id, e);
			return;
		}
	};

	let tcp_listener = create_tcp_listener(socket);

	// Server Accept Loop: Continuously listens for and accepts new incoming TCP connections.
	loop {
		match tcp_listener.accept().await {
			Ok(stream) => {
				glommio::spawn_local(async move {
					let mut connection = Connection::new(stream, shard_id);
					connection.run().await
				})
				.detach();
			}
			Err(e) => eprintln!("Accept error on shard {}: {}", shard_id, e),
		}
	}
}
