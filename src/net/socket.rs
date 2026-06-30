use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;

pub fn create_socket(address: SocketAddr, backlog: i32) -> std::io::Result<Socket> {
    let domain = match address.is_ipv4() {
        true => Domain::IPV4,
        false => Domain::IPV6,
    };

	let socket: Socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).expect("Failed to create socket");
	if address.is_ipv6() {
		socket.set_only_v6(false)?;
	}

	// SO_REUSEPORT lets every shard bind the same address; the kernel load-
	// balances incoming connections across them, so there's no shared accept
	// socket and no cross-core contention on the listener.
	socket.set_reuse_address(true)?;
	socket.set_reuse_port(true)?;

	socket.set_tcp_nodelay(true)?;

	socket.set_nonblocking(true)?;

	socket.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(std::time::Duration::from_secs(60)))?;

	socket.bind(&address.into())?;
	socket.listen(backlog)?;

	Ok(socket)
}
