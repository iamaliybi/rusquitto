//! TCP listener setup: a `SO_REUSEPORT` socket per shard, adapted into a glommio
//! `TcpListener`.

use std::net::SocketAddr;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::time::Duration;

use glommio::net::TcpListener;
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};

/// Binds a non-blocking, `SO_REUSEPORT` TCP listener on `address`.
///
/// `SO_REUSEPORT` lets every shard bind the same address; the kernel load-balances
/// incoming connections across them, so there is no shared accept socket and no
/// cross-core contention on the listener.
pub fn bind_listener(address: SocketAddr, backlog: i32) -> std::io::Result<TcpListener> {
	let domain = if address.is_ipv4() {
		Domain::IPV4
	} else {
		Domain::IPV6
	};
	let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

	if address.is_ipv6() {
		socket.set_only_v6(false)?;
	}
	socket.set_reuse_address(true)?;
	socket.set_reuse_port(true)?;
	socket.set_tcp_nodelay(true)?;
	socket.set_nonblocking(true)?;
	socket.set_tcp_keepalive(&TcpKeepalive::new().with_time(Duration::from_secs(60)))?;

	socket.bind(&address.into())?;
	socket.listen(backlog)?;

	// SAFETY: `into_raw_fd` yields a valid, open, listening TCP socket fd whose
	// ownership transfers directly to the glommio listener.
	Ok(unsafe { TcpListener::from_raw_fd(socket.into_raw_fd()) })
}
