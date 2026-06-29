use std::os::fd::{FromRawFd, IntoRawFd};
use glommio::net::TcpListener;
use socket2::Socket;

pub fn create_tcp_listener(socket: Socket) -> TcpListener {
    // SAFETY: `into_raw_fd` yields a valid, open TCP socket fd whose ownership we
    // transfer directly to the listener.
    unsafe { TcpListener::from_raw_fd(socket.into_raw_fd()) }
}