use std::os::fd::{FromRawFd, IntoRawFd};
use glommio::net::TcpListener;
use socket2::Socket;

pub fn create_tcp_listener(socket: Socket) -> TcpListener {
    unsafe {
        // We explicitly guarantee that 'fd' is a valid, open TCP socket.
        TcpListener::from_raw_fd(socket.into_raw_fd())
    }
}