//! Network transports the broker speaks MQTT over.
//!
//! [`ByteStream`] is the abstraction the connection layer depends on (DIP): an
//! async, byte-oriented, bidirectional stream. Plain TCP satisfies it directly;
//! the WebSocket transport wraps a TCP stream in a frame codec that also satisfies
//! it. `Connection` is written once against `ByteStream` and works over either.

pub mod tcp;
pub mod websocket;

use std::io::Result;

use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;

/// An async, bidirectional byte stream — the only capability the connection layer
/// needs from a transport. Using an async trait (rather than `AsyncRead`/`Write`)
/// lets a transport `await` internally while serving a read, which the WebSocket
/// codec relies on to answer control frames mid-stream.
///
/// `Send` bounds are intentionally omitted: every stream lives on one pinned core
/// in the thread-per-core model and never crosses a thread.
#[allow(async_fn_in_trait)]
pub trait ByteStream {
	/// Reads some bytes into `buf`, returning the count (0 means end of stream).
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
	/// Writes the whole buffer, framing it as the transport requires.
	async fn write_all(&mut self, buf: &[u8]) -> Result<()>;
}

/// Plain TCP is a byte stream as-is.
impl ByteStream for TcpStream {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		AsyncReadExt::read(self, buf).await
	}

	async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
		AsyncWriteExt::write_all(self, buf).await
	}
}
