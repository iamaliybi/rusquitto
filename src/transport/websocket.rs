//! Minimal RFC 6455 WebSocket server transport for MQTT-over-WebSockets.
//!
//! Web browsers can't open raw TCP, so `mqtt.js` and friends tunnel MQTT inside
//! WebSocket **binary** frames. This module performs the server handshake and then
//! presents the framed connection as a plain [`ByteStream`]: reads yield the
//! reassembled application bytes (feeding the same MQTT parser as TCP), writes are
//! wrapped in binary frames. Control frames (ping/pong/close) are handled
//! transparently inside `read`.
//!
//! Hardening: the handshake request is size-capped, every client data/control
//! frame must be masked (per spec), and frame payloads are bounded by
//! `max_frame`, so a malicious client can't exhaust memory before MQTT-level
//! limits apply.

use std::io::{Error, ErrorKind, Result};

use std::time::Duration;

use base64::Engine;
use bytes::BytesMut;
use futures_lite::io::{AsyncRead, AsyncWrite};
use futures_lite::{AsyncReadExt, AsyncWriteExt, FutureExt};
use sha1::{Digest, Sha1};

use crate::transport::ByteStream;

/// Magic GUID appended to `Sec-WebSocket-Key` before hashing (RFC 6455 §4.2.2).
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Largest handshake request we will buffer, to bound a slow-loris / oversized
/// header attack before the connection is even established.
const MAX_HANDSHAKE_BYTES: usize = 8 * 1024;

/// Scratch read size for pulling bytes off the socket.
const READ_CHUNK: usize = 4096;

/// A WebSocket-framed byte stream over an inner transport `S`.
///
/// `S` is any async byte stream — a plain TCP stream for `ws://`, or a
/// [`TlsStream`](crate::transport::tls::TlsStream) for `wss://` — so this codec is
/// written once and layers over either.
pub struct WsStream<S> {
	inner: S,
	/// Raw bytes read from the socket, not yet decoded into frames.
	raw: BytesMut,
	/// Decoded application bytes ready to hand to `read`.
	app: BytesMut,
	/// Largest frame payload accepted, bounding per-frame memory.
	max_frame: usize,
	/// A close frame was seen or sent; further reads report end of stream.
	closed: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsStream<S> {
	/// Performs the server handshake on an accepted connection and returns the
	/// framed stream, bounding the whole handshake by `timeout` so a client that
	/// opens the socket but stalls (slow-loris) can't hold the connection — the WS
	/// handshake runs before the MQTT event loop, so it needs its own deadline.
	/// Fails (closing the connection) on any malformed or non-MQTT upgrade request.
	pub async fn accept(inner: S, max_frame: usize, timeout: Duration) -> Result<Self> {
		let handshake = Self::handshake(inner, max_frame);
		if timeout.is_zero() {
			return handshake.await;
		}
		let deadline = async {
			glommio::timer::sleep(timeout).await;
			Err(protocol("websocket handshake timed out"))
		};
		handshake.or(deadline).await
	}

	/// The handshake proper: read the HTTP upgrade request, validate it, and reply
	/// `101 Switching Protocols`. Bounded by [`accept`](Self::accept)'s timeout.
	async fn handshake(mut inner: S, max_frame: usize) -> Result<Self> {
		let mut raw = BytesMut::new();
		let mut chunk = [0u8; READ_CHUNK];

		// Read until the end of the HTTP header block, bounded in size.
		let header_end = loop {
			if let Some(pos) = find_header_end(&raw) {
				break pos;
			}
			if raw.len() > MAX_HANDSHAKE_BYTES {
				return Err(protocol("websocket handshake headers too large"));
			}
			let n = AsyncReadExt::read(&mut inner, &mut chunk).await?;
			if n == 0 {
				return Err(protocol("connection closed during websocket handshake"));
			}
			raw.extend_from_slice(&chunk[..n]);
		};

		let request = std::str::from_utf8(&raw[..header_end])
			.map_err(|_| protocol("non-UTF-8 websocket handshake"))?
			.to_owned();
		let accept_key = handshake_accept(&request)?;
		let offers_mqtt = header_line(&request, "sec-websocket-protocol")
			.is_some_and(|v| v.split(',').any(|p| p.trim().eq_ignore_ascii_case("mqtt")));

		let mut response = format!(
			"HTTP/1.1 101 Switching Protocols\r\n\
			 Upgrade: websocket\r\n\
			 Connection: Upgrade\r\n\
			 Sec-WebSocket-Accept: {accept_key}\r\n"
		);
		if offers_mqtt {
			response.push_str("Sec-WebSocket-Protocol: mqtt\r\n");
		}
		response.push_str("\r\n");
		AsyncWriteExt::write_all(&mut inner, response.as_bytes()).await?;

		// Anything past the header block is the first WebSocket frame bytes.
		let leftover = raw.split_off(header_end + 4);
		Ok(Self {
			inner,
			raw: leftover,
			app: BytesMut::new(),
			max_frame,
			closed: false,
		})
	}

	/// Sends a control frame (ping/pong/close) with a short payload (server→client
	/// frames are never masked).
	async fn send_control(&mut self, opcode: u8, payload: &[u8]) -> Result<()> {
		let mut frame = Vec::with_capacity(2 + payload.len());
		frame.push(0x80 | opcode); // FIN + opcode
		frame.push(payload.len() as u8); // control payloads are <= 125 bytes
		frame.extend_from_slice(payload);
		AsyncWriteExt::write_all(&mut self.inner, &frame).await?;
		AsyncWriteExt::flush(&mut self.inner).await
	}

	/// Tries to decode one frame from `raw`, servicing control frames and appending
	/// data payloads to `app`. Returns `Ok(true)` if a frame was consumed,
	/// `Ok(false)` if more bytes are needed.
	async fn pump_frame(&mut self) -> Result<bool> {
		let Some(frame) = Frame::parse(&self.raw, self.max_frame)? else {
			return Ok(false);
		};
		self.raw.advance_to(frame.total_len);

		match frame.opcode {
			// Continuation / text / binary all carry application data for our
			// length-framed MQTT byte stream; we don't distinguish them.
			0x0..=0x2 => self.app.extend_from_slice(&frame.payload),
			0x8 => {
				// Close: echo and mark the stream ended.
				let _ = self.send_control(0x8, &[]).await;
				self.closed = true;
			}
			0x9 => {
				// Ping: reply with a matching pong.
				self.send_control(0xA, &frame.payload).await?;
			}
			0xA => {} // Pong: ignore.
			other => {
				return Err(protocol(&format!(
					"unsupported websocket opcode {other:#x}"
				)));
			}
		}
		Ok(true)
	}
}

impl<S: AsyncRead + AsyncWrite + Unpin> ByteStream for WsStream<S> {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		let mut chunk = [0u8; READ_CHUNK];
		loop {
			if !self.app.is_empty() {
				let n = self.app.len().min(buf.len());
				buf[..n].copy_from_slice(&self.app[..n]);
				self.app.advance_to(n);
				return Ok(n);
			}
			if self.closed {
				return Ok(0);
			}
			// Decode as many buffered frames as are complete before reading more.
			if self.pump_frame().await? {
				continue;
			}
			let n = AsyncReadExt::read(&mut self.inner, &mut chunk).await?;
			if n == 0 {
				return Ok(0);
			}
			self.raw.extend_from_slice(&chunk[..n]);
		}
	}

	async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
		// One unmasked binary frame per write (server→client frames are unmasked).
		let mut frame = Vec::with_capacity(buf.len() + 10);
		frame.push(0x82); // FIN + binary opcode
		let len = buf.len();
		if len < 126 {
			frame.push(len as u8);
		} else if len <= u16::MAX as usize {
			frame.push(126);
			frame.extend_from_slice(&(len as u16).to_be_bytes());
		} else {
			frame.push(127);
			frame.extend_from_slice(&(len as u64).to_be_bytes());
		}
		frame.extend_from_slice(buf);
		AsyncWriteExt::write_all(&mut self.inner, &frame).await?;
		// Flush so a buffering inner transport (TLS, for `wss`) actually emits the
		// frame; a no-op for plain TCP.
		AsyncWriteExt::flush(&mut self.inner).await
	}
}

/// A decoded WebSocket frame plus how many raw bytes it occupied.
struct Frame {
	opcode: u8,
	payload: Vec<u8>,
	total_len: usize,
}

impl Frame {
	/// Parses one frame from the front of `buf`. `Ok(None)` means the buffer holds
	/// only a partial frame. Enforces the RFC's client-masking requirement and the
	/// `max_frame` payload cap.
	fn parse(buf: &[u8], max_frame: usize) -> Result<Option<Frame>> {
		if buf.len() < 2 {
			return Ok(None);
		}
		let opcode = buf[0] & 0x0F;
		let masked = buf[1] & 0x80 != 0;
		let mut len = (buf[1] & 0x7F) as usize;
		let mut offset = 2;

		match len {
			126 => {
				if buf.len() < offset + 2 {
					return Ok(None);
				}
				len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
				offset += 2;
			}
			127 => {
				if buf.len() < offset + 8 {
					return Ok(None);
				}
				let mut bytes = [0u8; 8];
				bytes.copy_from_slice(&buf[offset..offset + 8]);
				len = u64::from_be_bytes(bytes) as usize;
				offset += 8;
			}
			_ => {}
		}

		if len > max_frame {
			return Err(protocol("websocket frame exceeds size limit"));
		}
		// Control frames (opcode >= 0x8) must be unfragmented and carry <= 125 bytes
		// (RFC 6455 §5.5); reject oversized/fragmented ones rather than desyncing.
		if opcode >= 0x8 && (buf[0] & 0x80 == 0 || len > 125) {
			return Err(protocol("invalid websocket control frame"));
		}
		// Every client-to-server frame must be masked (RFC 6455 §5.1).
		if !masked {
			return Err(protocol("unmasked client websocket frame"));
		}

		let key_end = offset + 4;
		if buf.len() < key_end + len {
			return Ok(None);
		}
		let mask = [buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]];
		let data = &buf[key_end..key_end + len];
		let payload: Vec<u8> = data
			.iter()
			.enumerate()
			.map(|(i, b)| b ^ mask[i % 4])
			.collect();

		Ok(Some(Frame { opcode, payload, total_len: key_end + len }))
	}
}

/// Validates the HTTP upgrade request and returns the `Sec-WebSocket-Accept` value.
fn handshake_accept(request: &str) -> Result<String> {
	let request_line = request.lines().next().unwrap_or_default();
	let mut parts = request_line.split_whitespace();
	if parts.next() != Some("GET") {
		return Err(protocol("websocket upgrade must be a GET"));
	}

	let upgrade_ok = header_line(request, "upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
	let connection_ok = header_line(request, "connection").is_some_and(|v| v.to_ascii_lowercase().contains("upgrade"));
	let version_ok = header_line(request, "sec-websocket-version").is_some_and(|v| v.trim() == "13");
	if !(upgrade_ok && connection_ok && version_ok) {
		return Err(protocol("malformed websocket upgrade request"));
	}

	let key = header_line(request, "sec-websocket-key").ok_or_else(|| protocol("missing Sec-WebSocket-Key"))?;

	let mut hasher = Sha1::new();
	hasher.update(key.trim().as_bytes());
	hasher.update(WS_GUID.as_bytes());
	Ok(base64::engine::general_purpose::STANDARD.encode(hasher.finalize()))
}

/// Returns the trimmed value of the first header named `name` (case-insensitive).
fn header_line<'a>(request: &'a str, name: &str) -> Option<&'a str> {
	request.lines().skip(1).find_map(|line| {
		let (k, v) = line.split_once(':')?;
		k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
	})
}

/// Finds the index just before the `\r\n\r\n` header terminator.
fn find_header_end(buf: &[u8]) -> Option<usize> {
	buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn protocol(msg: &str) -> Error {
	Error::new(ErrorKind::InvalidData, msg.to_string())
}

/// Small `BytesMut`-style front-advance without importing extra traits everywhere.
trait AdvanceTo {
	fn advance_to(&mut self, n: usize);
}

impl AdvanceTo for BytesMut {
	fn advance_to(&mut self, n: usize) {
		let _ = self.split_to(n);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn accept_key_matches_rfc_example() {
		// RFC 6455 §1.3 worked example.
		let req = "GET /chat HTTP/1.1\r\n\
			Host: server.example.com\r\n\
			Upgrade: websocket\r\n\
			Connection: Upgrade\r\n\
			Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
			Sec-WebSocket-Version: 13\r\n\r\n";
		assert_eq!(
			handshake_accept(req).unwrap(),
			"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
		);
	}

	#[test]
	fn rejects_non_upgrade() {
		let req = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";
		assert!(handshake_accept(req).is_err());
	}

	#[test]
	fn parse_requires_mask_and_decodes_payload() {
		// A masked 1-byte binary frame carrying 0xAB.
		let mask = [0x01, 0x02, 0x03, 0x04];
		let mut frame = vec![0x82, 0x81, mask[0], mask[1], mask[2], mask[3], 0xAB ^ mask[0]];
		let parsed = Frame::parse(&frame, 1024).unwrap().unwrap();
		assert_eq!(parsed.opcode, 0x2);
		assert_eq!(parsed.payload, vec![0xAB]);

		// Same frame unmasked is rejected.
		frame[1] = 0x01;
		assert!(Frame::parse(&frame, 1024).is_err());
	}

	#[test]
	fn rejects_oversized_and_fragmented_control_frames() {
		// A masked PING (0x89) claiming a 126-byte payload violates the control-frame
		// 125-byte cap and must be rejected before we try to buffer/echo it.
		let big_ping = vec![0x89, 0x80 | 126, 0x00, 0x7E, 0x01, 0x02, 0x03, 0x04];
		assert!(Frame::parse(&big_ping, 65536).is_err());

		// A fragmented control frame (FIN clear on a PING) is also invalid.
		let frag_ping = vec![0x09, 0x80, 0x01, 0x02, 0x03, 0x04];
		assert!(Frame::parse(&frag_ping, 65536).is_err());
	}
}
