//! Shared on-disk codec: atomic file I/O over glommio's io_uring `BufferedFile`,
//! plus small little-endian, length-prefixed value writers and a bounds-checked
//! reader used by both the retained and session snapshots.

use std::io::{Error, ErrorKind, Result};
use std::path::Path;

use bytes::BytesMut;
use glommio::io::BufferedFile;
use mqttbytes::{
	QoS,
	v5::{self as mqtt_v5, Packet, Publish},
};

/// Ceiling on a single serialized PUBLISH when loading (generous; the broker's own
/// payload cap applied when the message was first accepted).
const MAX_PACKET: usize = 256 * 1024 * 1024;

/// Writes `bytes` to `path` atomically: a temp file is written, `fdatasync`'d, then
/// renamed over `path`, so a crash mid-write can't corrupt the previous file.
pub async fn write_atomic(path: &Path, bytes: Vec<u8>) -> Result<()> {
	let tmp = path.with_extension("tmp");
	let file = BufferedFile::create(&tmp).await?;
	file.write_at(bytes, 0).await?;
	file.fdatasync().await?;
	file.close().await?;
	std::fs::rename(&tmp, path)?;
	Ok(())
}

/// Reads the whole file at `path`. A missing file yields `None` (a fresh broker).
pub async fn read_file(path: &Path) -> Result<Option<Vec<u8>>> {
	let file = match BufferedFile::open(path).await {
		Ok(f) => f,
		Err(e) => {
			// `BufferedFile` returns glommio's error type; convert to check the kind.
			let io: Error = e.into();
			return if io.kind() == ErrorKind::NotFound {
				Ok(None)
			} else {
				Err(io)
			};
		}
	};
	let size = file.file_size().await? as usize;
	let data = if size == 0 {
		Vec::new()
	} else {
		file.read_at(0, size).await?.to_vec()
	};
	file.close().await?;
	Ok(Some(data))
}

// --- value writers (append to a `Vec<u8>`) ---

pub fn put_u8(buf: &mut Vec<u8>, v: u8) {
	buf.push(v);
}

pub fn put_u16(buf: &mut Vec<u8>, v: u16) {
	buf.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u32(buf: &mut Vec<u8>, v: u32) {
	buf.extend_from_slice(&v.to_le_bytes());
}

/// A UTF-8 string, `u16` length-prefixed.
pub fn put_str(buf: &mut Vec<u8>, s: &str) {
	put_u16(buf, s.len() as u16);
	buf.extend_from_slice(s.as_bytes());
}

pub fn put_opt_str(buf: &mut Vec<u8>, s: Option<&str>) {
	match s {
		Some(x) => {
			put_u8(buf, 1);
			put_str(buf, x);
		}
		None => put_u8(buf, 0),
	}
}

pub fn put_opt_u32(buf: &mut Vec<u8>, v: Option<u32>) {
	match v {
		Some(x) => {
			put_u8(buf, 1);
			put_u32(buf, x);
		}
		None => put_u8(buf, 0),
	}
}

/// A PUBLISH as its MQTT wire bytes, `u32` length-prefixed. A QoS > 0 publish is
/// given a placeholder packet id (retained/queued delivery reassigns it).
pub fn put_publish(buf: &mut Vec<u8>, publish: &Publish) -> Result<()> {
	let mut tmp = BytesMut::new();
	let mut m = publish.clone();
	if m.qos != QoS::AtMostOnce && m.pkid == 0 {
		m.pkid = 1;
	}
	m.write(&mut tmp)
		.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
	put_u32(buf, tmp.len() as u32);
	buf.extend_from_slice(&tmp);
	Ok(())
}

pub fn qos_to_u8(qos: QoS) -> u8 {
	match qos {
		QoS::AtMostOnce => 0,
		QoS::AtLeastOnce => 1,
		QoS::ExactlyOnce => 2,
	}
}

pub fn qos_from_u8(v: u8) -> Result<QoS> {
	match v {
		0 => Ok(QoS::AtMostOnce),
		1 => Ok(QoS::AtLeastOnce),
		2 => Ok(QoS::ExactlyOnce),
		_ => Err(Error::new(ErrorKind::InvalidData, "invalid QoS byte")),
	}
}

// --- reader ---

/// A cursor over a snapshot's bytes, with bounds checks so a truncated or corrupt
/// file fails cleanly rather than panicking.
pub struct Reader<'a> {
	data: &'a [u8],
	pos: usize,
}

impl<'a> Reader<'a> {
	pub fn new(data: &'a [u8]) -> Self {
		Self { data, pos: 0 }
	}

	fn take(&mut self, n: usize) -> Result<&'a [u8]> {
		let end = self
			.pos
			.checked_add(n)
			.filter(|e| *e <= self.data.len())
			.ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "snapshot truncated"))?;
		let slice = &self.data[self.pos..end];
		self.pos = end;
		Ok(slice)
	}

	/// Bytes not yet consumed. Used by the WAL replay to detect a torn trailing
	/// record (a crash mid-append) before trying to parse it.
	pub fn remaining(&self) -> usize {
		self.data.len() - self.pos
	}

	pub fn expect_magic(&mut self, magic: &[u8]) -> Result<()> {
		if self.take(magic.len())? == magic {
			Ok(())
		} else {
			Err(Error::new(ErrorKind::InvalidData, "bad snapshot magic"))
		}
	}

	pub fn get_u8(&mut self) -> Result<u8> {
		Ok(self.take(1)?[0])
	}

	pub fn get_u16(&mut self) -> Result<u16> {
		let b = self.take(2)?;
		Ok(u16::from_le_bytes([b[0], b[1]]))
	}

	pub fn get_u32(&mut self) -> Result<u32> {
		let b = self.take(4)?;
		Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
	}

	pub fn get_str(&mut self) -> Result<String> {
		let n = self.get_u16()? as usize;
		let b = self.take(n)?;
		String::from_utf8(b.to_vec()).map_err(|_| Error::new(ErrorKind::InvalidData, "non-UTF-8 string"))
	}

	pub fn get_opt_str(&mut self) -> Result<Option<String>> {
		if self.get_u8()? == 1 {
			Ok(Some(self.get_str()?))
		} else {
			Ok(None)
		}
	}

	pub fn get_opt_u32(&mut self) -> Result<Option<u32>> {
		if self.get_u8()? == 1 {
			Ok(Some(self.get_u32()?))
		} else {
			Ok(None)
		}
	}

	pub fn get_publish(&mut self) -> Result<Publish> {
		let n = self.get_u32()? as usize;
		let bytes = self.take(n)?;
		let mut buf = BytesMut::from(bytes);
		match mqtt_v5::read(&mut buf, MAX_PACKET) {
			Ok(Packet::Publish(p)) => Ok(p),
			Ok(_) => Err(Error::new(
				ErrorKind::InvalidData,
				"expected PUBLISH in snapshot",
			)),
			Err(e) => Err(Error::new(
				ErrorKind::InvalidData,
				format!("corrupt publish in snapshot: {e:?}"),
			)),
		}
	}
}
