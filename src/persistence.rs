//! Disk-backed persistence of retained messages.
//!
//! Retained messages are the broker's "last known value" per topic and the one
//! piece of state that most needs to survive a restart. They are also the easiest
//! to persist correctly: every shard holds an identical copy (each retained
//! publish is broadcast to all shards), so there is a single authoritative set.
//! One shard writes the snapshot; on startup every shard reloads it into its own
//! table, with no cross-shard coordination.
//!
//! The snapshot is just the concatenated MQTT wire bytes of each retained PUBLISH
//! (self-delimiting, so no framing is needed) behind a small magic header — the
//! same codec used on the network, so all v5 properties round-trip. Writes are
//! atomic: a temp file is written, `fdatasync`'d, then renamed over the target, so
//! a crash mid-write never corrupts the previous snapshot. File I/O uses glommio's
//! io_uring-backed [`BufferedFile`], so it never blocks the reactor.

use std::io::{Error, ErrorKind, Result};
use std::path::Path;

use bytes::BytesMut;
use glommio::io::BufferedFile;
use mqttbytes::{
	QoS,
	v5::{self as mqtt_v5, Packet, Publish},
};

/// Magic header identifying a rusquitto retained-message snapshot, version 1.
const MAGIC: &[u8; 4] = b"RQR1";

/// Ceiling on a single serialized retained PUBLISH when loading (generous; the
/// broker's own payload cap applied when the message was first accepted).
const MAX_PACKET: usize = 256 * 1024 * 1024;

/// Serializes `messages` and writes them atomically to `path` (temp file →
/// `fdatasync` → rename).
pub async fn save_retained(path: &Path, messages: &[Publish]) -> Result<()> {
	let mut buf = BytesMut::new();
	buf.extend_from_slice(MAGIC);
	for message in messages {
		let mut m = message.clone();
		m.retain = true;
		// A QoS > 0 PUBLISH needs a non-zero packet id on the wire; retained delivery
		// reassigns it, so a placeholder is fine and round-trips.
		if m.qos != QoS::AtMostOnce && m.pkid == 0 {
			m.pkid = 1;
		}
		m.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
	}

	let tmp = path.with_extension("tmp");
	let file = BufferedFile::create(&tmp).await?;
	file.write_at(buf.to_vec(), 0).await?;
	file.fdatasync().await?;
	file.close().await?;
	std::fs::rename(&tmp, path)?;
	Ok(())
}

/// Loads retained messages from `path`. A missing file yields an empty vector (a
/// fresh broker); a file whose header or body is corrupt is an error.
pub async fn load_retained(path: &Path) -> Result<Vec<Publish>> {
	let file = match BufferedFile::open(path).await {
		Ok(f) => f,
		Err(e) => {
			// A missing snapshot is normal (fresh broker); anything else is a real error.
			let io: Error = e.into();
			return if io.kind() == ErrorKind::NotFound {
				Ok(Vec::new())
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

	parse_retained(&data)
}

/// Parses the concatenated PUBLISH bytes of a snapshot (behind the magic header).
fn parse_retained(data: &[u8]) -> Result<Vec<Publish>> {
	if data.len() < MAGIC.len() || &data[..MAGIC.len()] != MAGIC {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"not a rusquitto retained snapshot",
		));
	}
	let mut buf = BytesMut::from(&data[MAGIC.len()..]);
	let mut out = Vec::new();
	loop {
		match mqtt_v5::read(&mut buf, MAX_PACKET) {
			Ok(Packet::Publish(p)) => out.push(p),
			Ok(_) => {} // Only PUBLISH is written; ignore anything else defensively.
			Err(mqttbytes::Error::InsufficientBytes(_)) => break,
			Err(e) => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("corrupt retained snapshot: {e:?}"),
				));
			}
		}
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn retained(topic: &str, qos: QoS, payload: &[u8]) -> Publish {
		let mut p = Publish::new(topic, qos, payload.to_vec());
		p.retain = true;
		p
	}

	/// Serializes to bytes the same way `save_retained` does, for a file-less test.
	fn serialize(messages: &[Publish]) -> Vec<u8> {
		let mut buf = BytesMut::new();
		buf.extend_from_slice(MAGIC);
		for message in messages {
			let mut m = message.clone();
			m.retain = true;
			if m.qos != QoS::AtMostOnce && m.pkid == 0 {
				m.pkid = 1;
			}
			m.write(&mut buf).unwrap();
		}
		buf.to_vec()
	}

	#[test]
	fn round_trips_messages_of_each_qos() {
		let messages = vec![
			retained("sensors/temp", QoS::AtMostOnce, b"21.5"),
			retained("state/door", QoS::AtLeastOnce, b"open"),
			retained("config/mode", QoS::ExactlyOnce, b"auto"),
		];
		let loaded = parse_retained(&serialize(&messages)).unwrap();
		assert_eq!(loaded.len(), 3);
		for (a, b) in messages.iter().zip(&loaded) {
			assert_eq!(a.topic, b.topic);
			assert_eq!(a.qos, b.qos);
			assert_eq!(a.payload, b.payload);
			assert!(b.retain);
		}
	}

	#[test]
	fn empty_snapshot_loads_as_no_messages() {
		assert!(parse_retained(&serialize(&[])).unwrap().is_empty());
	}

	#[test]
	fn rejects_data_without_the_magic_header() {
		assert!(parse_retained(b"garbage bytes here").is_err());
		assert!(parse_retained(b"").is_err());
	}
}
