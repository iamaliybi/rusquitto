//! Retained-message snapshot.
//!
//! Retained messages are the broker's "last known value" per topic and the state
//! that most needs to survive a restart. Every shard holds an identical copy (each
//! retained publish is broadcast to all shards), so there is a single authoritative
//! set: one shard writes the snapshot and every shard reloads it on startup, with
//! no cross-shard coordination.
//!
//! The snapshot is the concatenated MQTT wire bytes of each retained PUBLISH
//! (self-delimiting, so no framing is needed) behind a magic header — the same
//! codec used on the network, so all v5 properties round-trip.

use std::io::{Error, ErrorKind, Result};
use std::path::Path;

use bytes::BytesMut;
use mqttbytes::{
	QoS,
	v5::{self as mqtt_v5, Packet, Publish},
};

use super::codec::{read_file, write_atomic};

/// Magic header identifying a rusquitto retained-message snapshot, version 1.
const MAGIC: &[u8; 4] = b"RQR1";

/// Ceiling on a single serialized retained PUBLISH when loading.
const MAX_PACKET: usize = 256 * 1024 * 1024;

/// Serializes `messages` and writes them atomically to `path`.
pub async fn save_retained(path: &Path, messages: &[Publish]) -> Result<()> {
	let mut buf = Vec::new();
	buf.extend_from_slice(MAGIC);
	for message in messages {
		let mut m = message.clone();
		m.retain = true;
		// A QoS > 0 PUBLISH needs a non-zero packet id on the wire; retained delivery
		// reassigns it, so a placeholder round-trips fine.
		if m.qos != QoS::AtMostOnce && m.pkid == 0 {
			m.pkid = 1;
		}
		let mut wire = BytesMut::new();
		m.write(&mut wire)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		buf.extend_from_slice(&wire);
	}
	write_atomic(path, buf).await
}

/// Loads retained messages from `path`. A missing file yields an empty vector.
pub async fn load_retained(path: &Path) -> Result<Vec<Publish>> {
	match read_file(path).await? {
		Some(data) => parse_retained(&data),
		None => Ok(Vec::new()),
	}
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

	fn serialize(messages: &[Publish]) -> Vec<u8> {
		let mut buf = Vec::new();
		buf.extend_from_slice(MAGIC);
		for message in messages {
			let mut m = message.clone();
			m.retain = true;
			if m.qos != QoS::AtMostOnce && m.pkid == 0 {
				m.pkid = 1;
			}
			let mut wire = BytesMut::new();
			m.write(&mut wire).unwrap();
			buf.extend_from_slice(&wire);
		}
		buf
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
