//! Durable-session snapshot.
//!
//! Sessions are *shard-local* — a client's session lives on exactly one shard —
//! so, unlike retained messages, they are not replicated. Each shard persists and
//! restores its own set (see `server::worker` for the per-shard files). Only
//! *suspended* (offline) sessions carry their durable state here; a connected
//! client's state lives in its connection and is captured when it disconnects
//! (or on graceful shutdown, after connections drain).
//!
//! The format nests the reusable [`codec`](super::codec) value encoding: per
//! session, the client id and remaining expiry, then its subscriptions, in-flight
//! QoS state, and offline queue. Each PUBLISH is stored as its MQTT wire bytes, so
//! all v5 properties round-trip.

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;

use super::codec::{
	Reader, put_opt_str, put_opt_u32, put_publish, put_str, put_u8, put_u16, put_u32, qos_from_u8, qos_to_u8,
	read_file, write_atomic,
};
use crate::broker::mesh::{MigratedSession, MigratedSub};
use crate::broker::session::{InflightMessage, InflightState, PersistedSession};

/// Magic header identifying a rusquitto session snapshot, version 1.
const MAGIC: &[u8; 4] = b"RQS1";

/// Serializes `sessions` and writes them atomically to `path`.
pub async fn save_sessions(path: &Path, sessions: &[PersistedSession]) -> Result<()> {
	write_atomic(path, serialize(sessions)?).await
}

/// Loads sessions from `path`. A missing file yields an empty vector.
pub async fn load_sessions(path: &Path) -> Result<Vec<PersistedSession>> {
	match read_file(path).await? {
		Some(data) => parse(&data),
		None => Ok(Vec::new()),
	}
}

fn serialize(sessions: &[PersistedSession]) -> Result<Vec<u8>> {
	let mut buf = Vec::new();
	buf.extend_from_slice(MAGIC);
	put_u32(&mut buf, sessions.len() as u32);
	for ps in sessions {
		encode_session(&mut buf, ps)?;
	}
	Ok(buf)
}

fn encode_session(buf: &mut Vec<u8>, ps: &PersistedSession) -> Result<()> {
	put_str(buf, &ps.client_id);
	put_u32(buf, ps.expiry_secs);

	let s = &ps.session;
	put_u32(buf, s.subscriptions.len() as u32);
	for sub in &s.subscriptions {
		put_str(buf, &sub.filter);
		put_u8(buf, qos_to_u8(sub.qos));
		let flags = u8::from(sub.nolocal) | (u8::from(sub.retain_as_published) << 1);
		put_u8(buf, flags);
		put_opt_str(buf, sub.share_group.as_deref());
		put_opt_u32(buf, sub.sub_id.map(|id| id as u32));
	}

	put_u16(buf, s.next_pkid);

	put_u32(buf, s.inflight.len() as u32);
	for (pkid, msg) in &s.inflight {
		put_u16(buf, *pkid);
		put_u8(buf, state_to_u8(msg.state));
		put_publish(buf, &msg.publish)?;
	}

	put_u32(buf, s.incoming_qos2.len() as u32);
	for (pkid, publish) in &s.incoming_qos2 {
		put_u16(buf, *pkid);
		put_publish(buf, publish)?;
	}

	put_u32(buf, s.offline.len() as u32);
	for (publish, qos, retain, sub_ids) in &s.offline {
		put_u8(buf, qos_to_u8(*qos));
		put_u8(buf, u8::from(*retain));
		put_u32(buf, sub_ids.len() as u32);
		for id in sub_ids {
			put_u32(buf, *id as u32);
		}
		put_publish(buf, publish)?;
	}
	Ok(())
}

fn parse(data: &[u8]) -> Result<Vec<PersistedSession>> {
	let mut r = Reader::new(data);
	r.expect_magic(MAGIC)?;
	let count = r.get_u32()? as usize;
	// Don't pre-allocate from the untrusted count; the bounds-checked reader stops
	// a corrupt/oversized count from reading past the buffer anyway.
	let mut out = Vec::new();
	for _ in 0..count {
		out.push(decode_session(&mut r)?);
	}
	Ok(out)
}

fn decode_session(r: &mut Reader) -> Result<PersistedSession> {
	let client_id = r.get_str()?;
	let expiry_secs = r.get_u32()?;

	let sub_count = r.get_u32()? as usize;
	let mut subscriptions = Vec::new();
	for _ in 0..sub_count {
		let filter = r.get_str()?;
		let qos = qos_from_u8(r.get_u8()?)?;
		let flags = r.get_u8()?;
		let share_group = r.get_opt_str()?;
		let sub_id = r.get_opt_u32()?.map(|v| v as usize);
		subscriptions.push(MigratedSub {
			filter,
			qos,
			nolocal: flags & 1 != 0,
			retain_as_published: flags & 2 != 0,
			share_group,
			sub_id,
		});
	}

	let next_pkid = r.get_u16()?;

	let inflight_count = r.get_u32()? as usize;
	let mut inflight = HashMap::new();
	for _ in 0..inflight_count {
		let pkid = r.get_u16()?;
		let state = state_from_u8(r.get_u8()?)?;
		let publish = r.get_publish()?;
		inflight.insert(pkid, InflightMessage { publish, state });
	}

	let incoming_count = r.get_u32()? as usize;
	let mut incoming_qos2 = HashMap::new();
	for _ in 0..incoming_count {
		let pkid = r.get_u16()?;
		incoming_qos2.insert(pkid, r.get_publish()?);
	}

	let offline_count = r.get_u32()? as usize;
	let mut offline = Vec::new();
	for _ in 0..offline_count {
		let qos = qos_from_u8(r.get_u8()?)?;
		let retain = r.get_u8()? != 0;
		let sid_count = r.get_u32()? as usize;
		let mut sub_ids = Vec::new();
		for _ in 0..sid_count {
			sub_ids.push(r.get_u32()? as usize);
		}
		let publish = r.get_publish()?;
		offline.push((publish, qos, retain, sub_ids));
	}

	Ok(PersistedSession {
		client_id,
		expiry_secs,
		session: MigratedSession { subscriptions, inflight, incoming_qos2, next_pkid, offline },
	})
}

fn state_to_u8(state: InflightState) -> u8 {
	match state {
		InflightState::Qos1 => 0,
		InflightState::Qos2Pending => 1,
		InflightState::Qos2Released => 2,
	}
}

fn state_from_u8(v: u8) -> Result<InflightState> {
	match v {
		0 => Ok(InflightState::Qos1),
		1 => Ok(InflightState::Qos2Pending),
		2 => Ok(InflightState::Qos2Released),
		_ => Err(Error::new(
			ErrorKind::InvalidData,
			"invalid in-flight state byte",
		)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use mqttbytes::{QoS, v5::Publish};

	fn sample() -> PersistedSession {
		let mut inflight = HashMap::new();
		inflight.insert(
			7,
			InflightMessage {
				publish: Publish::new("out/1", QoS::AtLeastOnce, b"hi".to_vec()),
				state: InflightState::Qos1,
			},
		);
		inflight.insert(
			8,
			InflightMessage {
				publish: Publish::new("out/2", QoS::ExactlyOnce, b"yo".to_vec()),
				state: InflightState::Qos2Released,
			},
		);
		let mut incoming = HashMap::new();
		incoming.insert(9, Publish::new("in/1", QoS::ExactlyOnce, b"z".to_vec()));

		PersistedSession {
			client_id: "client-A".to_string(),
			expiry_secs: 3600,
			session: MigratedSession {
				subscriptions: vec![
					MigratedSub {
						filter: "home/+/temp".to_string(),
						qos: QoS::AtLeastOnce,
						nolocal: true,
						retain_as_published: false,
						share_group: None,
						sub_id: Some(42),
					},
					MigratedSub {
						filter: "alerts/#".to_string(),
						qos: QoS::ExactlyOnce,
						nolocal: false,
						retain_as_published: true,
						share_group: Some("workers".to_string()),
						sub_id: None,
					},
				],
				inflight,
				incoming_qos2: incoming,
				next_pkid: 100,
				offline: vec![(
					Publish::new("q/1", QoS::AtLeastOnce, b"queued".to_vec()),
					QoS::AtLeastOnce,
					false,
					vec![1, 2, 3],
				)],
			},
		}
	}

	#[test]
	fn round_trips_a_full_session() {
		let sessions = vec![sample()];
		let loaded = parse(&serialize(&sessions).unwrap()).unwrap();
		assert_eq!(loaded.len(), 1);
		let s = &loaded[0];
		assert_eq!(s.client_id, "client-A");
		assert_eq!(s.expiry_secs, 3600);
		assert_eq!(s.session.next_pkid, 100);
		assert_eq!(s.session.subscriptions.len(), 2);

		let sub0 = &s.session.subscriptions[0];
		assert_eq!(sub0.filter, "home/+/temp");
		assert!(sub0.nolocal && !sub0.retain_as_published);
		assert_eq!(sub0.sub_id, Some(42));
		let sub1 = &s.session.subscriptions[1];
		assert_eq!(sub1.share_group.as_deref(), Some("workers"));
		assert!(sub1.retain_as_published);

		assert_eq!(s.session.inflight.len(), 2);
		assert!(matches!(
			s.session.inflight[&8].state,
			InflightState::Qos2Released
		));
		assert_eq!(s.session.inflight[&7].publish.topic, "out/1");
		assert_eq!(s.session.incoming_qos2[&9].topic, "in/1");
		assert_eq!(s.session.offline.len(), 1);
		assert_eq!(s.session.offline[0].3, vec![1, 2, 3]);
	}

	#[test]
	fn round_trips_empty_and_no_sessions() {
		assert!(parse(&serialize(&[]).unwrap()).unwrap().is_empty());
	}

	#[test]
	fn rejects_bad_magic_and_truncation() {
		assert!(parse(b"nope").is_err());
		let mut bytes = serialize(&[sample()]).unwrap();
		bytes.truncate(bytes.len() - 5); // chop the tail
		assert!(parse(&bytes).is_err());
	}
}
