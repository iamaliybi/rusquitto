//! Per-shard session write-ahead log.
//!
//! The session [`snapshot`](super::session) captures the full suspended-session
//! set periodically; between snapshots a crash would lose every session that
//! suspended — and every QoS > 0 message queued to a suspended session — since
//! the last one. This append-only log records those durable mutations as they
//! happen, group-committed (`fdatasync`'d) by the shard's persistence task, so
//! restore replays them over the snapshot and the crash window shrinks from
//! `snapshot_interval` to the WAL flush interval.
//!
//! Records are last-writer-wins per client id:
//! - **Upsert** — a suspended session's full durable state (offline queue
//!   included), re-logged whenever it changes.
//! - **Remove** — a tombstone: the session was resumed, destroyed, expired, or
//!   migrated to another shard.
//!
//! Framing: `[u32 len][u8 kind][payload]`, where `len` covers `kind + payload`.
//! On replay a torn trailing record (a crash mid-append) is detected via the
//! reader's remaining-bytes count and the log stops there — only the last,
//! un-`fdatasync`'d batch is lost. Because both record kinds are idempotent, a
//! WAL left un-truncated after a snapshot replays harmlessly on the next start.

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};

use glommio::io::{BufferedFile, OpenOptions};

use super::codec::{Reader, put_str, put_u8, put_u32, read_file};
use super::session::{decode_session, encode_session};
use crate::broker::session::PersistedSession;

const KIND_UPSERT: u8 = 1;
const KIND_REMOVE: u8 = 2;

/// Appends a framed Upsert record (kind + full session encoding) to `out`.
pub fn encode_upsert(out: &mut Vec<u8>, ps: &PersistedSession) -> Result<()> {
	let mut rec = Vec::new();
	put_u8(&mut rec, KIND_UPSERT);
	encode_session(&mut rec, ps)?;
	frame(out, &rec);
	Ok(())
}

/// Appends a framed Remove record (kind + client id) to `out`.
pub fn encode_remove(out: &mut Vec<u8>, client_id: &str) {
	let mut rec = Vec::new();
	put_u8(&mut rec, KIND_REMOVE);
	put_str(&mut rec, client_id);
	frame(out, &rec);
}

fn frame(out: &mut Vec<u8>, rec: &[u8]) {
	put_u32(out, rec.len() as u32);
	out.extend_from_slice(rec);
}

/// Replays the WAL at `path` over `sessions` (keyed by client id, seeded from the
/// snapshot). A missing file is a no-op. Returns the number of records applied. A
/// torn or corrupt trailing record stops the replay there.
pub async fn replay(path: &Path, sessions: &mut HashMap<String, PersistedSession>) -> Result<usize> {
	match read_file(path).await? {
		Some(data) => Ok(apply(&data, sessions)),
		None => Ok(0),
	}
}

/// Applies an in-memory WAL byte buffer over `sessions` (used by [`replay`] and
/// by tests that exercise the record path without a file).
pub(crate) fn apply(data: &[u8], sessions: &mut HashMap<String, PersistedSession>) -> usize {
	let mut r = Reader::new(data);
	let mut applied = 0;
	loop {
		if r.remaining() < 4 {
			break; // no room for a length prefix — clean end (or a stray tail byte)
		}
		let len = match r.get_u32() {
			Ok(l) => l as usize,
			Err(_) => break,
		};
		if len == 0 || r.remaining() < len {
			break; // torn trailing record: a crash mid-append
		}
		match read_record(&mut r) {
			Ok(Record::Upsert(ps)) => {
				sessions.insert(ps.client_id.clone(), ps);
				applied += 1;
			}
			Ok(Record::Remove(id)) => {
				sessions.remove(&id);
				applied += 1;
			}
			Err(_) => break, // corrupt record: stop, keep what we already applied
		}
	}
	applied
}

enum Record {
	Upsert(PersistedSession),
	Remove(String),
}

fn read_record(r: &mut Reader) -> Result<Record> {
	match r.get_u8()? {
		KIND_UPSERT => Ok(Record::Upsert(decode_session(r)?)),
		KIND_REMOVE => Ok(Record::Remove(r.get_str()?)),
		other => Err(Error::new(
			ErrorKind::InvalidData,
			format!("unknown WAL record kind {other}"),
		)),
	}
}

/// An open, appendable write-ahead log for one shard. Owns the file handle and
/// tracks the append offset so each flush is a single `write_at` + `fdatasync`.
pub struct Wal {
	path: PathBuf,
	file: BufferedFile,
	offset: u64,
}

impl Wal {
	/// Opens (creating if absent) the WAL at `path` for appending, positioned at
	/// the end so records already on disk from a prior run are preserved until the
	/// next snapshot truncates them.
	pub async fn open(path: &Path) -> Result<Self> {
		let file = OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.buffered_open(path)
			.await?;
		let offset = file.file_size().await?;
		Ok(Self { path: path.to_path_buf(), file, offset })
	}

	/// Appends one group-committed batch and `fdatasync`s it. A no-op for an empty
	/// batch.
	pub async fn append(&mut self, bytes: Vec<u8>) -> Result<()> {
		if bytes.is_empty() {
			return Ok(());
		}
		let n = bytes.len() as u64;
		self.file.write_at(bytes, self.offset).await?;
		self.offset += n;
		self.file.fdatasync().await?;
		Ok(())
	}

	/// Truncates the log to empty. Called right after a full snapshot is written,
	/// which subsumes every record; the durable snapshot makes this safe.
	pub async fn truncate(&mut self) -> Result<()> {
		let fresh = BufferedFile::create(&self.path).await?;
		fresh.fdatasync().await?;
		let old = std::mem::replace(&mut self.file, fresh);
		let _ = old.close().await;
		self.offset = 0;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::broker::messages::MigratedSession;
	use mqttbytes::{QoS, v5::Publish};

	fn session(id: &str, queued: &str) -> PersistedSession {
		PersistedSession {
			client_id: id.to_string(),
			expiry_secs: 3600,
			session: MigratedSession {
				subscriptions: Vec::new(),
				inflight: HashMap::new(),
				incoming_qos2: HashMap::new(),
				next_pkid: 1,
				offline: vec![(
					Publish::new("q/1", QoS::AtLeastOnce, queued.as_bytes().to_vec()),
					QoS::AtLeastOnce,
					false,
					vec![],
				)],
			},
		}
	}

	#[test]
	fn replays_upserts_and_removes_last_writer_wins() {
		let mut log = Vec::new();
		encode_upsert(&mut log, &session("a", "one")).unwrap();
		encode_upsert(&mut log, &session("b", "keep")).unwrap();
		encode_upsert(&mut log, &session("a", "two")).unwrap(); // supersedes a=one
		encode_remove(&mut log, "b"); // tombstones b

		let mut sessions = HashMap::new();
		assert_eq!(apply(&log, &mut sessions), 4);
		assert_eq!(sessions.len(), 1);
		let a = &sessions["a"];
		assert_eq!(a.session.offline[0].0.payload.as_ref(), b"two");
		assert!(!sessions.contains_key("b"));
	}

	#[test]
	fn replay_applies_over_a_snapshot_seed() {
		// Snapshot had a=old and c; the WAL updates a and removes c.
		let mut sessions = HashMap::new();
		sessions.insert("a".to_string(), session("a", "old"));
		sessions.insert("c".to_string(), session("c", "gone"));

		let mut log = Vec::new();
		encode_upsert(&mut log, &session("a", "new")).unwrap();
		encode_remove(&mut log, "c");

		apply(&log, &mut sessions);
		assert_eq!(sessions["a"].session.offline[0].0.payload.as_ref(), b"new");
		assert!(!sessions.contains_key("c"));
	}

	#[test]
	fn torn_trailing_record_is_ignored() {
		let mut log = Vec::new();
		encode_upsert(&mut log, &session("a", "one")).unwrap();
		let good = log.len();
		encode_upsert(&mut log, &session("b", "two")).unwrap();
		log.truncate(good + 6); // chop the second record mid-body (keeps its length prefix)

		let mut sessions = HashMap::new();
		assert_eq!(apply(&log, &mut sessions), 1); // only the intact first record
		assert!(sessions.contains_key("a"));
		assert!(!sessions.contains_key("b"));
	}
}
