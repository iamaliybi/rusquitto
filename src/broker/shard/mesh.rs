//! Cross-shard coordination: forwarding publishes over the channel mesh and
//! migrating a suspended session to whichever shard a client reconnects on.

use std::collections::VecDeque;
use std::rc::Rc;

use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalSender;
use mqttbytes::v5::Publish;

use super::ShardState;
use crate::broker::mesh::{MeshMsg, MigratedSession, MigratedSub, SessionControl};
use crate::broker::session::{Delivery, SessionSnapshot};
use crate::broker::topics::SubOptions;

impl ShardState {
	/// Stores this shard's mesh senders so publishes can be forwarded to peers.
	pub fn set_mesh(&mut self, senders: Senders<MeshMsg>) {
		self.mesh = Some(Rc::new(senders));
	}

	/// A cloneable handle to this shard's mesh senders. Lets the publish path
	/// `await` a cross-shard `send_to` (backpressure for QoS > 0) after dropping the
	/// `ShardState` borrow, rather than dropping the message with `try_send_to`.
	pub fn mesh_senders(&self) -> Option<Rc<Senders<MeshMsg>>> {
		self.mesh.clone()
	}

	/// Forwards a publish to every *other* shard in the mesh, best-effort. Each peer
	/// runs its own local `route`, so a remote subscriber receives it identically.
	///
	/// `try_send_to` is non-blocking (drop-on-full), so the caller never stalls on a
	/// slow peer — used for QoS 0 and broker-internal (`$SYS`) publishes where a drop
	/// is acceptable. The QoS > 0 publish path instead awaits [`mesh_senders`]'s
	/// `send_to` for backpressure. Self is skipped — local fan-out is done by `route`.
	///
	/// [`mesh_senders`]: Self::mesh_senders
	pub fn broadcast(&self, publish: &Publish) {
		let Some(senders) = &self.mesh else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			let _ = senders.try_send_to(idx, MeshMsg::Publish(publish.clone()));
		}
	}

	/// The number of *other* shards in the mesh (peers this shard can talk to).
	/// Zero for a single-shard broker, which short-circuits cross-shard migration.
	pub fn mesh_peers(&self) -> usize {
		self.mesh
			.as_ref()
			.map_or(0, |s| s.nr_consumers().saturating_sub(1))
	}

	/// Sends a single control message to one peer shard (best effort, drop-on-full).
	fn send_control_to(&self, peer: usize, control: SessionControl) {
		if let Some(senders) = &self.mesh {
			let _ = senders.try_send_to(peer, MeshMsg::Control(Box::new(control)));
		}
	}

	/// Broadcasts a session [`Claim`](SessionControl::Claim) to every peer shard.
	/// With `resume = true` peers holding a suspended session hand it back; with
	/// `resume = false` (Clean Start) they discard it instead. A no-op when there
	/// are no peers.
	pub fn broadcast_claim(&self, client_id: &str, resume: bool) {
		let Some(senders) = &self.mesh else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			let _ = senders.try_send_to(
				idx,
				MeshMsg::Control(Box::new(SessionControl::Claim {
					client_id: client_id.to_string(),
					requester: me,
					resume,
				})),
			);
		}
	}

	/// Registers a pending claim: the CONNECT handler awaits `tx`'s receiver while
	/// this sender is delivered any [`Handoff`](SessionControl::Handoff) replies.
	pub fn register_claim(&mut self, client_id: String, tx: LocalSender<Option<MigratedSession>>) {
		self.pending_claims.insert(client_id, tx);
	}

	/// Removes a pending claim once the CONNECT handler is done waiting.
	pub fn unregister_claim(&mut self, client_id: &str) {
		self.pending_claims.remove(client_id);
	}

	/// Dispatches a control message received from a peer over the mesh.
	pub fn on_control(&mut self, control: SessionControl) {
		match control {
			SessionControl::Claim { client_id, requester, resume } => self.handle_claim(client_id, requester, resume),
			SessionControl::Handoff { client_id, session } => {
				// Route the reply to whichever CONNECT handler is awaiting it. If none
				// is (timed out, or a stray/duplicate reply), it is simply dropped.
				if let Some(tx) = self.pending_claims.get(&client_id) {
					let _ = tx.try_send(session);
				}
			}
		}
	}

	/// Handles a peer's session [`Claim`](SessionControl::Claim): reply with the
	/// session if we own one and this is a resume, otherwise discard/none.
	fn handle_claim(&mut self, client_id: String, requester: usize, resume: bool) {
		// Decide with an immutable peek first so the borrow ends before we mutate.
		let session = match self.sessions.get(&client_id).map(|s| s.mailbox.is_none()) {
			// Suspended session and the client wants to resume: migrate it wholesale.
			Some(true) if resume => Some(self.extract_session(&client_id)),
			// A still-live session (cross-shard takeover) or a Clean Start discard:
			// drop it here — dropping the mailbox also disconnects the live client —
			// without migrating any durable state.
			Some(_) => {
				self.sessions.remove(&client_id);
				self.trie.remove_client(&client_id);
				None
			}
			// Nothing for this client id.
			None => None,
		};
		self.send_control_to(requester, SessionControl::Handoff { client_id, session });
	}

	/// Removes a suspended session from this shard and packages it for migration:
	/// its subscriptions (pulled from the trie), durable QoS state, and offline
	/// queue (unwrapped from `Rc` to owned publishes).
	fn extract_session(&mut self, client_id: &str) -> MigratedSession {
		let subscriptions = self
			.trie
			.take_client(client_id)
			.into_iter()
			.map(|f| MigratedSub {
				filter: f.filter,
				qos: f.qos,
				nolocal: f.nolocal,
				retain_as_published: f.retain_as_published,
				share_group: f.share_group,
				sub_id: f.sub_id,
			})
			.collect();

		let session = self
			.sessions
			.remove(client_id)
			.expect("extract_session called for a client without a session");
		let offline = session
			.offline_queue
			.into_iter()
			.map(|d| ((*d.publish).clone(), d.qos, d.retain, d.sub_ids))
			.collect();

		MigratedSession {
			subscriptions,
			inflight: session.snapshot.inflight,
			incoming_qos2: session.snapshot.incoming_qos2,
			next_pkid: session.snapshot.next_pkid,
			offline,
		}
	}

	/// Installs a session migrated from another shard onto the freshly-opened
	/// local session for `client_id`: re-arms its subscriptions in the trie and
	/// returns the durable QoS state and offline queue for the connection to load.
	///
	/// The local session must already exist (just created by `open_session`); its
	/// expiry is governed by the current CONNECT, so the migrated deadline is not
	/// carried over.
	pub fn install_migrated(
		&mut self,
		client_id: &str,
		migrated: MigratedSession,
	) -> (SessionSnapshot, VecDeque<Delivery>) {
		for sub in migrated.subscriptions {
			self.trie.insert(
				&sub.filter,
				client_id,
				SubOptions {
					qos: sub.qos,
					nolocal: sub.nolocal,
					retain_as_published: sub.retain_as_published,
					share_group: sub.share_group.as_deref(),
					sub_id: sub.sub_id,
				},
			);
		}

		let offline = migrated
			.offline
			.into_iter()
			.map(|(publish, qos, retain, sub_ids)| Delivery { publish: Rc::new(publish), qos, retain, sub_ids })
			.collect();

		let snapshot = SessionSnapshot {
			inflight: migrated.inflight,
			incoming_qos2: migrated.incoming_qos2,
			next_pkid: migrated.next_pkid,
		};
		(snapshot, offline)
	}
}
