//! Cross-shard coordination: forwarding publishes over the channel mesh and
//! migrating a suspended session to whichever shard a client reconnects on.

use std::collections::VecDeque;
use std::rc::Rc;

use glommio::channels::channel_mesh::Senders;
use glommio::channels::local_channel::LocalSender;
use mqttbytes::v5::Publish;

use super::ShardState;
use crate::broker::delivery::Delivery;
use crate::broker::messages::{MeshMsg, MigratedSession, MigratedSub, SessionControl, SharedEvent};
use crate::broker::session::SessionSnapshot;
use crate::broker::topics::SubOptions;

impl ShardState {
	/// Stores this shard's mesh senders so publishes can be forwarded to peers.
	pub fn set_mesh(&mut self, senders: Senders<MeshMsg>) {
		self.mesh_tx = Some(Rc::new(senders));
	}

	/// A cloneable handle to this shard's mesh senders. Lets the publish path
	/// `await` a cross-shard `send_to` (backpressure for QoS > 0) after dropping the
	/// `ShardState` borrow, rather than dropping the message with `try_send_to`.
	pub fn mesh_senders(&self) -> Option<Rc<Senders<MeshMsg>>> {
		self.mesh_tx.clone()
	}

	/// Installs the sender half of the reliable control-plane outbox (its receiver
	/// is drained by a per-shard task that awaits `send_to`). Called once the mesh
	/// is joined, only when there are peers.
	pub fn set_control_tx(&mut self, tx: LocalSender<(usize, MeshMsg)>) {
		self.control_tx = Some(tx);
	}

	/// Reliably enqueues one control-plane message for `peer`. Synchronous and
	/// non-dropping (the local outbox is unbounded); the drain task applies mesh
	/// backpressure. A no-op with no peers (single-shard broker), where the message
	/// would have gone nowhere anyway.
	fn enqueue_control(&self, peer: usize, msg: MeshMsg) {
		if let Some(tx) = &self.control_tx {
			// The unbounded local channel only errors if the receiver (the drain
			// task) is gone, which happens only at shard teardown.
			let _ = tx.try_send((peer, msg));
		}
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
		let Some(senders) = &self.mesh_tx else {
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
		self.mesh_tx
			.as_ref()
			.map_or(0, |s| s.nr_consumers().saturating_sub(1))
	}

	/// Reliably sends a single control message to one peer shard (queued to the
	/// control outbox, never dropped).
	fn send_control_to(&self, peer: usize, control: SessionControl) {
		self.enqueue_control(peer, MeshMsg::Control(Box::new(control)));
	}

	/// Announces a shared-group membership change to every peer shard, **reliably**
	/// (via the control outbox — never dropped, and delivered in order so a
	/// `Join` can't be reordered past a later `Leave`). Peers fold it into their
	/// replicated membership view so all shards agree on the group's connected
	/// members; a dropped announcement would desync the deterministic global
	/// shared-subscription pick (double- or zero-delivery), so this must not drop.
	pub(super) fn broadcast_shared(&self, group: &str, client_id: &str, join: bool) {
		let Some(senders) = &self.mesh_tx else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			let event = if join {
				SharedEvent::Join { group: group.to_string(), client_id: client_id.to_string() }
			} else {
				SharedEvent::Leave { group: group.to_string(), client_id: client_id.to_string() }
			};
			self.enqueue_control(idx, MeshMsg::Shared(event));
		}
	}

	/// Folds a peer's membership announcement into the replicated view used for
	/// the global shared-subscription delivery pick.
	pub fn apply_shared_event(&mut self, event: SharedEvent) {
		match event {
			SharedEvent::Join { group, client_id } => {
				self.shared_remote
					.entry(group)
					.or_default()
					.insert(client_id);
			}
			SharedEvent::Leave { group, client_id } => {
				if let Some(members) = self.shared_remote.get_mut(&group) {
					members.remove(&client_id);
					if members.is_empty() {
						self.shared_remote.remove(&group);
					}
				}
			}
		}
	}

	/// Broadcasts a session [`Claim`](SessionControl::Claim) to every peer shard,
	/// **reliably** (via the control outbox). With `resume = true` peers holding a
	/// suspended session hand it back; with `resume = false` (Clean Start) they
	/// discard it instead. A no-op when there are no peers. Dropping a claim under
	/// overload would silently lose a migrating client's session, so it must not
	/// drop.
	pub fn broadcast_claim(&self, client_id: &str, resume: bool) {
		let Some(senders) = &self.mesh_tx else {
			return;
		};
		let me = senders.peer_id();
		for idx in 0..senders.nr_consumers() {
			if idx == me {
				continue;
			}
			self.enqueue_control(
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
		// A parked session is about to be migrated or discarded either way; its
		// dormant fd on this shard must close (takeover semantics — no Will).
		// Signalled first, while the session record still exists.
		self.signal_close_parked(&client_id);
		// Decide with an immutable peek first so the borrow ends before we mutate.
		let session = match self.sessions.get(&client_id).map(|s| s.mailbox.is_none()) {
			// Suspended session and the client wants to resume: migrate it wholesale.
			// It now lives on the requesting shard, so tombstone it here.
			Some(true) if resume => {
				let migrated = self.extract_session(&client_id);
				self.wal_removed(&client_id);
				Some(migrated)
			}
			// A still-live session (cross-shard takeover) or a Clean Start discard:
			// drop it here — dropping the mailbox also disconnects the live client —
			// without migrating any durable state.
			Some(_) => {
				self.sessions.remove(&client_id);
				self.trie.remove_client(&client_id);
				self.wal_removed(&client_id);
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
		// Suspended sessions carry their durable state boxed; absent means empty.
		let snapshot = session.snapshot.map(|b| *b).unwrap_or_default();
		let offline = session
			.offline_queue
			.into_iter()
			.map(|d| ((*d.publish).clone(), d.qos, d.retain, d.sub_ids))
			.collect();

		MigratedSession {
			subscriptions,
			inflight: snapshot.inflight,
			incoming_qos2: snapshot.incoming_qos2,
			next_pkid: snapshot.next_pkid,
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
					// No Local on a shared subscription is a protocol error (MQTT 5
					// §3.8.3.1) and is rejected at SUBSCRIBE; strip it defensively
					// from state that predates that rule, since it would desync the
					// cluster-wide membership pick.
					nolocal: sub.nolocal && sub.share_group.is_none(),
					retain_as_published: sub.retain_as_published,
					share_group: sub.share_group.as_deref(),
					sub_id: sub.sub_id,
				},
			);
		}
		// The arriving client is connected here: its shared subscriptions are
		// live group memberships again, announced from their new home shard.
		for group in self.shared_groups_of(client_id) {
			self.broadcast_shared(&group, client_id, true);
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
