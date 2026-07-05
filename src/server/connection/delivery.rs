//! The outbound delivery path: in-flight window control, cross-shard fan-out,
//! packet-id allocation, and resend-on-resume.

use mqttbytes::{
	QoS,
	v5::{self as mqtt_v5},
};
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use tracing::{debug, warn};

use super::{Connection, PENDING_OUTBOUND_LIMIT};
use crate::broker::mesh::MeshMsg;
use crate::broker::session::{Delivery, InflightMessage, InflightState};
use crate::transport::ByteStream;

/// An all-`None` v5 property set (mqttbytes has no constructor for it).
fn empty_publish_properties() -> mqtt_v5::PublishProperties {
	mqtt_v5::PublishProperties {
		payload_format_indicator: None,
		message_expiry_interval: None,
		topic_alias: None,
		response_topic: None,
		correlation_data: None,
		user_properties: Vec::new(),
		subscription_identifiers: Vec::new(),
		content_type: None,
	}
}

impl<S: ByteStream> Connection<S> {
	/// The outbound in-flight ceiling: the smaller of the client's Receive Maximum
	/// and our own configured `max_inflight`, and always at least 1.
	fn outbound_window(&self) -> usize {
		usize::from(self.peer_receive_max.min(self.limits.max_inflight)).max(1)
	}

	/// Queues a delivery for the wire now if the in-flight window has room (QoS 0
	/// always sends), otherwise holds it in the pending queue for later draining.
	/// The pending queue is bounded: a client that stalls its acks drops its
	/// oldest held messages rather than growing broker memory without limit.
	pub(super) fn deliver(&mut self, delivery: Delivery) -> Result<()> {
		if delivery.qos == QoS::AtMostOnce || self.inflight.len() < self.outbound_window() {
			self.send_publish(
				&delivery.publish,
				delivery.qos,
				delivery.retain,
				&delivery.sub_ids,
			)
		} else {
			if self.pending_outbound.len() >= PENDING_OUTBOUND_LIMIT {
				self.pending_outbound.pop_front();
			}
			self.pending_outbound.push_back(delivery);
			Ok(())
		}
	}

	/// Releases held-back messages up to the in-flight window; called after an
	/// acknowledgement frees a slot.
	pub(super) fn drain_pending(&mut self) -> Result<()> {
		while self.inflight.len() < self.outbound_window() {
			let Some(delivery) = self.pending_outbound.pop_front() else {
				break;
			};
			self.send_publish(
				&delivery.publish,
				delivery.qos,
				delivery.retain,
				&delivery.sub_ids,
			)?;
		}
		// A fully drained hold queue releases its burst-sized ring, so a client
		// that once stalled behind a deep backlog doesn't pin that memory forever.
		if self.pending_outbound.is_empty() && self.pending_outbound.capacity() > 64 {
			self.pending_outbound = VecDeque::new();
		}
		Ok(())
	}

	/// Forwards a publish to peer shards, then fans it out to local subscribers.
	///
	/// The cross-shard forward is where at-least/exactly-once could previously be
	/// lost: a full mesh link dropped the message. Now a **QoS > 0** publish is sent
	/// with the awaiting `send_to`, so the publisher applies backpressure (its own
	/// PUBACK/PUBREC is only written after this returns) rather than dropping —
	/// making the guarantee hold across shards, not just within one. A **QoS 0**
	/// publish keeps the non-blocking `try_send_to` (fire-and-forget). The mesh
	/// senders are cloned out of `ShardState` so its borrow isn't held across the
	/// await. `publisher` is this connection's client id for a client publish (No
	/// Local), or `None` for a broker-originated one such as a Will Message.
	pub(super) async fn fan_out(&self, message: mqtt_v5::Publish, publisher: Option<&str>) {
		let senders = self.state.borrow().mesh_senders();
		if let Some(senders) = senders {
			let me = senders.peer_id();
			for idx in 0..senders.nr_consumers() {
				if idx == me {
					continue;
				}
				if message.qos == QoS::AtMostOnce {
					let _ = senders.try_send_to(idx, MeshMsg::Publish(message.clone()));
				} else {
					// Backpressure: wait for room so a QoS > 0 message is never dropped
					// on a full mesh link. Err means the peer is gone — nothing to do.
					let _ = senders
						.send_to(idx, MeshMsg::Publish(message.clone()))
						.await;
				}
			}
		}
		self.state.borrow_mut().deliver_local(message, publisher);
	}

	/// Queues a routed message for this client at the given effective QoS and
	/// retain flag (the coalesced output buffer reaches the wire at the next
	/// event-loop flush).
	///
	/// QoS 0 is fire-and-forget. QoS 1/2 are assigned a fresh packet id, recorded
	/// in the in-flight window, and delivered with their QoS set; the rest of the
	/// handshake (PUBACK / PUBREC+PUBREL+PUBCOMP) is driven by the ack handlers.
	/// `retain` is decided by the caller (set for a retained replay or a
	/// Retain-As-Published subscriber, cleared for ordinary live fan-out). `sub_ids`
	/// are the Subscription Identifiers to echo to the client.
	pub(super) fn send_publish(
		&mut self,
		publish: &mqtt_v5::Publish,
		qos: QoS,
		retain: bool,
		sub_ids: &[usize],
	) -> Result<()> {
		let mut message = publish.clone();
		message.qos = qos;
		message.dup = false;
		message.retain = retain;

		// Property hygiene for delivery: attach this subscriber's Subscription
		// Identifiers, and never forward the publisher's Topic Alias (it is scoped
		// to the publisher's connection; our own outbound alias is applied below).
		// Other v5 properties (message expiry, content type, user properties, …)
		// pass through.
		if !sub_ids.is_empty() || message.properties.is_some() {
			let props = message
				.properties
				.get_or_insert_with(empty_publish_properties);
			props.topic_alias = None;
			props.subscription_identifiers = sub_ids.to_vec();
		}

		let pkid = match qos {
			QoS::AtMostOnce => {
				message.pkid = 0;
				None
			}
			QoS::AtLeastOnce => {
				let pkid = self.alloc_pkid();
				message.pkid = pkid;
				self.track_inflight(pkid, &message, InflightState::Qos1);
				Some(pkid)
			}
			QoS::ExactlyOnce => {
				let pkid = self.alloc_pkid();
				message.pkid = pkid;
				self.track_inflight(pkid, &message, InflightState::Qos2Pending);
				Some(pkid)
			}
		};

		// Outbound topic alias (MQTT 5): when the client accepts aliases, a repeat
		// of a registered topic goes out as just the alias (empty topic name); the
		// first use of a topic registers an alias alongside the full name. Applied
		// *after* the in-flight copy was stored above, so a retransmit on a later
		// connection (whose alias table starts empty) still carries the full topic.
		let mut newly_aliased: Option<String> = None;
		if self.peer_topic_alias_max > 0 {
			if let Some(&alias) = self.outbound_aliases.get(&message.topic) {
				message
					.properties
					.get_or_insert_with(empty_publish_properties)
					.topic_alias = Some(alias);
				message.topic.clear();
			} else if (self.outbound_aliases.len() as u16) < self.peer_topic_alias_max {
				let alias = self.outbound_aliases.len() as u16 + 1;
				self.outbound_aliases.insert(message.topic.clone(), alias);
				message
					.properties
					.get_or_insert_with(empty_publish_properties)
					.topic_alias = Some(alias);
				newly_aliased = Some(message.topic.clone());
			}
			// Alias table full: send the full topic, unaliased.
		}

		// Encode straight into the coalesced output buffer; on failure, roll the
		// partial bytes back so the batch stays well-formed.
		let start = self.out.len();
		if let Err(e) = message.write(&mut self.out) {
			self.out.truncate(start);
			if let Some(pkid) = pkid {
				self.inflight.remove(&pkid);
			}
			if let Some(topic) = &newly_aliased {
				self.outbound_aliases.remove(topic);
			}
			return Err(Error::new(ErrorKind::InvalidData, e.to_string()));
		}

		// The client's Maximum Packet Size is a hard ceiling: we must not send a
		// larger packet. Drop it (rolling back the encoded bytes, the in-flight
		// slot, and a just-registered alias the client will now never see) — it
		// can never be delivered.
		let written = self.out.len() - start;
		if let Some(max) = self.peer_max_packet_size
			&& written as u64 > u64::from(max)
		{
			warn!(
				size = written,
				max, "outbound publish exceeds client max packet size, dropping"
			);
			self.out.truncate(start);
			if let Some(pkid) = pkid {
				self.inflight.remove(&pkid);
			}
			if let Some(topic) = &newly_aliased {
				self.outbound_aliases.remove(topic);
			}
			return Ok(());
		}

		self.metrics.message_sent(message.payload.len());
		Ok(())
	}

	/// Records an outbound QoS 1/2 message in the in-flight window, keeping a copy
	/// of the PUBLISH so it can be retransmitted with the DUP flag on resume.
	fn track_inflight(&mut self, pkid: u16, message: &mqtt_v5::Publish, state: InflightState) {
		self.inflight
			.insert(pkid, InflightMessage { publish: message.clone(), state });
	}

	/// Allocates the next unused packet id (1..=65535) for an outbound message.
	fn alloc_pkid(&mut self) -> u16 {
		loop {
			self.next_pkid = self.next_pkid.wrapping_add(1);
			if self.next_pkid == 0 {
				self.next_pkid = 1;
			}
			// In practice the in-flight window is tiny, so this resolves at once.
			if !self.inflight.contains_key(&self.next_pkid) {
				return self.next_pkid;
			}
		}
	}

	/// Restores message flow on a resumed session: first retransmit the unacked
	/// in-flight QoS 1/2 messages (with the DUP flag, reusing their packet ids),
	/// then deliver everything buffered while the client was offline. Everything
	/// is encoded into the coalesced output buffer, flushing periodically so a
	/// deep backlog can't balloon it.
	pub(super) async fn resume_delivery(&mut self, offline_queue: VecDeque<Delivery>) -> Result<()> {
		if !self.inflight.is_empty() {
			debug!(
				count = self.inflight.len(),
				"retransmitting in-flight messages"
			);
		}
		// Direct field borrows keep `self.inflight` (shared) and `self.out`
		// (mutable) disjoint, so no intermediate per-packet buffers are needed.
		let out = &mut self.out;
		for (pkid, entry) in &self.inflight {
			match entry.state {
				// Message not yet acknowledged: resend the PUBLISH marked DUP.
				InflightState::Qos1 | InflightState::Qos2Pending => {
					let mut publish = entry.publish.clone();
					publish.pkid = *pkid;
					publish.dup = true;
					publish
						.write(out)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				}
				// PUBLISH already acknowledged via PUBREC: resume at PUBREL.
				InflightState::Qos2Released => {
					mqtt_v5::PubRel::new(*pkid)
						.write(out)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				}
			}
		}
		if self.out.len() >= super::FLUSH_THRESHOLD {
			self.flush().await?;
		}

		// Deliver messages that arrived while the session was suspended; each gets
		// a fresh packet id via the normal outbound path, respecting the window.
		if !offline_queue.is_empty() {
			debug!(count = offline_queue.len(), "flushing offline queue");
			for delivery in offline_queue {
				self.deliver(delivery)?;
				if self.out.len() >= super::FLUSH_THRESHOLD {
					self.flush().await?;
				}
			}
		}

		Ok(())
	}
}
