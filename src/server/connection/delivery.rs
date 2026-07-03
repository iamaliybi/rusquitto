//! The outbound delivery path: in-flight window control, cross-shard fan-out,
//! packet-id allocation, and resend-on-resume.

use bytes::BytesMut;
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

impl<S: ByteStream> Connection<S> {
	/// The outbound in-flight ceiling: the smaller of the client's Receive Maximum
	/// and our own configured `max_inflight`, and always at least 1.
	fn outbound_window(&self) -> usize {
		usize::from(self.peer_receive_max.min(self.limits.max_inflight)).max(1)
	}

	/// Sends a delivery now if the in-flight window has room (QoS 0 always sends),
	/// otherwise holds it in the pending queue for later draining. The pending queue
	/// is bounded: a client that stalls its acks drops its oldest held messages
	/// rather than growing broker memory without limit.
	pub(super) async fn deliver(&mut self, delivery: Delivery) -> Result<()> {
		if delivery.qos == QoS::AtMostOnce || self.inflight.len() < self.outbound_window() {
			self.send_publish(
				&delivery.publish,
				delivery.qos,
				delivery.retain,
				&delivery.sub_ids,
			)
			.await
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
	pub(super) async fn drain_pending(&mut self) -> Result<()> {
		while self.inflight.len() < self.outbound_window() {
			let Some(delivery) = self.pending_outbound.pop_front() else {
				break;
			};
			self.send_publish(
				&delivery.publish,
				delivery.qos,
				delivery.retain,
				&delivery.sub_ids,
			)
			.await?;
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

	/// Delivers a routed message to this client at the given effective QoS and
	/// retain flag.
	///
	/// QoS 0 is fire-and-forget. QoS 1/2 are assigned a fresh packet id, recorded
	/// in the in-flight window, and delivered with their QoS set; the rest of the
	/// handshake (PUBACK / PUBREC+PUBREL+PUBCOMP) is driven by the ack handlers.
	/// `retain` is decided by the caller (set for a retained replay or a
	/// Retain-As-Published subscriber, cleared for ordinary live fan-out). `sub_ids`
	/// are the Subscription Identifiers to echo to the client.
	pub(super) async fn send_publish(
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
		// Identifiers, and never forward the publisher's Topic Alias (it is scoped to
		// the publisher's connection; we don't assign outbound aliases). Other v5
		// properties (message expiry, content type, user properties, …) pass through.
		if !sub_ids.is_empty() || message.properties.is_some() {
			let props = message
				.properties
				.get_or_insert_with(|| mqtt_v5::PublishProperties {
					payload_format_indicator: None,
					message_expiry_interval: None,
					topic_alias: None,
					response_topic: None,
					correlation_data: None,
					user_properties: Vec::new(),
					subscription_identifiers: Vec::new(),
					content_type: None,
				});
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

		let mut buf = BytesMut::new();
		message
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

		// The client's Maximum Packet Size is a hard ceiling: we must not send a
		// larger packet. Drop it (rolling back the in-flight slot so it doesn't
		// wedge the window) — it can never be delivered to this client.
		if let Some(max) = self.peer_max_packet_size
			&& buf.len() as u64 > u64::from(max)
		{
			warn!(
				size = buf.len(),
				max, "outbound publish exceeds client max packet size, dropping"
			);
			if let Some(pkid) = pkid {
				self.inflight.remove(&pkid);
			}
			return Ok(());
		}

		self.metrics.message_sent(message.payload.len());
		self.stream.write_all(&buf).await
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
	/// then deliver everything buffered while the client was offline.
	pub(super) async fn resume_delivery(&mut self, offline_queue: VecDeque<Delivery>) -> Result<()> {
		// Encode the retransmissions before writing, so we don't hold a borrow of
		// `self.inflight` across the await points.
		let mut packets: Vec<BytesMut> = Vec::with_capacity(self.inflight.len());
		for (pkid, entry) in &self.inflight {
			let mut buf = BytesMut::new();
			match entry.state {
				// Message not yet acknowledged: resend the PUBLISH marked DUP.
				InflightState::Qos1 | InflightState::Qos2Pending => {
					let mut publish = entry.publish.clone();
					publish.pkid = *pkid;
					publish.dup = true;
					publish
						.write(&mut buf)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				}
				// PUBLISH already acknowledged via PUBREC: resume at PUBREL.
				InflightState::Qos2Released => {
					mqtt_v5::PubRel::new(*pkid)
						.write(&mut buf)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
				}
			}
			packets.push(buf);
		}

		if !packets.is_empty() {
			debug!(count = packets.len(), "retransmitting in-flight messages");
			for buf in packets {
				self.stream.write_all(&buf).await?;
			}
		}

		// Deliver messages that arrived while the session was suspended; each gets
		// a fresh packet id via the normal outbound path, respecting the window.
		if !offline_queue.is_empty() {
			debug!(count = offline_queue.len(), "flushing offline queue");
			for delivery in offline_queue {
				self.deliver(delivery).await?;
			}
		}

		Ok(())
	}
}
