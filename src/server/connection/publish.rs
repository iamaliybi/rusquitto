//! Inbound PUBLISH: topic-alias resolution, topic validation, authorization,
//! and the receiver side of the QoS 0/1/2 handshakes.

use mqttbytes::{
	QoS,
	v5::{self as mqtt_v5},
};
use std::io::{Error, ErrorKind, Result};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use super::Connection;
use crate::protocol::valid_publish_topic;
use crate::telemetry::logging::redact;
use crate::transport::ByteStream;

impl<S: ByteStream> Connection<S> {
	pub(super) async fn handle_publish(&mut self, mut publish: mqtt_v5::Publish) -> Result<()> {
		// Per-connection PUBLISH throttle: reserve a token before doing any routing
		// work. When the client is over its configured rate the connection sleeps for
		// the returned delay — pacing this publisher to its budget and yielding the
		// (pinned) core to other connections — instead of dropping the message. PUBLISH
		// is the amplifier (one message fans out to every subscriber), so throttling it
		// bounds the CPU a single noisy client can draw on its core.
		let wait = match self.rate_limiter.as_mut() {
			Some(bucket) => bucket.acquire(Instant::now()),
			None => Duration::ZERO,
		};
		if !wait.is_zero() {
			glommio::timer::sleep(wait).await;
		}

		// Resolve an inbound topic alias (MQTT 5) before anything else reads the
		// topic. A PUBLISH may register an alias (topic + alias) or use one (empty
		// topic + alias); an out-of-range or unknown alias is a protocol error.
		if let Some(alias) = publish.properties.as_ref().and_then(|p| p.topic_alias) {
			if alias == 0 || alias > Self::INBOUND_TOPIC_ALIAS_MAX {
				warn!(alias, "topic alias out of range, disconnecting");
				self.send_disconnect(mqtt_v5::DisconnectReasonCode::TopicAliasInvalid)?;
				return Err(Error::new(ErrorKind::InvalidData, "topic alias invalid"));
			}
			if publish.topic.is_empty() {
				match self.inbound_aliases.get(&alias) {
					Some(topic) => publish.topic = topic.clone(),
					None => {
						warn!(alias, "unknown topic alias, disconnecting");
						self.send_disconnect(mqtt_v5::DisconnectReasonCode::TopicAliasInvalid)?;
						return Err(Error::new(ErrorKind::InvalidData, "unknown topic alias"));
					}
				}
			} else {
				self.inbound_aliases.insert(alias, publish.topic.clone());
			}
		}

		// A PUBLISH topic must be a concrete name: non-empty, no wildcards, no NUL,
		// and never `$`-prefixed (those are broker-reserved, so a client can't spoof
		// `$SYS`). Anything else is a protocol violation — disconnect.
		if !valid_publish_topic(&publish.topic) {
			warn!(topic = %publish.topic, "invalid publish topic, disconnecting");
			self.send_disconnect(mqtt_v5::DisconnectReasonCode::TopicNameInvalid)?;
			return Err(Error::new(ErrorKind::InvalidData, "invalid publish topic"));
		}

		// Payload contents are never logged — only topic, QoS, and byte length.
		debug!(
			topic = %publish.topic,
			qos = ?publish.qos,
			retain = publish.retain,
			payload = %redact::payload(&publish.payload),
			"publish received"
		);

		self.metrics.message_received(publish.payload.len());

		// Enforce publish authorization before routing. On denial the message is
		// not fanned out: QoS > 0 gets a negative acknowledgement (Not Authorized),
		// QoS 0 is dropped silently as there is no way to signal the sender.
		if !self
			.auth
			.authorize_publish(self.username.as_deref(), &publish.topic)
		{
			warn!(topic = %publish.topic, "publish not authorized, rejecting");
			return match publish.qos {
				QoS::AtMostOnce => Ok(()),
				QoS::AtLeastOnce => {
					let mut ack = mqtt_v5::PubAck::new(publish.pkid);
					ack.reason = mqtt_v5::PubAckReason::NotAuthorized;
					self.send(|buf| ack.write(buf))
				}
				QoS::ExactlyOnce => {
					let mut rec = mqtt_v5::PubRec::new(publish.pkid);
					rec.reason = mqtt_v5::PubRecReason::NotAuthorized;
					self.send(|buf| rec.write(buf))
				}
			};
		}

		// Normalize for fan-out: clear the publisher's packet id and dup flag, but
		// keep the original QoS so each subscriber can be downgraded individually
		// to `min(publish QoS, granted QoS)` at delivery time.
		let mut msg = publish.clone();
		msg.pkid = 0;
		msg.dup = false;

		// Inbound QoS handshake (receiver side).
		match publish.qos {
			// Fire and forget.
			QoS::AtMostOnce => {
				self.fan_out(msg, Some(&self.client_id)).await;
				Ok(())
			}
			// At least once: forward (awaiting mesh backpressure), then acknowledge —
			// the PUBACK is only sent once the message has been accepted for delivery
			// on every shard, so the guarantee holds cross-shard.
			QoS::AtLeastOnce => {
				self.fan_out(msg, Some(&self.client_id)).await;
				let pkid = publish.pkid;
				self.send(|buf| mqtt_v5::PubAck::new(pkid).write(buf))
			}
			// Exactly once: store the message and acknowledge receipt with PubRec.
			// Actual delivery is deferred to PUBREL so it happens exactly once even
			// if the publisher re-sends the PUBLISH.
			QoS::ExactlyOnce => {
				// Enforce the inbound Receive Maximum we advertised: bound the number
				// of concurrent unacknowledged QoS 2 PUBLISHes. A fresh pkid past the
				// quota is a protocol violation → DISCONNECT (0x93). A re-send of a
				// pkid we already hold doesn't count against the quota.
				if !self.incoming_qos2.contains_key(&publish.pkid)
					&& self.incoming_qos2.len() >= usize::from(self.limits.max_inflight)
				{
					warn!(
						quota = self.limits.max_inflight,
						"inbound receive maximum exceeded, disconnecting"
					);
					self.send_disconnect(mqtt_v5::DisconnectReasonCode::ReceiveMaximumExceeded)?;
					return Err(Error::new(
						ErrorKind::InvalidData,
						"receive maximum exceeded",
					));
				}
				let pkid = publish.pkid;
				self.incoming_qos2.insert(pkid, msg);
				self.send(|buf| mqtt_v5::PubRec::new(pkid).write(buf))
			}
		}
	}
}
