//! PING, DISCONNECT, and the sender side of the QoS 1/2 acknowledgement flows
//! (PUBACK / PUBREC → PUBREL / PUBCOMP).

use mqttbytes::v5::{self as mqtt_v5};
use std::io::{Error, ErrorKind, Result};
use tracing::info;

use super::Connection;
use crate::broker::session::InflightState;
use crate::transport::ByteStream;

impl<S: ByteStream> Connection<S> {
	pub(super) async fn handle_ping(&mut self) -> Result<()> {
		self.send(|buf| mqtt_v5::PingResp.write(buf)).await
	}

	pub(super) async fn handle_disconnect(&mut self, disconnect: mqtt_v5::Disconnect) -> Result<()> {
		// A normal DISCONNECT (0x00) suppresses the will; reason 0x04 explicitly
		// asks for it, and any other reason code leaves it to fire.
		let reason = disconnect.reason_code;
		if reason == mqtt_v5::DisconnectReasonCode::NormalDisconnection {
			self.will = None;
		}
		info!(reason = ?reason, "client sent disconnect");
		// Returning an error unwinds the event loop and closes the connection.
		Err(Error::new(
			ErrorKind::ConnectionAborted,
			"Client Disconnected",
		))
	}

	pub(super) async fn handle_puback(&mut self, puback: mqtt_v5::PubAck) -> Result<()> {
		// QoS 1, sender side: the client acknowledged a message we delivered. The
		// transaction is complete; release the packet id and let a held message
		// through the freed window slot.
		if self.inflight.remove(&puback.pkid).is_some() {
			self.drain_pending().await?;
		}
		Ok(())
	}

	pub(super) async fn handle_pubrec(&mut self, pubrec: mqtt_v5::PubRec) -> Result<()> {
		// QoS 2, sender side (step 2): the client received our PUBLISH. Advance the
		// transaction to "released" and send PUBREL.
		if let Some(entry) = self.inflight.get_mut(&pubrec.pkid)
			&& matches!(entry.state, InflightState::Qos2Pending)
		{
			entry.state = InflightState::Qos2Released;
		}

		self.send(|buf| mqtt_v5::PubRel::new(pubrec.pkid).write(buf))
			.await
	}

	pub(super) async fn handle_pubrel(&mut self, pubrel: mqtt_v5::PubRel) -> Result<()> {
		// QoS 2, receiver side: the publisher has released the message. Commit it
		// (deliver exactly once) if we still hold it, then finalize with PubComp.
		if let Some(message) = self.incoming_qos2.remove(&pubrel.pkid) {
			self.fan_out(message, Some(&self.client_id)).await;
		}

		self.send(|buf| mqtt_v5::PubComp::new(pubrel.pkid).write(buf))
			.await
	}

	pub(super) async fn handle_pubcomp(&mut self, pubcomp: mqtt_v5::PubComp) -> Result<()> {
		// QoS 2, sender side (step 4): the client finalized the transaction.
		// Release the packet id and admit a held message into the freed slot.
		if self.inflight.remove(&pubcomp.pkid).is_some() {
			self.drain_pending().await?;
		}
		Ok(())
	}
}
