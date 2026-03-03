use bytes::BytesMut;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use mqttbytes::{
	v5::{self as mqtt_v5, Packet},
	Error as MqttError,
};
use std::io::{Error, ErrorKind, Result};

pub struct Connection {
	stream: TcpStream,
	buffer: BytesMut,
	shard_id: usize,
	client_id: String,
}

impl Connection {
	const MAX_PACKET_SIZE: usize = 64 * 1024;
	
	const INITIAL_BUFFER_SIZE: usize = 4 * 1024;
	
	const READ_BUFFER_SIZE: usize = 2048;
	
	pub fn new(stream: TcpStream, shard_id: usize) -> Self {
		Self {
			stream,
			buffer: BytesMut::with_capacity(Self::INITIAL_BUFFER_SIZE),
			shard_id,
			client_id: String::new(),
		}
	}

	pub async fn run(&mut self) -> Result<()> {
		println!("[Shard {}] Connection Started", self.shard_id);

		// Continuously reads raw bytes from the socket until the client disconnects (EOF)
		loop {
			match self.read_packet().await {
				Ok(Some(packet)) => {
					if let Err(e) = self.process_packet(packet).await {
						eprintln!("[Shard {}] Protocol/IO Error: {}", self.shard_id, e);
						return Ok(()); // Close connection on error
					}
				}
				Ok(None) => break,
				Err(e) => {
					eprintln!("[Shard {}] Network Error: {}", self.shard_id, e);
					return Err(e);
				}
			}
		}

		println!("[Shard {}] Connection Closed", self.shard_id);
		Ok(())
	}

	async fn read_packet(&mut self) -> Result<Option<Packet>> {
		let mut temp_buf = [0u8; Self::READ_BUFFER_SIZE];

		// A single TCP read might contain multiple MQTT packets (Batch Processing)
		loop {
			match mqtt_v5::read(&mut self.buffer, Self::MAX_PACKET_SIZE) {
				Ok(packet) => return Ok(Some(packet)),
				Err(MqttError::InsufficientBytes(_)) => {
					// Not enough data, continue reading from network
				}
				Err(e) => {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!("MQTT Parse Error: {:?}", e),
					));
				}
			}

			let n = self.stream.read(&mut temp_buf).await?;
			if n == 0 {
				return Ok(None);
			}

			self.buffer.extend_from_slice(&temp_buf[..n]);
		}
	}

	async fn process_packet(&mut self, packet: Packet) -> Result<()> {
		match packet {
			// Client -> Server Requests
			Packet::Connect(connect) => self.handle_connect(connect).await,
			Packet::Publish(publish) => self.handle_publish(publish).await,
			Packet::Subscribe(subscribe) => self.handle_subscribe(subscribe).await,
			Packet::Unsubscribe(unsubscribe) => self.handle_unsubscribe(unsubscribe).await,
			Packet::PingReq => self.handle_ping().await,
			Packet::Disconnect(_) => self.handle_disconnect().await,

			// QoS 1 & 2 Flows (Client Responses)
			Packet::PubAck(puback) => self.handle_puback(puback).await,
			Packet::PubRec(pubrec) => self.handle_pubrec(pubrec).await,
			Packet::PubRel(pubrel) => self.handle_pubrel(pubrel).await,
			Packet::PubComp(pubcomp) => self.handle_pubcomp(pubcomp).await,

			// Server -> Client Packets (Should NOT receive these from client)
			Packet::ConnAck(_) | Packet::SubAck(_) | Packet::UnsubAck(_) | Packet::PingResp => {
				eprintln!(
					"[Shard {}] Protocol Violation: Received Server-Only Packet",
					self.shard_id
				);
				Ok(())
			}
		}
	}
}

impl Connection {
	async fn handle_connect(&mut self, connect: mqtt_v5::Connect) -> Result<()> {
		self.client_id = connect.client_id;
		println!(
			"[Shard {}] Client Connected: {}",
			self.shard_id, self.client_id
		);

		let conn_ack = mqtt_v5::ConnAck::new(mqtt_v5::ConnectReturnCode::Success, false);
		let mut buf = BytesMut::new();
		conn_ack
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

		self.stream.write_all(&buf).await
	}

	async fn handle_disconnect(&mut self) -> Result<()> {
		println!("[Shard {}] Client sent Disconnect", self.shard_id);
		// Clean exit
		Err(Error::new(
			ErrorKind::ConnectionAborted,
			"Client Disconnected",
		))
	}

	async fn handle_ping(&mut self) -> Result<()> {
		let mut buf = BytesMut::new();
		mqtt_v5::PingResp
			.write(&mut buf)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
		self.stream.write_all(&buf).await
	}

	async fn handle_publish(&mut self, publish: mqtt_v5::Publish) -> Result<()> {
		// [ACTION PLAN]
		// 1. Authorization: Check if the client is allowed to publish to 'publish.topic'.
		// 2. Retain Handling: If 'publish.retain' is true, update the global Retain Table.
		// 3. Routing:
		//    a. Local: Lookup topic in the 'Topic Trie' to find matching local subscriptions.
		//    b. Cluster: Broadcast this message to other Shards (CPUs) via Inter-Shard Channel.
		// 4. QoS Handling:
		//    - QoS 0: Do nothing (Fire and Forget).
		//    - QoS 1: Send 'PubAck' back to client.
		//    - QoS 2: Store in Session State and send 'PubRec'.
		
		unimplemented!("Pending: Topic Trie, ACL Check, Retain Storage, QoS Handshake")
	}

	async fn handle_subscribe(&mut self, subscribe: mqtt_v5::Subscribe) -> Result<()> {
		// [ACTION PLAN]
		// 1. Loop through 'subscribe.filters'.
		// 2. For each filter:
		//    a. Insert (ClientID, Filter) into the global 'Topic Trie'.
		//    b. Determine the granted QoS (usually min(req_qos, server_max_qos)).
		// 3. Retained Messages:
		//    - Check if any Retained Messages match these filters.
		//    - If yes, immediately enqueue them to be sent to this client.
		// 4. Construct and send 'SubAck' with the list of Reason Codes.
		
		unimplemented!("Pending: Topic Trie Insertion, Retained Msg Matcher, SubAck Generation")
	}

	async fn handle_unsubscribe(&mut self, unsubscribe: mqtt_v5::Unsubscribe) -> Result<()> {
		// [ACTION PLAN]
		// 1. Loop through 'unsubscribe.topics'.
		// 2. Remove the matching subscriptions from the 'Topic Trie'.
		// 3. Construct and send 'UnsubAck'.
		
		unimplemented!("Pending: Topic Trie Removal, UnsubAck Generation")
	}

	// --- QoS Handlers ---

	async fn handle_puback(&mut self, puback: mqtt_v5::PubAck) -> Result<()> {
		// [ACTION PLAN] -> QoS 1 (Sender Side)
		// We received an ACK for a message we sent to the client.
		// 1. Find the message in 'Session State' (Infltght Window) using 'puback.pkid'.
		// 2. Mark it as acknowledged and remove it from the retry queue.
		
		unimplemented!("Pending: Session State Update (Remove from In-flight)")
	}

	async fn handle_pubrec(&mut self, pubrec: mqtt_v5::PubRec) -> Result<()> {
		// [ACTION PLAN] -> QoS 2 (Step 2: Sender Side)
		// We sent a QoS 2 message, and client replied with PubRec.
		// 1. Update 'Session State': Transition message status from 'Published' to 'Received'.
		// 2. Send 'PubRel' packet to the client.
		
		unimplemented!("Pending: Session State Update, Send PubRel")
	}

	async fn handle_pubrel(&mut self, pubrel: mqtt_v5::PubRel) -> Result<()> {
		// [ACTION PLAN] -> QoS 2 (Step 3: Receiver Side)
		// We received a PubRel from a publisher (Client).
		// It means the publisher knows we received the message (via our PubRec).
		// 1. Check 'Session State' to ensure we have the stored message for 'pubrel.pkid'.
		// 2. Commit the message: Actually publish it to subscribers now.
		// 3. Send 'PubComp' packet back to the publisher to finalize transaction.
		
		unimplemented!("Pending: Finalize Publish, Send PubComp")
	}

	async fn handle_pubcomp(&mut self, pubcomp: mqtt_v5::PubComp) -> Result<()> {
		// [ACTION PLAN] -> QoS 2 (Step 4: Sender Side)
		// Client received our PubRel and sent PubComp.
		// 1. The transaction is fully complete.
		// 2. Remove the message ID from 'Session State' entirely.
		
		unimplemented!("Pending: Clean up Session State")
	}
}
