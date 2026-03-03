# MQTT Control Packets Reference

This document provides a comprehensive, deep dive into the 14 standard Control Packets used in MQTT v5.0. Understanding
these is essential for implementing a correct and robust Broker.

The packets are categorized by their function within the protocol lifecycle.

---

## 1. Connection Management

These packets handle the initialization, authentication, and termination of a network connection between the Client and
the Broker.

### CONNECT

* **Direction:** Client ➡️ Server
* **Description:** The very first packet sent by a Client after a TCP connection is established. If the Broker receives
  any other packet first, it must close the connection.
* **Key Responsibilities:**
    * **Handshake:** Establishes protocol version (e.g., MQTT 5.0).
    * **Identification:** Provides the `Client ID`. If empty, the Broker must assign one.
    * **Session State:** The `Clean Start` flag tells the Broker whether to discard previous session data or resume an
      existing session.
    * **Keep Alive:** Sets the maximum time allowed between packets before the Broker considers the Client dead.
    * **Will Message:** (Optional) Defines a message to be published automatically if the Client disconnects
      ungracefully (e.g., network timeout).

### CONNACK

* **Direction:** Server ➡️ Client
* **Description:** The Broker's response to a `CONNECT` packet. The Client cannot send data until this is received.
* **Key Responsibilities:**
    * **Reason Code:** Indicates success (`0x00`) or specific failure reasons (e.g., "Bad Username/Password", "Protocol
      Error").
    * **Session Present:** A boolean flag telling the Client: "I found your old session and restored your
      subscriptions/queued messages."

### DISCONNECT

* **Direction:** Bidirectional (Client ↔️ Server)
* **Description:** Used to terminate the connection.
* **Client to Server:** Indicates a "Graceful Shutdown." The Broker must **discard** the Client's Will Message (since
  the disconnection was intentional) and close the socket.
* **Server to Client (MQTT 5.0):** The Broker can send this before closing the TCP connection to explain *why* it is
  kicking the client (e.g., "Session taken over", "Server shutting down").

---

## 2. Message Transport (Publishing)

These packets are the core of MQTT, responsible for moving actual application data.

### PUBLISH

* **Direction:** Bidirectional (Client ↔️ Server)
* **Description:** Carries the actual payload application data.
    * **Client -> Server:** A device sending data (e.g., a sensor sending temperature).
    * **Server -> Client:** The Broker delivering that message to subscribing clients.
* **Key Fields:**
    * **Topic Name:** The routing key (e.g., `home/kitchen/temp`).
    * **Payload:** The binary data.
    * **QoS (Quality of Service):** Determines how hard the network tries to deliver the message (0, 1, or 2).
    * **Retain Flag:** If set to `1`, the Broker must store this message and send it immediately to anyone who
      subscribes to this topic in the future.

---

## 3. Quality of Service (Flow Control)

These packets ensure message delivery based on the QoS level requested in the `PUBLISH` packet.

### ⚠QoS 1: "At Least Once"

Simple acknowledgment mechanism. Duplicates are possible if the ACK is lost.

#### PUBACK (Publish Acknowledgment)

* **Direction:** Bidirectional
* **Description:** The response to a QoS 1 `PUBLISH`.
* **Meaning:** "I have received the message and taken ownership of it. You can delete it from your retry queue."

---

### QoS 2: "Exactly Once"

The highest guarantee. Ensures the message arrives exactly once, preventing duplicates. This requires a 4-step
handshake.

#### PUBREC (Publish Received)

* **Direction:** Bidirectional (Response to `PUBLISH`)
* **Meaning:** "I received your QoS 2 message. I have stored it, but I haven't processed/forwarded it yet. I am waiting
  for you to release it."

#### PUBREL (Publish Release)

* **Direction:** Bidirectional (Response to `PUBREC`)
* **Meaning:** "I see that you received the message (PUBREC). I have discarded my copy. You are now free to
  process/deliver the message."
* *Note:* This packet ensures the sender knows the receiver is ready.

#### PUBCOMP (Publish Complete)

* **Direction:** Bidirectional (Response to `PUBREL`)
* **Meaning:** "Transaction complete. I have delivered the message. We can both delete the Packet ID."

---

## 4. Subscription Management

These packets allow clients to tell the Broker which topics they are interested in.

### SUBSCRIBE

* **Direction:** Client ➡️ Server
* **Description:** A request to listen to one or more topics.
* **Key Fields:**
    * **Topic Filters:** Strings describing interest (can include wildcards `+` and `#`).
    * **Requested QoS:** The maximum QoS the client wants to receive for this topic.

### SUBACK

* **Direction:** Server ➡️ Client
* **Description:** Confirmation of the subscription.
* **Key Responsibilities:**
    * Must contain a **Reason Code** for *every* filter in the `SUBSCRIBE` packet.
    * Example: "Filter 1 granted at QoS 0", "Filter 2 granted at QoS 1", "Filter 3 failed (Not Authorized)".

### UNSUBSCRIBE

* **Direction:** Client ➡️ Server
* **Description:** Tells the Broker to stop sending messages for specific topics.
* **Key Fields:** List of Topic Filters to remove.

### UNSUBACK

* **Direction:** Server ➡️ Client
* **Description:** Confirms that the subscriptions have been removed.

---

## 5. Maintenance (Keep Alive)

Used to keep the TCP connection open through firewalls and detect dead connections.

### PINGREQ (Ping Request)

* **Direction:** Client ➡️ Server
* **Description:** Sent by the Client if it has no other data to send but the `Keep Alive` timer is running out.
* **Meaning:** "I am still alive, please don't disconnect me."

### PINGRESP (Ping Response)

* **Direction:** Server ➡️ Client
* **Description:** The Broker's reply to `PINGREQ`.
* **Meaning:** "I hear you. The connection is healthy."

---

## Summary of Flows

### Connection Flow

```text
Client                          Broker
  |----------- CONNECT ---------->|
  |<---------- CONNACK -----------|
```

### QoS 1 Flow (At Least Once)

```text
Sender                          Receiver
  |----------- PUBLISH (ID=X) --->|
  |<---------- PUBACK  (ID=X) ----|
```

### QoS 2 Flow (Exactly Once)

```text
Sender                          Receiver
  |----------- PUBLISH (ID=Y) --->|
  |<---------- PUBREC  (ID=Y) ----|
  |----------- PUBREL  (ID=Y) --->|
  |<---------- PUBCOMP (ID=Y) ----|
```