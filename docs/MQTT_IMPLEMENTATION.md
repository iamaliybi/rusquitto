# MQTT 5.0 Protocol Implementation Logic

## 1. Philosophy: The Finite State Machine (FSM)

We implement the protocol logic as an asynchronous Finite State Machine strictly adhering to the MQTT 5.0 specification.

### Why an FSM?

* **Protocol Compliance:** The spec dictates strict transitions (e.g., a client cannot Publish before a `CONNACK` is
  sent). An FSM enforces these rules explicitly.
* **Security:** By isolating states (`Handshake`, `Active`, `Error`), we prevent state-confusion attacks where
  unauthenticated clients attempt to inject data.

### How it is Implemented?

* **State Enum:** The `Session` struct holds a `State` variant.
* **Transition:** Every received packet triggers a check: `match (current_state, packet_type)`. If the pair is invalid (
  e.g., `WaitConnect` + `PUBLISH`), the connection is immediately terminated with a Protocol Error.

---

## 2. Ingestion Layer: Framing & Zero-Copy Decoding

TCP provides a stream of bytes, not packets. We must "Frame" this stream into discrete MQTT Control Packets.

### Why Framing is Critical?

* **Fragmentation:** A single `PUBLISH` packet might arrive in 3 separate TCP chunks.
* **Coalescing:** Multiple small packets (e.g., `PUBACK`, `PINGREQ`) might arrive in a single TCP read.

### How it is Implemented?

1. **Read Fixed Header (2 bytes):** We peek at the first byte for the Packet Type.
2. **Decode Variable Length:** We run the **Variable Byte Integer** algorithm to determine exactly how many more bytes
   to read.
3. **Buffer Allocation:** We allocate a buffer (or slice from a pre-allocated slab) of *exactly* that size.
4. **Read Payload:** We perform a `read_exact` for the remaining bytes. This ensures we process whole packets
   atomically.

---

## 3. Metadata Engine: Dynamic Property Parsing (MQTT 5.0)

Unlike v3.1.1, MQTT 5.0 packets contain extensive metadata called **Properties**.

### Why Properties?

* **Extensibility:** They allow adding features (like Message Expiry, Response Topic, User Properties) without breaking
  the binary format.
* **Flow Control:** They carry critical limits like `Receive Maximum`.

### How it is Implemented?

* **TLV Pattern:** Properties are encoded as **Type-Length-Value**.
* **The Parser Loop:** Inside the Variable Header decoder, we iterate through the Property Section. We switch on the
  `Property Identifier` (e.g., `0x11` for Session Expiry) and decode the value according to its data type (Byte, 4-Byte
  Int, UTF-8 String).

---

## 4. Phase I: The Handshake (Negotiation)

The `CONNECT` packet in MQTT 5.0 is a negotiation of capabilities, not just a login.

### Why Negotiation?

* **Resource Protection:** The broker must tell the client "I only support packets up to 16KB" or "I don't support
  Retained Messages".
* **Session Control:** The client dictates `Clean Start` (wipe memory) and `Session Expiry Interval` (how long to
  remember me).

### How it is Implemented?

* **Parameter Extraction:** We parse the `CONNECT` properties to set the session's `Receive Maximum` (flow control
  quota).
* **Capability Response:** In the `CONNACK`, we inject our own Properties (Server Capabilities) to inform the client of
  our limits.

---

## 5. Phase II: The Command Loop (Active Processing)

Once connected, the FSM enters the `Active` state to process command packets.

### Why Reason Codes?

* **Granularity:** In v5.0, an ACK isn't just "OK". We can return specific errors (e.g., `QuotaExceeded`,
  `TopicNameInvalid`) in `PUBACK`, `SUBACK`, and even `DISCONNECT`.

### How it is Implemented?

* **PUBLISH:** We validate ACLs. If successful, we route the message. If QoS > 0, we respond with a `PUBACK` containing
  a `Reason Code 0x00`.
* **SUBSCRIBE:** We process the new **Subscription Options** (like `No Local` to prevent echo). We insert filters into
  the Topic Trie and respond with a `SUBACK`.

---

## 6. Phase III: Lifecycle & Persistence

Managing the life and death of a session is more complex in v5.0 due to "Session Expiry".

### Why Session Expiry?

* **The Problem:** In v3, `CleanSession=0` meant "keep data forever". This fills up the disk.
* **The Solution:** v5 allows a client to say "Keep my data for 1 hour after I leave".

### How it is Implemented?

* **The Timer:** When a TCP disconnect happens, if `Session Expiry > 0`, we do *not* delete the Session struct. Instead,
  we move it to a "Suspended" state and start an expiry timer.
* **Resurrection:** If the client reconnects (with the same Client ID) before the timer fires, we re-attach the new TCP
  stream to the old Session struct (restoring subscriptions).
* **The End:** If the timer expires, we drop the data and publish the **Will Message** (if set).