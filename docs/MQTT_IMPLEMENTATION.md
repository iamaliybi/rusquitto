# MQTT 5.0 Protocol Implementation Logic

> **Implementation status.** This document describes the intended MQTT 5.0 design. Some of it is built,
> some is still planned — the table below is the source of truth; sections further down explain the design
> and call out what is not yet wired up.
>
> | Area | Status |
> |--------------------------------------------|--------------------------------------------------|
> | Framing / fragmentation / coalescing | ✅ Implemented (`connection.rs::read_packet`) |
> | CONNECT / CONNACK | ✅ Implemented (assigns empty client ids; advertises server keep-alive) |
> | PUBLISH routing + retained messages | ✅ Implemented (wildcard trie, cross-shard mesh) |
> | QoS 1 / QoS 2 (in- and outbound) | ✅ Implemented (per-connection in-flight window) |
> | SUBSCRIBE / UNSUBSCRIBE + reason codes | ✅ Implemented |
> | Granted-QoS negotiation (`min(req, max)`) | ✅ Implemented |
> | Property parsing (handled by `mqttbytes`) | ✅ Decoded; most server-side properties not yet acted on |
> | Formal state-machine gating | ⛔ Planned — packets are dispatched by type, not gated by a state enum |
> | ACLs / auth, `No Local`, flow control | ⛔ Planned |
> | Session expiry / persistence / will msgs | ⛔ Planned — sessions are treated as clean |

## 1. Protocol Dispatch (intended: a Finite State Machine)

We implement the protocol logic as an asynchronous Finite State Machine strictly adhering to the MQTT 5.0 specification.

### Why an FSM?

* **Protocol Compliance:** The spec dictates strict transitions (e.g., a client cannot Publish before a `CONNACK` is
  sent). An FSM enforces these rules explicitly.
* **Security:** By isolating states (`Handshake`, `Active`, `Error`), we prevent state-confusion attacks where
  unauthenticated clients attempt to inject data.

### How it is Implemented today

* **Per-packet dispatch:** `Connection::process_packet` matches on the packet type and routes to a handler. Server-only
  packets (CONNACK, SUBACK, …) sent by a client are rejected as protocol violations.
* **Not yet a strict FSM:** there is no explicit `State` enum gating transitions, so the "must CONNECT first" ordering
  is not enforced beyond the empty-client-id handling. A formal state machine (`WaitConnect` → `Active` → `Error`) is
  planned.

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

### How it is Implemented today

* **Capability Response:** The `CONNACK` carries a property set (it must, even if empty — a v5 CONNACK without the
  property-length byte is malformed and clients reject it). We advertise the configured **Server Keep-Alive** so
  clients adopt our ceiling.
* **Planned:** acting on `Receive Maximum` (flow-control quota) and the other client-side properties, which `mqttbytes`
  already decodes for us.

---

## 5. Phase II: The Command Loop (Active Processing)

Once connected, the FSM enters the `Active` state to process command packets.

### Why Reason Codes?

* **Granularity:** In v5.0, an ACK isn't just "OK". We can return specific errors (e.g., `QuotaExceeded`,
  `TopicNameInvalid`) in `PUBACK`, `SUBACK`, and even `DISCONNECT`.

### How it is Implemented today

* **PUBLISH:** We route the message to local subscribers (wildcard-matched via the topic trie) and forward it to peer
  shards over the mesh; retained publishes update the retain table. QoS 1 replies with `PUBACK`; QoS 2 runs the
  PUBREC→PUBREL→PUBCOMP handshake and delivers exactly once. *(ACL checks are planned, not yet enforced.)*
* **SUBSCRIBE:** We insert each filter into the topic trie, grant `min(requested, server max)` QoS, replay any matching
  retained messages, and reply with a `SUBACK` carrying a reason code per filter. *(Subscription options such as
  `No Local` are parsed by `mqttbytes` but not yet acted on.)*

---

## 6. Phase III: Lifecycle & Persistence

Managing the life and death of a session is more complex in v5.0 due to "Session Expiry".

### Why Session Expiry?

* **The Problem:** In v3, `CleanSession=0` meant "keep data forever". This fills up the disk.
* **The Solution:** v5 allows a client to say "Keep my data for 1 hour after I leave".

### How it is Implemented today

* **Clean sessions only:** on disconnect (or EOF) we immediately deregister the client — its mailbox is dropped and all
  of its subscriptions are purged. There is no suspended state and no expiry timer yet.
* **Session takeover:** if a new connection registers with an existing Client ID, it overwrites the old mailbox; the
  displaced connection's mailbox channel closes and that connection tears down cleanly.
* **Planned:** honouring `Session Expiry Interval` (suspend instead of drop, with resurrection on reconnect) and
  publishing the **Will Message** on ungraceful disconnect.