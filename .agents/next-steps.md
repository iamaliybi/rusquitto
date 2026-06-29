# Rusquitto — What's Missing & Next Steps

## Phase 2 Priority Order

### 1. Topic Trie (Blocker for Everything)

Needed by SUBSCRIBE, PUBLISH, UNSUBSCRIBE. Must support:

- Exact match: `home/sensor/temp`
- Single-level wildcard: `home/+/temp`
- Multi-level wildcard: `home/#`

Suggested approach: trie where each node holds a `Vec<SubscriberHandle>`.  
Lives in `src/broker/engine.rs` (currently empty).

### 2. SUBSCRIBE / SUBACK

Handler stub in `src/server/connection.rs: handle_subscribe()`.

Steps needed:

1. For each filter: insert `(shard_id, client_id, QoS)` into Topic Trie
2. Negotiate granted QoS (min of requested vs. broker max)
3. Deliver any matching retained messages
4. Write SUBACK with per-filter result codes

### 3. PUBLISH routing

Handler stub: `handle_publish()`.

Steps:

1. Lookup matching subscribers in Topic Trie (wildcard-aware)
2. For same-shard subscribers: deliver directly via local channel
3. For cross-shard subscribers: send via SPSC inter-shard channel
4. QoS 0: fire-and-forget; QoS 1: send PUBACK; QoS 2: PUBREC flow
5. If retain flag: upsert Retain Table

### 4. UNSUBSCRIBE / UNSUBACK

Remove entries from Topic Trie, send UNSUBACK with result codes.

### 5. QoS 1 Session State

Per-connection `inflight: HashMap<PacketId, Publish>`.  
On PUBACK: remove from inflight.  
On reconnect with `clean_start=false`: re-send inflight with DUP flag.

### 6. QoS 2 Session State

Two-phase commit: PUBREC → PUBREL → PUBCOMP.  
Requires `inflight_qos2: HashMap<PacketId, Phase>`.

### 7. Global State Structures

All of these need careful partitioning for the shared-nothing model:

| Structure           | Options                                                  |
|---------------------|----------------------------------------------------------|
| **Topic Trie**      | Per-shard replica (reads local, cross-shard via message) |
| **Client Registry** | Sharded by `hash(client_id) % num_shards`                |
| **Retain Table**    | Per-shard or central shard with message passing          |

### 8. Inter-Shard Channels

`crossbeam` SPSC queues, one per shard pair (or a ring of channels).  
Wire into `src/broker/engine.rs` and thread through to `worker.rs`.

### 9. CONNECT Negotiation

Current CONNACK sends bare success with no properties. Should negotiate:

- `receive_maximum`
- `maximum_packet_size`
- `topic_alias_maximum`
- `server_keep_alive`

### 10. Will Messages

On unclean disconnect: publish the will message from the CONNECT packet.

---

## Known Stubs / TODOs in Code

- `src/broker/engine.rs` — 1 line, completely empty
- `src/broker/mod.rs` — just `pub mod engine;`
- `handle_publish()` — `unimplemented!()`
- `handle_subscribe()` — `unimplemented!()`
- `handle_unsubscribe()` — `unimplemented!()`
- `handle_puback()` — `unimplemented!()`
- `handle_pubrec()` — `unimplemented!()`
- `handle_pubrel()` — `unimplemented!()`
- `handle_pubcomp()` — `unimplemented!()`

---

## Test Script Notes (`scripts/mosquitto.sh`)

- Spawns 100 concurrent publishers per QoS level (0, 1, 2) + retain
- 50 messages per publisher
- 4 long-lived subscribers per scenario writing to `.log` files
- No automated pass/fail — logs must be manually inspected
- Requires `mosquitto_pub` and `mosquitto_sub` on PATH
