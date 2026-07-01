# Rusquitto — Implementation Progress

Tracks completion of the Phase 2 plan (pub/sub). Updated 2026-06-29.

## Done & verified end-to-end

| Step  | Description                                                                                                                                                                                                    | Status |
|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| 1     | Bidirectional connection: shard-local `ShardState` (`Rc<RefCell>`) + per-connection glommio `local_channel` mailbox + `run()` select loop (`read.or(recv)`)                                                    | ✅      |
| 2     | `handle_subscribe` → register mailbox at CONNECT, `ShardState::subscribe`, SubAck with granted QoS (capped at QoS 0)                                                                                           | ✅      |
| 3     | `handle_publish` QoS 0 local fan-out via `ShardState::route` (shared `Rc<Publish>`, `try_send`)                                                                                                                | ✅      |
| 4     | Cross-shard routing via glommio `channel_mesh` (`MeshBuilder::full`), broadcast-to-all-peers                                                                                                                   | ✅      |
| 5     | Topic Trie with `+`/`#` wildcard matching (`src/broker/topic_trie.rs`); `route` dedups overlapping matches; UNSUBSCRIBE/UnsubAck implemented                                                                   | ✅      |
| 6a    | Retained messages — per-shard `retained` table, replicated to all shards via mesh `deliver_local`; replayed to new subscribers; empty payload clears                                                           | ✅      |
| 6b-i  | Inbound QoS 1/2 receiver handshake — QoS 1 PubAck; QoS 2 store-on-PUBLISH → PubRec → deliver-on-PUBREL (exactly once) → PubComp                                                                                | ✅      |
| 6b-ii | Outbound QoS 1/2 to subscribers — per-connection packet-id allocation + in-flight window; delivery at `min(publish QoS, granted QoS)`; PUBACK / PUBREC→PUBREL→PUBCOMP handlers; retained replay downgraded too | ✅      |

**Verification (Steps 1–4):** `mosquitto_pub`/`mosquitto_sub -V 5` on a 4-core box (3 shards). Publisher landed on Shard
1, subscriber on Shard 3 — message delivered across the mesh. Clean connect/subscribe/publish/disconnect.

**Verification (Step 5):** wildcard cases all correct — `sensor/+/temp`→`sensor/kitchen/temp` ✅, `home/#`→
`home/floor1/light` ✅, `sport/#`→`sport` (parent) ✅, `sensor/+/temp`✗`sensor/a/b/temp` (correctly rejected — `+` is
single-level).

**Verification (Step 6a/6b-i):** retained message replayed to a late subscriber ✅, cleared by empty-payload retain ✅;
QoS 1 publisher completes + subscriber receives ✅; QoS 2 publisher completes the full PUBREC/PUBREL/PUBCOMP handshake +
subscriber receives ✅.

## Phase 3a — Session persistence & expiry (2026-07-02)

`clients: HashMap<String, Mailbox>` in `ShardState` replaced by `sessions: HashMap<String, Session>`. A
`Session` outlives the `Connection`: `{ mailbox: Option<Mailbox>, expires_at, generation, snapshot, offline_queue }`.

| Item | Description | Status |
|------|-------------|--------|
| Session store | `open_session` (Clean Start → discard/resume) / `close_session` (expiry 0 → destroy, else suspend) / `sweep_expired` (per-shard 1 s timer task in `worker.rs`) | ✅ |
| Takeover | `next_generation` counter; `close_session` no-ops on generation mismatch so a displaced connection can't tear down the new session (also fixes a latent takeover race) | ✅ |
| Durable QoS state | `SessionSnapshot { inflight, incoming_qos2, next_pkid }` moved out of `Connection` on suspend, restored on resume. `Inflight` enum → `InflightMessage { publish, state }` so PUBLISHes can be retransmitted | ✅ |
| Offline queue | `route` buffers QoS > 0 for suspended sessions (`OFFLINE_QUEUE_LIMIT = 1024`, oldest dropped); `route` now takes `&mut self` | ✅ |
| Resume delivery | `Connection::resume_delivery` after CONNACK: retransmit in-flight (DUP; PUBREL for released QoS 2), then flush offline queue via `send_publish` | ✅ |
| CONNACK | `session_present` from resume result; `assigned_client_identifier` echoed for anonymous clients | ✅ |

**Verification (single shard, `runtime.shards = 1`):** new session → `session_present=false` ✅; reconnect same
id after `kill -9` → `session_present=true` (resumed) ✅; Clean Start → discarded (`false`) ✅; 1 s expiry + 3 s
wait → swept, reconnect `false` ✅; 3× QoS 1 published while offline → all 3 delivered in order on reconnect
(`flushing offline queue count:3`) ✅.

**Known limitation:** cross-shard resume (see [next-steps.md](next-steps.md) item 2) — `SO_REUSEPORT` may
rehash a reconnecting client to another shard. Exact within a shard; always exact for `runtime.shards = 1`.

## Phase 3b — Will messages (2026-07-02)

| Item | Description | Status |
|------|-------------|--------|
| Storage | CONNECT `last_will` → pre-built `Publish` on `Connection` (`will: Option<Publish>`), retain flag preserved | ✅ |
| Fire on abnormal close | `run()` cleanup publishes the will (broadcast + `deliver_local`) when the loop ends via EOF / IO error / non-normal DISCONNECT | ✅ |
| Suppress on graceful | `handle_disconnect` clears the will on reason `0x00`; keeps it on `0x04` (Disconnect With Will Message) | ✅ |
| No spurious will on takeover | `close_session` now returns `owned: bool`; will fires only if this connection still owned the session | ✅ |
| Zero-length DISCONNECT fix | `E0 00` was framed as EOF (skipping `handle_disconnect`); now synthesized into a normal `Disconnect` so the will is suppressed | ✅ |

**Verification (single shard):** will fires on `kill -9` (subscriber receives it) ✅; suppressed on a graceful
`mosquitto_pub` DISCONNECT (subscriber does *not* receive it) ✅; exactly one `publishing will message` logged.

**Known limitation:** Will Delay Interval treated as `0` (immediate) — see [next-steps.md](next-steps.md) item 3.

## Phase 3c — CONNECT negotiation & outbound flow control (2026-07-02)

| Item | Description | Status |
|------|-------------|--------|
| CONNACK capability advertisement | receive-max, max-packet-size, max-qos (< 2), retain-available, wildcard/sub-id/shared availability, topic-alias-max = 0 | ✅ |
| Receive Maximum (outbound) | `outbound_window() = min(client receive-max, max_inflight)`; over-window deliveries held in `pending_outbound`, released by `drain_pending` on PUBACK/PUBCOMP | ✅ |
| Pending survives suspend | `close_session` takes the pending deque and prepends it to the session's offline queue | ✅ |
| Maximum Packet Size (outbound) | encoded PUBLISH exceeding the client's limit is dropped, in-flight slot rolled back | ✅ |
| Windowed delivery paths | live fan-out, retained replay, and offline-queue flush all route through `deliver()` | ✅ |

Client props (`receive_maximum`, `max_packet_size`) captured in `handle_connect`; new `Connection` fields
`peer_receive_max`, `peer_max_packet_size`, `pending_outbound`.

**Verification (single shard, via `mosquitto -D CONNECT ...`):** client `maximum-packet-size 100` → a 300-byte
publish is dropped (`exceeds client max packet size`), only the small one delivered, connection stays up ✅;
client `receive-maximum 1` → 5 QoS 1 messages all delivered in order (drain works) ✅.

**Known limitation:** inbound Receive Maximum and Topic Alias not yet enforced — see [next-steps.md](next-steps.md) item 5.

## Phase 3d — Authentication (2026-07-02)

New `src/auth.rs`: `Authenticator { allow_anonymous, users: HashMap<String,String> }` built per shard from
`[auth]` config; `check(username, password) -> AuthResult { Granted | BadUserNamePassword | NotAuthorized }`.

| Item | Description | Status |
|------|-------------|--------|
| Config | `[auth]` with `allow_anonymous` (default true) + `[[auth.users]]` (username/password); validates empty/duplicate usernames | ✅ |
| Wiring | `Rc<Authenticator>` built once per shard in `worker.rs`, passed to `Connection::new`; startup log on shard 0 | ✅ |
| Enforcement | `handle_connect` authenticates before opening a session; failure → `reject_connect` writes CONNACK reason and closes | ✅ |
| Reason codes | `BadUserNamePassword` (0x86) for wrong user/pass, `NotAuthorized` (0x87) for forbidden anonymous | ✅ |
| Unit tests | `auth::tests` — open, anonymous-forbidden, good/bad password (3 tests) | ✅ |

Default config stays open (anonymous allowed, no users) so existing behaviour is unchanged.

**Verification (single shard):** correct creds connect + deliver ✅; wrong password → client exit 134
(`Bad User Name or Password`) ✅; anonymous → client exit 135 (`Not authorized`) ✅; broker logs both failures
with redacted credentials. Both sample configs parse with the new `[auth]` section.

**Known limitation:** passwords are plaintext; no topic ACL yet — see [next-steps.md](next-steps.md) item 4.

## Architecture decisions locked in

- **Mailbox payload:** `Rc<Publish>` for local fan-out; the mesh carries owned `Publish`, re-wrapped in `Rc` on the
  receiving shard.
- **`LocalSender` is NOT `Clone`** → registry holds **one mailbox per client** (`clients: HashMap<client_id, Mailbox>`);
  subscriptions reference clients by id, not by sender.
- **Mesh routing:** broadcast to all peers (each runs its own local `route`); `try_send_to` (drop-on-full), self
  skipped.
- **Empty client_id** (MQTT 5 lets the server assign): generated as `auto-{shard}-{counter}` via a static `AtomicU64`.
  Without this, all anonymous clients collide on `""` and trigger session-takeover on each other.
- **Granted QoS capped at QoS 0** (`SERVER_MAX_QOS`) until outbound QoS 1/2 session state (Step 6).

## ⚠️ mqttbytes 0.6 gotchas (cost real debugging time)

1. **`ConnAck::write` omits the mandatory v5 property-length byte when `properties == None`** — produces a malformed
   CONNACK that clients silently reject (they keep re-sending CONNECT). Fix: set
   `conn_ack.properties = Some(ConnAckProperties::new())`. NOTE: `SubAck`/`Publish`/`PubAck` handle `None` correctly —
   only `ConnAck` is broken.
2. **`mqtt_v5::read` rejects any zero-length packet except PING** with `Error::PayloadRequired` — including the valid
   bare `E0 00` DISCONNECT mosquitto sends. Worked around in `read_packet`: on `PayloadRequired` with first-byte
   high-nibble `14` (DISCONNECT), return `Ok(None)` (clean close).

## Phase 2 status: COMPLETE

The broker is a functional MQTT 5 pub/sub broker: CONNECT/CONNACK, SUBSCRIBE (wildcards) + SubAck, UNSUBSCRIBE +
UnsubAck, PUBLISH at QoS 0/1/2 (inbound *and* outbound), retained messages, PING, DISCONNECT, and cross-shard routing
over the glommio mesh. Builds with zero warnings.

### Known limitations / not implemented (deliberate scope)

- **Cross-shard QoS>0 is best-effort:** mesh forwarding uses `try_send_to` (drop-on-full, `MESH_CHANNEL_SIZE = 1024`). A
  burst exceeding the buffer drops cross-shard messages — so the "at least/exactly once" guarantee holds *within* a
  shard but is best-effort *across* shards. Fixing needs backpressure (async `send_to`) or per-link flow control.
- **No retransmission / persistent sessions:** all sessions are treated as clean. In-flight QoS 1/2 state is
  per-connection and dropped on disconnect; no redelivery timers, no session takeover replay.
- **No will messages, no auth/ACL, no CONNECT capability negotiation** (CONNACK carries only an empty property set), no
  `$SYS` topics, no flow control (`receive maximum`), no message/session expiry.

## Logging (tracing ecosystem)

`src/logger.rs` — production logging tuned for thread-per-core:

- **Non-blocking, lossy** `tracing_appender::non_blocking` (background writer thread; drops rather than blocking a
  pinned core under overload).
- **Layers:** pretty stdout (dev) / JSON stdout (prod, chosen by `cfg!(debug_assertions)`) + daily-rotating JSON
  `logs/rusquitto.log` + ERROR-only `logs/rusquitto.error.log`.
- **Dynamic levels:** global reloadable `EnvFilter` (`RUST_LOG` or default `info,rusquitto=debug`);
  `Guards::set_filter(...)` changes levels at runtime.
- **Spans:** one `info_span!("connection", shard, client_id = Empty)` per connection in `worker.rs`, attached via
  `.instrument(span)` (NOT `enter()` — async-safe). `client_id` backfilled in `handle_connect` via
  `Span::current().record(...)`, so every later log line carries it.
- **Redaction** (`logger::redact`): passwords never passed to the logger (`credentials()` masks to `[REDACTED]`);
  payloads logged as `<N bytes>` only. Verified: `SuperSecret123` / payload contents never appear in any log file.
- `Guards` (holding the `WorkerGuard`s) must be kept alive in `main` for the whole run.
- Deps: `tracing`, `tracing-subscriber` (features: env-filter, json, fmt), `tracing-appender`. `logs/` is gitignored.

### Outbound QoS design (6b-ii)

- `Delivery` (engine.rs) now carries `{ publish: Rc<Publish>, qos }` — the effective per-subscriber QoS =
  `min(publish QoS, max granted across that client's matching filters)`, computed in `ShardState::route`.
- The subscriber `Connection` owns `inflight: HashMap<u16, Inflight>` (`Qos1` / `Qos2Pending` / `Qos2Released`) and a
  rolling `next_pkid`. `send_publish` assigns a pkid for QoS>0; `handle_puback`/`handle_pubrec`(→PUBREL)/
  `handle_pubcomp` drive the rest.
- Inbound (`incoming_qos2`, keyed by the publisher's pkid) and outbound (`inflight`, our pkids) packet-id namespaces are
  independent — no collision on a connection that both publishes and subscribes.

### Trie design notes

- `src/broker/topic_trie.rs`: `Subscription { client_id, qos }` now lives here. `+`/`#` stored as ordinary segment keys.
  `#` matches the parent level too (`sport/#` matches `sport`). Wildcards don't match a first level starting with `$`.
- `route` collects matches then dedups by `client_id` (a client overlapping via several filters gets one copy).
- Build target note: binary is at `target/x86_64-unknown-linux-gnu/debug/rusquitto` (a custom default target triple is
  configured), NOT `target/debug/`.
