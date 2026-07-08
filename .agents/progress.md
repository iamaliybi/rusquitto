# Rusquitto — Implementation Progress

Tracks the build from the Phase 2 pub/sub plan through the 1.0.0 release. Updated 2026-07-03.

> **Layout note (1.0):** the crate was restructured. Historical entries below name the
> old paths — the current map is: `broker/engine.rs` → `broker/shard.rs` (+ `session.rs`,
> `mesh.rs`); `broker/topic_trie.rs` → `broker/topics/trie.rs` (+ `interner.rs`);
> `logger.rs` → `telemetry/logging.rs`; `metrics.rs` → `telemetry/metrics.rs`;
> `net/` → `transport/` (`tcp.rs`, `websocket.rs`, `ByteStream`); pure helpers in
> `protocol.rs`; orchestration in `lib.rs` with a thin `main.rs`.

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

## Phase 3e — Topic ACL (2026-07-02)

Per-user publish/subscribe authorization layered on Phase 3d auth.

| Item | Description | Status |
|------|-------------|--------|
| Config | `[[auth.users]]` gains optional `publish` / `subscribe` topic-filter allow-lists (`Option<Vec<String>>`, `None` = unrestricted) | ✅ |
| Authenticator | stores per-user ACLs in a `UserEntry`; `authorize_publish` / `authorize_subscribe` use `filter_matches` (anonymous & no-list = allowed, empty list = deny all) | ✅ |
| Connection | records the authenticated `username` at CONNECT for ACL checks | ✅ |
| Publish enforcement | `handle_publish`: deny → PUBACK/PUBREC `NotAuthorized` (QoS 1/2), silent drop (QoS 0); never fans out | ✅ |
| Subscribe enforcement | `handle_subscribe`: per-filter deny → SubAck `NotAuthorized`, trie not armed, no retained replay | ✅ |
| Will topic | an unauthorized will topic is dropped at CONNECT | ✅ |
| Unit tests | 5 ACL tests (unrestricted, anonymous, publish/subscribe wildcards, empty-list-denies) | ✅ |

**Verification (single shard):** `alice` limited to `sensors/#` — publish to `sensors/temp` routed to an
unrestricted watcher ✅; publish to `actuators/door` → client `Publish failed: Not authorized`, not routed ✅;
subscribe to `secret/#` → `subscribe not authorized` (SubAck NotAuthorized) ✅. 8/8 unit tests pass.

**Known limitation:** plaintext passwords; anonymous clients are unrestricted (no anonymous ACL).

## Phase 3f — Graceful shutdown (2026-07-02)

| Item | Description | Status |
|------|-------------|--------|
| Signal handling | `main` registers SIGTERM + SIGINT via `signal-hook` → sets a shared `Arc<AtomicBool>` | ✅ |
| Accept-loop stop | `worker::init` takes the flag; the accept loop races `accept()` against a 500 ms tick (`AcceptTurn` enum, `.or`) and breaks when set — `.or` polls accept first so no ready connection is lost to the tick | ✅ |
| Clean exit + flush | shards return, `LocalExecutorPoolBuilder::join_all` unwinds, `main` returns `Ok` → log guards drop and flush; exit code 0 | ✅ |
| New dep | `signal-hook = "0.4"` (glommio also pulls its own 0.3 transitively) | ✅ |

**Verification (single shard):** `kill -TERM` → broker exits with code 0; logs contain `shutdown signal
received, stopping accept loop` and `broker shut down` (previously SIGTERM killed the process before the
non-blocking appender flushed).

**Known limitation:** in-flight connection tasks are dropped on shutdown — no client DISCONNECT and `run()`
cleanup (session suspend / will) doesn't run. Draining connections is the next ops step
(see [next-steps.md](next-steps.md) item 7).

## Phase 3g — `$SYS` metrics (2026-07-02)

New `src/metrics.rs`: `Arc<Metrics>` of relaxed `AtomicU64` counters shared across shards.

| Item | Description | Status |
|------|-------------|--------|
| Counters | clients connected (gauge) / total, messages + bytes received/sent, uptime | ✅ |
| Increments | `connection.rs` — `client_connected`/`disconnected` (guarded by a `counted` flag), `message_received` in `handle_publish`, `message_sent` in `send_publish` | ✅ |
| Publisher | mesh **peer 0** publishes retained `$SYS/broker/...` every `[sys].interval` s (broadcast + `deliver_local`) | ✅ |
| Config | `[sys]` (`enabled` default true, `interval` default 10 s); validated non-zero | ✅ |

**Shard-election gotcha (fixed):** `glommio::executor().id()` is **1-based**, so the earlier `shard_id == 0`
guard never matched — the `$SYS` publisher (and the `authentication configured` startup log) never fired.
Both now use the 0-based mesh `peer_id()`.

**Verification (single shard, interval 2 s):** a `$SYS/#` subscriber received all eight topics; values matched
reality — `clients/total = 3`, `clients/connected = 1`, `messages/received = 2`, `bytes/received = 10`
(`hello`+`world`), version/uptime correct. `messages/sent` includes the `$SYS` deliveries to the subscriber
(expected). 8/8 unit tests pass.

## Phase 3h — Connection draining on shutdown (2026-07-02)

Completes graceful shutdown: connected clients are told the server is stopping instead of being dropped.

| Item | Description | Status |
|------|-------------|--------|
| Wake mechanism | `ShardState::shutdown_connections` drops every session's mailbox → each connection wakes via its existing `Outgoing(None)` arm (no per-connection timers) | ✅ |
| Client notice | on `Outgoing(None)` with the shutdown flag set, the connection sends DISCONNECT `ServerShuttingDown` (0x8B) via `send_disconnect`, suppresses its will, then runs normal cleanup (session suspends per expiry) | ✅ |
| Bounded drain | after breaking the accept loop, the shard calls `shutdown_connections` and waits (poll `conn_count`, `SHUTDOWN_GRACE = 5 s`) before returning | ✅ |
| Wiring | `Connection` gains the shared `Arc<AtomicBool>` shutdown flag (via `Connection::new`) | ✅ |

**Verification (single shard):** with clients connected, `kill -TERM` → the client logs `Received DISCONNECT
(139)` (0x8B), broker logs `draining connections connections:N` then `shard stopped remaining:0`, exit code 0.

## Phase 3i — Subscription options (2026-07-02)

No Local, Retain As Published, Retain Handling — all three MQTT 5 SUBSCRIBE options.

| Item | Description | Status |
|------|-------------|--------|
| Trie | `Subscription` gains `nolocal` + `retain_as_published`; `insert` returns `is_new` for Retain Handling | ✅ |
| No Local | `route` receives the publisher client id (via `deliver_local`/`fan_out`; `None` for mesh/internal) and skips the publisher's own matching sub | ✅ |
| Retain As Published | `Delivery.retain` = `was_retained && retain_as_published`; `send_publish` sets `message.retain` from it | ✅ |
| Retain Handling | `handle_subscribe` replays retained per `OnEverySubscribe` / `OnNewSubscribe` (only if new) / `Never` | ✅ |
| Overlap rule | when a client has several matching filters, routing uses the highest-QoS match's options (`Match` struct) | ✅ |
| Tests | 3 trie unit tests (is_new, options stored, resubscribe replaces) | ✅ |

**Verification (paho-mqtt v5, single shard):** 8/8 checks — No Local (publisher excluded, others included);
Retain As Published (retain kept for =1, cleared for =0 on live delivery); Retain Handling (Never / every /
new-vs-resubscribe). 11/11 cargo unit tests pass.

## Phase 3j — Cross-shard session resume (2026-07-03)

Completes the session story: a reconnecting client resumes even when `SO_REUSEPORT` lands it on a different
shard than the one holding its session. All shards share one bind address, so there is nothing to redirect to
(an MQTT 5 Server Reference is a dead end here) — the *session* migrates over the mesh instead.

| Item | Description | Status |
|------|-------------|--------|
| Mesh message type | `Senders<Publish>` → `Senders<MeshMsg>`; `MeshMsg { Publish(Publish), Control(Box<SessionControl>) }` (control boxed to keep the hot publish path small) | ✅ |
| Migration protocol | `SessionControl { Claim { client_id, requester, resume }, Handoff { client_id, session } }` exchanged over the mesh; `Claim` broadcast to peers, `Handoff` sent back to `requester` (targeted `try_send_to`) | ✅ |
| Session payload | `MigratedSession` carries owned data (mesh moves values across executors): subscriptions (flat `MigratedSub`), `inflight`, `incoming_qos2`, `next_pkid`, offline queue as `(Publish, QoS, bool)` (Rc unwrapped, re-wrapped on arrival) | ✅ |
| Trie extraction | `TopicTrie::take_client` removes a client's subscriptions and returns them with reconstructed filter paths | ✅ |
| Claim/await | `handle_connect`: on a non-clean connect that opened a *fresh* local session, `claim_remote_session` broadcasts a `Claim` and awaits replies (resolves when all peers answer or the first session arrives; `SESSION_CLAIM_TIMEOUT = 250 ms` fallback for a mesh-dropped reply). Clean Start broadcasts a discard | ✅ |
| Install | `install_migrated` re-arms subscriptions in the trie and hands the snapshot + offline queue to the connection, which resumes delivery (retransmit in-flight, flush offline) exactly as a local resume | ✅ |
| Single-shard no-op | `mesh_peers() == 0` short-circuits — the claim path never runs for `runtime.shards = 1`, so behaviour there is unchanged | ✅ |
| Pending claims | `ShardState::pending_claims: HashMap<client_id, LocalSender<Option<MigratedSession>>>` routes a `Handoff` back to the awaiting CONNECT handler; `on_control` dispatches inbound `Claim`/`Handoff` | ✅ |
| Unit test | `topic_trie::take_client_removes_and_returns_filters` (12/12 unit tests pass) | ✅ |

**Verification (paho-mqtt v5, `runtime.shards = 2`):** a `mover` client repeatedly disconnected (suspending its
session with a subscription), had 3 QoS 1 messages published while offline, and reconnected on a fresh socket
(new ephemeral port). **10/10 reconnects resumed** (`session_present = true`) and delivered all 3 queued messages
in order; broker logs show **7 cross-shard migrations** (`resumed session migrated from another shard`) with the
`mover` session bouncing between shard 1 and shard 2 — proving subscriptions *and* the offline queue travel with
the session (a lost subscription would starve the offline queue on the next iteration). The 8th claim was the
initial fresh connect (found nothing).

**Known limitations:** migration is best-effort under a saturated mesh (drop-on-full `try_send_to`) — a dropped
claim/hand-off falls back to a fresh session and the stranded one expires on its old shard (shares item 1's
backpressure gap). A cross-shard *takeover* of a still-live connection drops it without migrating in-flight state.

## Phase 3k — Shared subscriptions (2026-07-03)

MQTT 5 `$share/{group}/{filter}`: a group of sessions splits the load instead of every member getting a copy.

| Item | Description | Status |
|------|-------------|--------|
| Parse | `parse_shared_filter` in `connection.rs` splits `$share/{group}/{topic}` → `(effective, group)`; malformed (empty/wildcard group or empty topic) → SubAck `TopicFilterInvalid`. UNSUBSCRIBE mirrors the parse | ✅ |
| Trie | `Subscription` gains `share_group`; entries keyed by `(client_id, share_group)`, so a client may hold both an ordinary and a shared sub on the same filter. `remove` / `take_client` carry the group | ✅ |
| Route | `route` buckets shared matches by group into `groups: HashMap<group, HashMap<client, Match>>` and delivers to one member per group via a per-group round-robin cursor (`ShardState::shared_cursor`), preferring connected members; ordinary subs unchanged (each gets a copy). Extracted a `deliver_to` helper (online → mailbox, suspended → offline queue) shared by both paths | ✅ |
| Retained | shared subs never get retained replay on subscribe (`send_retained && share_group.is_none()`) | ✅ |
| CONNACK | `shared_subscription_available` flipped `0 → 1` | ✅ |
| Migration | `MigratedSub` carries `share_group` so shared subs survive cross-shard session resume | ✅ |
| Tests | `topic_trie::shared_and_regular_on_same_filter_coexist`; 13/13 unit tests pass | ✅ |

**Verification (paho-mqtt v5, `runtime.shards = 1`):** two members of `$share/g/shared/topic` + one ordinary
subscriber to `shared/topic`; 10 QoS 1 publishes → members split **5/5** with **each message delivered exactly
once**, ordinary sub got all **10**; after one member unsubscribed the group, the next **6** all went to the
remaining member (**6/0**). RESULT: PASS.

**Known limitation:** load balancing is per-shard — each shard selects among its *local* group members, so a
group whose members span shards receives one message per shard. Exact single-delivery for `runtime.shards = 1`
(or when a group's members share a shard). Globally-coordinated selection is future work.

## Phase 3l — Cross-shard QoS backpressure (2026-07-03)

Closes the last cross-shard best-effort gap: a QoS > 0 publish forwarded to a peer is no longer dropped when the
mesh link is full.

| Item | Description | Status |
|------|-------------|--------|
| Mesh handle | `ShardState.mesh` is now `Option<Rc<Senders<MeshMsg>>>`; `mesh_senders()` returns a clone so the publish path can `await send_to` without holding the `ShardState` borrow across the await | ✅ |
| Reliable forward | `Connection::fan_out` is async: QoS > 0 uses the awaiting `senders.send_to` (backpressure), QoS 0 keeps `try_send_to` (fire-and-forget). Local `deliver_local` runs after. The PUBACK/PUBREC is written only after `fan_out` returns, so the publisher is throttled instead of dropping | ✅ |
| Callers | `handle_publish` (QoS 0/1), `handle_pubrel` (QoS 2 commit), and the Will publish in `run()` cleanup all `.await` `fan_out`; the Will now forwards reliably too | ✅ |
| Best-effort remnant | `ShardState::broadcast` (sync `try_send_to`) is kept only for `$SYS` metric publishes (QoS 0, retained) | ✅ |
| No deadlock | each shard's mesh drain task only routes to local unbounded mailboxes (never blocks), so it keeps consuming — freeing peer links — while connection tasks await their sends | ✅ |

**Verification (`runtime.shards = 2`, `mesh_capacity = 4`, paho-mqtt v5):** four subscribers to `burst/topic`
(placement showed two on the publisher's shard 1, two on shard 2) + a publisher firing a **200-message QoS 1
burst** (50× the mesh buffer). All four subscribers — including the two cross-shard — received **all 200**
messages, zero loss. Under the previous drop-on-full forward the cross-shard subscribers would have lost most.
13/13 unit tests pass.

## Phase 3m — Protocol completions (2026-07-03)

A bundle of remaining MQTT 5 / ops items, shipped together.

| Item | Description | Status |
|------|-------------|--------|
| Will Delay Interval | `Connection.will_delay` from the will's `delay_interval`. `run()` cleanup publishes immediately when `min(will_delay, session_expiry) == 0`, else `ShardState::arm_will` stores `Session.pending_will = (will, deadline)`. `sweep_expired` returns due wills (delay elapsed, or session expired first); the sweep timer publishes them (best-effort). `open_session` clears `pending_will` on resume → reconnect within the delay cancels the will | ✅ |
| Inbound Receive Maximum | QoS 2 path: a new pkid past `incoming_qos2.len() >= max_inflight` → DISCONNECT `ReceiveMaximumExceeded` (0x93) | ✅ |
| Inbound topic aliases | CONNACK advertises `topic_alias_max = 16`; `handle_publish` resolves aliases first via `Connection.inbound_aliases` (register on topic+alias, substitute on empty-topic+alias); invalid → DISCONNECT `TopicAliasInvalid` (0x94) | ✅ |
| Hashed passwords | `[[auth.users]]` accepts `password_hash` (hex SHA-256) instead of `password`; config validates exactly one is set + 64 hex chars; `auth::Credential { Plain \| Sha256 }` verifies via the `sha2` crate | ✅ |
| RLIMIT_MEMLOCK | documented in the README Requirements section | ✅ |
| Tests | `auth::sha256_hashed_password`; 14/14 unit tests pass | ✅ |

**Verification (paho-mqtt v5, `runtime.shards = 1`):** one script, all PASS —
- **hashed password:** `alice` with `password_hash = sha256("s3cret")` → correct password connects (rc 0), wrong password rejected (rc 134);
- **will delay:** a will with delay 3 s + session expiry 60 s, victim killed abruptly → **not** delivered at 1.5 s, delivered exactly once at ~4 s (broker logs `arming delayed will message`);
- **will cancel:** same, but the client reconnects within the delay → the will is **not** delivered;
- **topic alias:** register alias 1 → `alias/topic`, then publish alias-only (empty topic) → subscriber receives both payloads on `alias/topic`.

Inbound Receive Maximum is a guard on the QoS 2 receive path (a conforming client can't exceed the quota it was
told), so it's covered by construction plus the unchanged QoS 2 flow.

## Phase 3n — Subscription identifiers (2026-07-03)

MQTT 5 Subscription Identifiers: a client tags a SUBSCRIBE with an id and the broker echoes it on matching
deliveries. Post-v0.4.0; bumps to 0.5.0.

| Item | Description | Status |
|------|-------------|--------|
| Trie | `Subscription` gains `sub_id: Option<usize>`; `insert` takes it. `take_client` now returns `FlatSub` structs (replacing the growing tuple), carrying `sub_id` for migration | ✅ |
| Route | `Match` and `Delivery` gain `sub_ids: Vec<usize>`; `route` accumulates the identifiers of *every* matching subscription per client (not just the QoS winner) and `deliver_to` threads them onto the `Delivery` | ✅ |
| Send | `send_publish` takes `sub_ids` and sets `PublishProperties.subscription_identifiers`; also **strips the publisher's `topic_alias`** on delivery (it's connection-scoped — a latent bug from the inbound-alias work) while passing other v5 properties through | ✅ |
| Subscribe | `handle_subscribe` reads `SubscribeProperties.id` (one per SUBSCRIBE, applies to all its filters) and passes it through; retained replays carry it too | ✅ |
| CONNACK | `subscription_identifiers_available` flipped `0 → 1` | ✅ |
| Migration | `MigratedSub` + the offline-queue tuple carry `sub_id` / `sub_ids`, so identifiers survive cross-shard resume | ✅ |

**Verification (paho-mqtt v5, `runtime.shards = 1`):** all PASS — a subscription with id 42 → delivery carries
`[42]`; two overlapping subscriptions (ids 1 and 2, distinct SUBSCRIBEs) → a single delivery carrying **both**
`[1, 2]`; a subscription with no id → delivery carries none. 14/14 unit tests pass.

## Phase 4 — 1.0.0 production release (2026-07-03)

Restructure, WebSocket transport, a memory optimization, and a security-hardening pass.

| Item | Description | Status |
|------|-------------|--------|
| Restructure | lib+bin split; `telemetry/`, `transport/`, `broker/{mesh,session,shard,topics}`, pure `protocol` module; dev-only `mosquitto` bin removed | ✅ |
| Transport abstraction (DIP) | `Connection<S: ByteStream>`; TCP implements `ByteStream` directly, `WsStream` wraps TCP in an RFC 6455 codec that also implements it | ✅ |
| WebSocket `:1884` | server handshake (SHA-1/base64 accept, `mqtt` subprotocol), masked-frame decode / binary-frame encode, ping/pong/close; size-capped handshake + frames; unmasked client frames rejected | ✅ |
| Topic interning | trie levels keyed by interned `Rc<str>` segments (`topics/interner.rs`); repeated names across the tree share one allocation | ✅ |
| Auth ordering | first packet must be CONNECT, only one allowed → closes pre-auth PUBLISH/SUBSCRIBE bypass | ✅ |
| Handshake + keep-alive timeouts | `connect_timeout` for the CONNECT wait; idle drop at 1.5× negotiated keep-alive | ✅ |
| Topic validation | client PUBLISH to `$`-prefixed/wildcard/empty/NUL topics rejected; malformed SUBSCRIBE filters refused per-filter | ✅ |
| Credential timing | constant-time compare + throwaway hash for unknown users; unguessable server-assigned client ids; client-id length/charset checks | ✅ |
| Resource caps | `max_session_expiry`, `max_subscriptions_per_client`, `max_retained_messages` (per shard), bounded per-connection pending-outbound queue | ✅ |
| Config | `[server] websocket`/`websocket_port`; `[limits]` security caps; single `rusquitto.config.toml` updated | ✅ |
| Deploy | hardened `deploy/rusquitto.service` systemd unit | ✅ |
| Tests | 32 unit tests (protocol, interner, websocket handshake/frame + existing shard/trie); clippy clean `--all-targets` | ✅ |

**Verification (release build, `runtime.cores = 2`):** paho-mqtt v5 pub/sub over **both** TCP `:1883`
and WebSocket `:1884` — QoS 0/1/2 delivered on each ✅. Security probes: a PUBLISH sent as the first packet
is dropped ✅; a socket that sends no CONNECT is closed within the handshake timeout ✅; a client PUBLISH to
`$SYS/broker/fake` triggers a `TopicNameInvalid` DISCONNECT ✅.

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

---

## Phase 5 — Connection memory diet + write coalescing (2026-07-05)

Target: high concurrency on 1-2 GB hosts (t4g-class). Baseline measured with
stress/memprobe.py (2000 conns, release): idle 24.9 KiB RSS/conn, 342.8 KiB
VmSize/conn, stalled-subscriber burst 86.9 KiB/conn, +66 MB retained after
close-all.

Top-3 hogs found: (1) glommio bounded local_channel PRE-ALLOCATES its ring —
MAILBOX_CAPACITY 8192 x 40 B Delivery = 320 KiB virtual per conn, resident after
bursts; (2) eager 4 KiB initial_read_buffer + 2 KiB temp_buf held across await
in every task future (+1 memcpy per read); (3) per-packet write path: fresh
BytesMut + one write_all (= 1 io_uring op / TLS record / WS frame) per packet.

Fixes: mailbox -> new_unbounded() + MAILBOX_LIMIT=256 drop-on-full guard at the
routing site (session.rs const, checked via LocalSender::len()); read path ->
lazy adaptive buffer (512 B-8 KiB chunk, resize/truncate directly into
BytesMut tail, cancel-safe via truncate(valid) after the race), buffers trimmed
when empty above 16 KiB; write path -> per-connection out: BytesMut buffer, all
sends append, one flush per event-loop wakeup (drain-parse -> drain-mailbox ->
flush -> block), FLUSH_THRESHOLD 16 KiB doubles as the stalled-consumer memory
ceiling. event_loop restructured accordingly (parse_packet is sync; the race
returns raw bytes). initial_read_buffer default 4096 -> 0 (0 = on demand).

Result (same probe): idle 16.1 KiB/conn RSS (VmSize 0.84 KiB), burst 31.1
KiB/conn. run() future = 3312 B, Connection = 832 B. Remaining ~12 KiB/conn is
glommio task/sources/channel + allocator overhead — needs a heaptrack pass
(follow-up). stress/soak.py added (churn/flood/stall/recover cycles, RSS trend
verdict): PASS, +0.7% over 14 cycles. Verified: 77 unit tests, clippy -D
warnings, mosquitto v5 QoS2 + retained round-trip on the new event loop.

GOTCHA: tests drive process_packet directly and now must flush — see
tests::drive(). GOTCHA: glommio LocalReceiver has no try_recv; non-blocking
drain uses futures_lite::future::poll_once(recv()).

---

## Phase 6 — Backlog clear (2026-07-05, v1.5.0)

All four remaining next-steps items, low->high priority:

1. Anonymous-client ACL: [auth] anonymous_publish / anonymous_subscribe
   allow-lists (None = unrestricted, [] = deny all); Authenticator gained
   anonymous_*_acl fields, authorize_* None-arm checks them.
2. Argon2id passwords: password_hash accepts * PHC strings (argon2
   crate; params ride in the string). Config validation requires salt+hash
   present (bare PHC parsing is lax!). Unknown-user timing dummy upgraded:
   reuses the first Argon2 user PHC as the dummy verify target when any user
   is Argon2 (else sha256 dummy). NOTE: verify blocks the accepting core
   10-50ms + ~19MiB transient per attempt (documented in config).
3. Outbound topic aliases: peer_topic_alias_max from CONNECT (capped
   OUTBOUND_TOPIC_ALIAS_MAX=32), per-conn outbound_aliases map. Applied in
   send_publish AFTER track_inflight so inflight copies keep the full topic
   (retransmit on a new conn has an empty alias table). Alias rolled back if
   the registering packet is dropped for peer max-packet-size.
4. Globally-coordinated shared subscriptions: MeshMsg::Shared(SharedEvent
   Join/Leave) broadcasts replicate CONNECTED members per group to all shards
   (hooks: subscribe, unsubscribe-last-filter, close_session suspend/destroy,
   open_session resume, install_migrated). shared_remote:
   HashMap<group, BTreeSet<client>>. route(): if a group has remote members,
   merged sorted view + deterministic content hash (shared_pick_index,
   fixed-key DefaultHasher over topic+payload) picks ONE member cluster-wide;
   only the owning shard delivers. Purely-local groups keep round-robin +
   suspended-member queueing (old tests unchanged). NoLocal on a shared sub =
   Protocol Error (MQTT5 3.8.3.1) -> SubAck TopicFilterInvalid; also stripped
   from persisted/migrated snapshots (it would desync the global pick, which
   was why the old per-publisher exclusion had to go).

Verified: 87 unit tests (incl. two-ShardState exactly-once simulation),
clippy -D warnings; E2E on a REAL 2-shard broker: 6 members split shards
1+2 (confirmed via logs), 40 publishes -> exactly 40 deliveries, all members
hit. Old behavior would have delivered ~80.

GOTCHA: PasswordHash::new() accepts "" (parses as salt-only)
— require .salt.is_some() && .hash.is_some() for a usable credential.

---

## Phase 7 — Memory deep-dive + small-host levers (2026-07-05, v1.6.0)

Built examples/allocprobe.rs (histogram global allocator + in-process broker;
no root needed — heaptrack/valgrind unavailable, no passwordless sudo). It
attributed the mystery ~13 KiB/conn in ONE allocation: the spawned task future.
Root causes, in layers: (1) serve()s state machine reserved every transport
branchs connection future at once; (2) inline Box::pin(fut).await does NOT
help — temporaries in a statement containing .await live across the suspension
and keep their frame slot; (3) even moved-from stream bindings (TlsStream,
WsStream values, ~2 KiB each) kept slots. Fix: box each transport PIPELINE via
plain-fn seams (boxed_run / boxed_serve_ws / boxed_serve_tls / boxed_serve_wss)
so the box is constructed on a normal stack frame. Task future 13144 -> 600 B
(measured via temporary size_of_val probe at the spawn site). Idle RSS 16.1 ->
7.5 KiB/conn; burst 31.1 -> 22.4. Remaining = run_stream box ~4.5 KiB (the
connection state machine) + ~1.4 KiB small allocs — fully attributed.

malloc_trim(0) every 30 sweeps on peer 0 (libc dep): post-burst RSS 51.0 ->
20.3 MB at the first tick (glibc arenas otherwise never return freed pages).

[server] socket_recv_buffer / socket_send_buffer -> SO_RCVBUF/SO_SNDBUF on the
listeners pre-listen(2); inherited by accepted sockets; verified via ss skmem
(rb16384/tb16384 for 8192 configured — kernel doubles).

aarch64: no root for apt, so zig 0.13 tarball + cargo-zigbuild; build with
cargo zigbuild --release --target aarch64-unknown-linux-gnu.2.31 (ring
compiles under zig cc). Release now ships the arm64 asset (untested on real
arm hardware — no qemu here; same code, cross-checked by the full x86 suite).

Transports re-verified after the serve() restructure: plain + mqtts (openssl
self-signed cert) + raw-RFC6455 ws client -> CONNACK. GOTCHA that cost an
hour: a hand-rolled test CONNECT with a wrong remaining-length byte made the
WS path look broken; the broker was fine.

---

## Phase 7b — Sub-4-KiB idle connections (2026-07-05, feat/connection-future-diet)

Idle RSS 7.5 -> 3.9 KiB/conn (allocprobe, 2000 conns). run() future 3312 ->
624 B. THE TECHNIQUE THAT WORKS (vs source-level slot elimination, which
provably does nothing): boxing through plain-fn seams —
1. fan_out mesh forward: try_send_to first (sync, common case); only a FULL
   link falls back to boxed send_to (backpressure preserved; QoS0 unchanged).
   Also reduce GlommioError<MeshMsg> (~230 B) to a bool before the await.
2. Hot-arm boxing: PUBLISH / PUBREL / CONNECT handlers boxed per packet
   (boxed_handle_* seams). One ~1KB-class alloc per such packet. Throughput
   A/B vs v1.6.0 binary (stresser, 400 conns QoS1 12s): 49.9k -> 57.2k msg/s
   — FASTER, because the in-place publish normalization killed a String
   alloc+copy per publish and try-send-first skips future setup.
3. parse+dispatch merged into process_one (one Packet slot; process_packet is
   GONE — tests drive via encode_packet + process_one, see tests::drive).
4. Rare data boxed out of hot structs: Connection.will, Session.pending_will,
   Session.snapshot (Option<Box<SessionSnapshot>>, None while connected) —
   the last two shrink every sessions-table slot (~400 -> ~100 B), visible as
   the amortized >16KiB class dropping 892 -> 463 -> ... B/conn.
Remaining floor ~1.7 KiB/conn = glommio task (600) + stream/source allocs
(1073) — below that means glommio-internal changes.

Verified: 87 tests, clippy -D warnings, QoS 0/1/2 smoke, WS smoke, 2-shard
shared-sub exactly-once (40/40), throughput A/B. probe_future_tree doc updated
as the regression watchpoint.

---

## Phase 8 — Architectural refactor (2026-07-05, v1.7.0)

Behaviour-preserving structure pass (allocprobe 3.9 KiB/conn unchanged; all
transport/persistence/shared-sub smokes pass; 87 tests):

- clippy.toml: disallowed-types (Mutex/RwLock/Condvar/mpsc/Barrier) +
  disallowed-methods (thread::spawn/sleep) => shared-nothing now MECHANICALLY
  enforced by the pre-commit clippy -D warnings. Test harnesses (alloc_probe,
  stresser examples) opt out with file-level #![allow(clippy::disallowed_methods)].
- server/worker.rs (882) -> server/shard.rs (run_shard, was init) + shard/
  {accept,serve,maintenance}.rs. New ConnCtx bundle killed the 7-arg serve chain
  + 4x too_many_arguments allows. ShardIds{executor,peer} bundle for the spawn
  orchestration. GOTCHA: LoadMonitor::new() returns Rc<Self>; passing &LoadMonitor
  and calling .clone() clones the REFERENCE (escape error) - take &Rc<LoadMonitor>,
  use Rc::clone.
- Renames: Connection.state->shard, buffer/out->inbound/outbound,
  ShardState.mesh->mesh_tx, broker/mesh.rs->messages.rs (vocabulary vs
  shard/mesh.rs behaviour), connection/ack.rs->control.rs, allocprobe->alloc_probe.
- broker/session.rs -> extracted Delivery/Mailbox/limits into broker/delivery.rs.
- NEXT_CLIENT_ID static AtomicU64 -> ShardState.next_assigned_id (shard-local;
  id embeds shard_id so still unique). Last mutable global gone.
- All 8 foo/mod.rs -> foo.rs (file-based modules). Pure git mv, zero code change.
- stress/stresser.rs registered as [[example]] with explicit path; needs
  #![allow(clippy::disallowed_methods)]; surfaced a latent is_multiple_of lint
  (invisible under bare rustc) - fixed.
- DEFERRED (documented, not done): multi-crate workspace split - premature at
  8.9k LoC, would break the child-module test pattern for zero isolation gain.
- Docs: CLAUDE.md architecture section + .agents/architecture.md Key Files table
  fully rewritten to current layout.

GOTCHA reconfirmed hard this session: backticks inside a double-quoted
wsl bash -lc "..." get command-substituted even around a single-quoted heredoc
delimiter. Use the Write/Edit tools for any content with backticks, never inline
python heredocs with backtick-containing strings.

## Phase 9 — Session WAL + mutual TLS (2026-07-05, v1.8.0)

Durability + transport-security release. 96 tests, clippy -D warnings clean;
three features validated with live brokers, not just unit tests.

- **Partial-frame stall guard** (the audit's 15th adversarial case). The 15th
  was a header-only truncated CONNECT (`0x10 0x0A`): a complete fixed header
  claiming 10 body bytes, then silence. Not a crash — reaped by connect_timeout —
  but its post-CONNECT sibling (keep_alive=0 => idle deadline None) was an
  UNBOUNDED slow-loris. Fix: track `partial_since` (when the current incomplete
  frame first appeared); `framing_deadline()` bounds it by connect_timeout even
  with keep-alive off; `earlier(deadline, framing_deadline)` folds the two.
  `partial_since` reset on each complete packet. Tested via a new StallStream
  (yields bytes once, then parks — a live-but-silent socket, unlike MockStream's
  EOF) driving the real event_loop with connect_timeout=1s.
- **Session WAL** (`persistence/wal.rs`, `[persistence] wal_flush_ms`, default
  200). Group-commit, NOT per-mutation: ShardState keeps a dirty/removed client-id
  set (cheap `HashSet::insert` on the hot path); the persistence task drains it
  each flush (`take_wal_batch`), serializes Upsert/Remove records, appends +
  fdatasyncs. Hooks: close_session (suspend=>dirty / expiry0=>removed),
  open_session (clean-start + resume => removed), sweep_expired (=>removed),
  routing deliver_to offline-enqueue (=>dirty), mesh handle_claim (=>removed).
  Replay is last-writer-wins per client id over the snapshot-seeded map; framed
  `[u32 len][kind][payload]` so a torn tail from a crash mid-append is skipped
  (idempotent => an un-truncated WAL replays harmlessly). A periodic checkpoint
  (full snapshot) truncates the log; when snapshot_interval=0, a 60s fallback
  bounds growth. GOTCHA: `if let Some(b)=state.borrow_mut().take_wal_batch()` in
  an if-let holds the RefMut across the append .await — bind in a `let` first.
  Verified: kill -9 between snapshots (snapshot_interval=3600), restart replayed
  2 WAL records, session_present=1, queued QoS1 message redelivered.
- **Mutual TLS + hot-reload** (`[tls] client_ca_file`, `require_client_cert`,
  `reload_interval`). Verifier: `WebPkiClientVerifier::builder_with_provider`
  (required, or `.allow_unauthenticated()` for optional). A cert-verified client
  with no MQTT username is granted via a threaded `tls_verified` bool
  (serve.rs `client_cert_present` => run_stream => Connection::new => connect.rs).
  Hot-reload is SHARD-LOCAL: acceptor is `Rc<RefCell<Option<TlsAcceptor>>>` (not
  the cross-thread shared Arc), a per-shard maintenance task watches cert/key/CA
  mtimes and swaps in a rebuilt acceptor; accept loop clones the current one per
  connection. Fits thread-per-core with no lock. GOTCHAs: `.build()` returns
  `Arc<dyn ClientCertVerifier>` not `Arc<WebPkiClientVerifier>`; rustls rejects
  X.509 v1 client certs (`UnsupportedCertVersion`) — openssl needs `-extfile`
  with an extension to emit v3. Verified live with an openssl CA: cert client
  accepted under allow_anonymous=false, certless client rejected at handshake,
  rotated server cert served to new handshakes after reload_interval.
- DEFERRED (=> next-steps): cert-CN => username ACL mapping (needs an X.509
  parsing dep; rustls verifies but doesn't expose the parsed subject).

## Phase 9b — idle-memory investigation (2026-07-06, v1.8.1)

Patch release. Lazily boxed the topic-alias tables (`Option<Box<AliasTables>>`;
connection.rs + publish.rs + delivery.rs) => idle 3.87→3.7 KiB/conn. Plus
`examples/park_probe.rs` (io-uring dev-dep): proved the parked-fd floor = 48-B
struct on one shared IORING_OP_POLL_ADD ring, 0.06 KiB heap / 0.08 KiB RSS (~46×,
under Mosquitto), wake 2000/2000. Full decomposition + staged parking plan in
next-steps.md item 1. NOT building parking yet (user gated it).

## Phase 10 — mesh reliability + hot-path efficiency (2026-07-06, v1.9.0)

Three audit follow-ups (user explicitly deferred the memory-density/parking item).
94 tests, clippy clean, multi-shard verified.

- **Reliable mesh control plane.** Claim/Handoff + shared-sub Join/Leave were
  `try_send_to` (drop-on-full) => a drop under overload desyncs the shared-sub
  membership pick (double/zero-deliver) or loses a migrating session. Now a
  per-shard reliable outbox: ShardState.control_tx (unbounded local_channel);
  `enqueue_control` (sync, non-dropping) replaces the three try_send_to sites
  (mesh.rs send_control_to/broadcast_shared/broadcast_claim); a foreground drain
  task (maintenance.rs spawn_control_drain) awaits rx.recv() and `send_to(peer,
  msg).await` (backpressure), FIFO so Join can't reorder past Leave. Only spawned
  with peers. $SYS/QoS0 stay best-effort. Control volume low => outbox small.
- **Batch-drain the inbound mesh receiver** (maintenance.rs): after recv() wakes,
  drain all queued msgs via `poll_once(receiver.recv())` without yielding =>
  one wake per forwarded burst, not one reschedule per msg (cross-shard CPU +
  tail latency). Factored the match into `handle_mesh_msg(&state, msg)`.
- **QoS1/2 delivery: one Publish clone, not two** (delivery.rs send_publish).
  Was: clone working copy + track_inflight clones again. Now: `retransmit_copy`
  cloned only when `peer_topic_alias_max > 0` (aliasing may clear the topic);
  else move `message` into inflight AFTER a successful write. inflight recorded
  post-write => rollback paths no longer remove it. track_inflight() deleted.
- Verified: 3-shard smoke — cross-shard 40/40, shared-sub exactly-once 60/60
  (31/29 split, no dupes/loss); battery 8/8; QoS0 3-shard 380k msg/s (≈ audit
  373k, no regression). GOTCHA: mqttwire.read_packet RAISES socket.timeout (not
  None) — catch it when draining. The QoS1 publisher-ack microbench doesn't touch
  send_publish (no delivery), so Task-2's clone win shows on fan-out, not that bench.

## Phase 11 — ack/latency measured at floor + explicit TCP_NODELAY (2026-07-06, v1.9.1)

Investigated the audit's two remaining perf items (per-core ack-bound parity;
cross-shard single-message latency). KEY RESULT: BOTH AT FLOOR, no app-level
headroom — measured, not guessed.

- Diagnostic (`/tmp/lat/ping.py`, synchronous 1-in-flight PUBLISH→PUBACK RTT):
  single-shard QoS1 p50=55µs / p90=76µs / p99=128µs, NO Nagle artifacts (tight
  tail) => `TCP_NODELAY` already effective. Per-request cost = mqttbytes parse +
  socket round-trip, at C-broker parity. Cross-shard delta = 1 cross-thread
  reactor wake (glommio-internal). Only way lower = the below-glommio parking /
  faster-mesh-wakeup tier (next-steps §1).
- Shipped two GENUINE-BUT-PERF-NEUTRAL changes: (1) `stream.set_nodelay(true)`
  EXPLICITLY per accepted socket in accept.rs (robustness/portability — the
  listener's option is inherited on Linux but that isn't contractual); (2)
  `fan_out` skips the mesh path (no senders clone, no self-only loop) when
  `mesh_peers()==0` (single-shard). A/B: new RTT p50=56µs (within noise).
- Told the user honestly: robustness/cleanup, NOT a throughput/latency win.
  CHANGELOG + next-steps say so. 94 tests, clippy/fmt clean.

## Phase 12 — end-to-end integration test suite + TESTING.md (2026-07-06, v1.9.2)

Closed the biggest test-coverage gap: E2E MQTT flows were only covered by Python
scripts run by hand, not `cargo test`. Now `tests/integration.rs` (15 tests) boots
a REAL broker in-process (`rusquitto::run`) on an ephemeral port and drives it with
a minimal Rust MQTT5 client (mqttbytes + std TcpStream). Covers: CONNACK, QoS0/1/2
full handshakes, downgrade-to-granted, retained replay+clear, +/# wildcards,
unsubscribe, persistent-session offline-queue replay, will-on-abrupt-close,
malformed-frame survival, auth (bad-pw/anon reject/success), ACL, cross-shard
delivery, shared-sub exactly-once. Brokers are lazily started + SHARED per config
via OnceLock (3 fixtures: default anon cores=1, auth cores=1, multishard cores=3 =
5 executor rings total — under WSL's low RLIMIT_MEMLOCK, works fine; stable across
3 runs, ~2s). GOTCHA: had to make logging init idempotent — `.init()`→`.try_init()`
in telemetry/logging.rs, else the 2nd broker in-process panics on the global
subscriber. Also a genuine robustness win for embedding. GOTCHA: the integration
test uses std::thread (client harness) so needs `#![allow(clippy::disallowed_methods)]`
like the examples. Test client key details: `read()` accumulates socket bytes +
`v5::read` until a frame; the broker's minimal DISCONNECT (E0 form) fails v5::read
=> treated as close; `recv()` auto-completes receiver-side QoS handshakes so the
window doesn't stall. Also wrote TESTING.md (root) — the full A-Z test strategy
(unit/integration/adversarial/crash-recovery/mTLS/soak/probes) + known gaps
(no parser fuzz yet; wss not E2E). 94 unit + 15 integration, clippy/fmt clean.

## Phase 13 — mTLS cert-CN → username ACL mapping (2026-07-06, v1.10.0)

The deferred mTLS follow-up (next-steps §2). A verified client cert's subject CN
becomes the MQTT username so `[[auth.users]]` ACLs apply per-device.

- Dep: `x509-parser = "0.18"` (rustls verifies the chain but doesn't expose the
  parsed subject). Already in the tree as a transitive DEV dep of rcgen, so its
  transitive deps (asn1-rs/der-parser/nom/oid-registry) were locked; now compiled
  into the release binary.
- tls.rs: new `pub enum TlsIdentity { None, Verified, Cn(String) }` +
  `client_tls_identity(stream, map_cn)` (reads peer_certificates leaf, extracts CN
  via `X509Certificate::from_der(...).subject().iter_common_name().next().as_str()`).
  Replaced the old `client_cert_present()->bool`.
- Threading: serve.rs computes the identity from `ctx.map_cert_cn`; ConnCtx gained
  `map_cert_cn` (set from `config.tls.cert_cn_as_username`); Connection's
  `tls_verified: bool` field/param became `tls_identity: TlsIdentity`.
- connect.rs: `cert_grant: Option<Option<String>>` computed from `&self.tls_identity`
  BEFORE mutating self (borrow-checker: match on the borrow, clone the CN, then
  assign self.username) — `Some(Some(cn))` => CN is the username, `Some(None)` =>
  verified but anonymous, `None` => normal `[auth]` check. Explicit username always
  takes the normal path (guard `_ if has_username => None`).
- Config: `[tls] cert_cn_as_username: bool` (default false). A `[[auth.users]]`
  entry named after the CN still needs a credential (validation), but the cert
  path never checks it — the cert IS the credential; the entry exists only to carry
  ACLs.
- Tests: 2 new unit tests (cert_cn_becomes_username_when_no_login,
  verified_cert_without_mapping_has_no_username) + a LIVE harness (openssl CA,
  client CN=sensor-01, user sensor-01 publish=["sensors/01/#"], cert_cn_as_username=
  true, allow_anonymous=false): in-ACL publish DELIVERED, out-of-ACL publish BLOCKED.
  96 unit + 15 integration, clippy/fmt clean. FUTURE: SAN fallback when no CN.

## Phase 14 — parked-connection idle path (2026-07-07, v2.0.0)

The connection-density project (old next-steps §1), all four phases in one PR.
Idle plain-TCP conns: task + io_uring read Source torn down after
`[parking] idle_grace_secs` (default 30, ON by default); only the fd — oneshot
POLL_ADD on a per-shard RAW io-uring (1024 SQ entries, ~100 KiB memlock; creation
failure degrades to parking-off with a warning) — plus a boxed ResumeState remain.
MEASURED: 0.68 KiB/conn live heap parked vs 3.8 on v1.10 (alloc_probe 2000 conns);
live-idle rose to ~5.0 (park-capable driver holds ctx across await) — with
parking disabled the v1.x path is used verbatim (zero regression). Battery 12/12
(4 new park scenarios), 111 unit + 22 integration tests.

Key design decisions (details in the code docs):
- Broker model: parked == suspended session (mailbox None, snapshot stored incl.
  next_pkid, expires_at None) + `parked`/`wake_pending` flags → mesh
  Claim/extract, persistence-skip need no special cases. deliver_to parked arm
  queues ALL QoS (client is connected!) + sends ONE deduped UnparkCmd::Wake.
  Shared-sub online filter counts parked (else cross-shard pick desyncs).
  persist_one skips parked; shutdown converts parked→suspended pre-snapshot.
- Connection: run()/event_loop return Flow{Closed,Park}; park predicate =
  connected && all QoS/buffers empty && no partial frame; park deadline
  (last_activity + grace) folded into the block race as a third deadline; the
  race resolving as Timeout PROVES nothing arrived (single-threaded shard) —
  transition is fully synchronous (complete_park is a plain fn: structural
  no-await invariant). into_parts()/resume() carry aliases BOTH directions,
  will+delay, peer limits, rate limiter; next_pkid travels via the session
  snapshot so migration keeps it.
- fd extraction: glommio TcpStream has NO IntoRawFd and mem::forget would leak
  its reactor Source → F_DUPFD_CLOEXEC then drop (dup shares the open file
  description). Resume via TcpStream::from_raw_fd.
- Ring task (parking.rs): glommio can't await a foreign eventfd (yolo_recv is
  recv(2), ENOTSOCK) → adaptive tick: 25ms empty / 1ms busy / 10ms quiet; CQE
  reap is a shared-memory read (zero syscalls when empty); egress Wakes race the
  tick so they're immediate. user_data = slab idx<<32 | generation tag (stale
  CQEs filtered); PollRemove on non-CQE removals. Runs on the DEFAULT queue
  (maintenance queue would starve parked ingress under overload). Parked
  keepalive: deadline frozen at park; 1s sweep closes + fires Will + suspends.
- ConnSlot moves INTO ParkedConn (parked conns still occupy limits + gate the
  shutdown drain); every registry-removal path pairs client_disconnected();
  park/unpark pair the new clients_parked gauge ($SYS/broker/parked-connections).
- Memory discipline: Connection built on the plain frame of boxed_run_tcp and
  moved as an ARG into drive_tcp (one slot, not two); complete_park is sync (its
  locals never enter the future); resume prelude boxed via plain-fn seam;
  can't-park fallback SPAWNS a fresh task instead of returning a Connection
  through the frame. Watch alloc_probe's 3072-class when touching drive_tcp.

GOTCHA (cost a debugging round): the parking task originally blocked purely on
wake_rx.recv() while the registry was empty — but connections park themselves in
WITHOUT signalling it (LocalSender isn't Clone, drive_tcp has no handle), so
after the FIRST park nothing reaped CQEs / swept deadlines; ingress + keepalive
tests failed while egress tests passed (their Wake unblocked the task as a side
effect). Fix: TICK_EMPTY=25ms heartbeat even when empty.

GOTCHA: park-herd once showed 497/498 right after the classic battery (20k-conn
churn); unreproducible in 6 clean runs with logs accounting 500=parked=resumed.
Loopback TIME_WAIT/port pressure, not broker state — but keep park-herd in the
battery, it's the regression net for exactly this class of bug.

## Phase 15 — QoS 1/2 performance investigation: the debug-logging tax (2026-07-07, v2.0.0)

User flagged the audit chart: QoS 0 · 3 shards 407k/s great, but QoS 1 36.5k vs
"Mosquitto ~83k" — why would a single-threaded C broker beat us 2×? Investigated
top-down with fresh measurements; the answer had three layers.

LAYER 1 — THE BUG (fixed): every bench config left `[logging]` at its default
`"info,rusquitto=debug"`, and publish.rs had a per-PUBLISH `debug!` (topic, qos,
retain, redacted payload). Under the default filter that event is FORMATTED AND
DISPATCHED on the shard thread for EVERY message: measured **~38 µs/msg of CPU**
(single-conn QoS1 ping-pong: 64.5 µs/msg CPU with it, 26.5 without; wall RTT
p50 51.1 → 37.5 µs). Every audit number — QoS 0 included — carried this tax;
Mosquitto never did. Fix: demoted the event to `trace!` (wire-level per-message
detail is what trace is for) with a comment carrying the measured cost. Default
filter now costs ~0 per message (remaining debug! sites are per-connection
lifecycle only). LESSON: a per-message event at debug level IS a hot-path
allocation+format+channel-send under the default filter — grep for `debug!` in
any per-message path before benchmarking, and bench configs must pin
`logging.level = "error"` anyway.

LAYER 2 — THE APPLES-TO-ORANGES: the audit's "Mosquitto ~83k (prior)" was a
SATURATING Rust-hammer number; rusquitto's 36.5k was the PYTHON ping-pong
harness (200 asyncio conns, 1 in flight each, client-capped ~33-40k). Reran both
brokers under identical harnesses (mosquitto with set_tcp_nodelay=true, its
default false throttles it ~2×).

LAYER 3 — THE STRUCTURAL RESIDUAL (documented in next-steps §1): built
`examples/wake_probe.rs` — a bare glommio echo loop = the runtime's per-wake
floor: ~30 µs RTT / 21.5 µs CPU per wake vs mosquito's full-MQTT 15.5 µs CPU on
epoll. Our MQTT layer adds only ~5 µs CPU over the floor. The floor amortizes
under load; residual: saturating per-core QoS1 ~11% behind. Candidates
(spin_before_park knob, ring tuning) in next-steps.

POST-FIX NUMBERS (same box, 4 vCPU WSL, mosquitto 2.0.18 w/ nodelay):
- 1-conn QoS1 ping-pong RTT p50: rusq 37.5 µs vs mosq 32.8 (was 51.1)
- CPU/msg ping-pong: rusq 26.5 µs vs mosq 15.5 (was 64.5)
- 200-conn Python harness: QoS0 328k vs 149k (2.2×); QoS1 36.1k vs 32.7k
  (+11%); QoS2 21.6k vs 17.4k (+24%) — rusquitto leads EVERY tier
- Saturating Rust hammer QoS1: 1-shard 77.6k vs mosq 87.1k (−11%);
  **3-shard 328.9k = 3.8× mosquitto's ceiling**
- publish→deliver p50: same-core 43.4 µs (was 60), 3-shard 60.9 (was 93),
  mosq 36.5

MEASUREMENT KIT (kept): examples/wake_probe.rs (runtime floor);
/tmp/perf/{rtt.py,cputime.py,dellat.py,matrix.sh,sat.sh} (session-local).
CPU-per-message via /proc/<pid>/stat utime+stime around N ping-pongs — the
decisive signal was CPU/msg ≈ wall/msg (broker never idle ⇒ per-wake work, not
waiting), and the echo probe splitting runtime floor from MQTT-layer cost.

## Phase 16 — memory + latency optimization: io_memory, spin_before_park, A0 study (2026-07-07, v2.1.0)

User: "maximum research on minimizing memory + peak optimization, keep shared-
nothing/thread-per-core, complete next-steps, deploy release." Did deep research
(glommio executor-tuning source dive) and landed the two SAFE, high-leverage wins
from the parity program; documented the flagship (dispatcher mode) design study;
deferred the dispatcher REWRITE to the next cycle per my own "prove on a prototype"
gate. Shipped as v2.1.0.

WIN 1 — io_memory (Workstream D, the headline). ROOT CAUSE of the empty-broker
baseline gap FOUND: glommio's `LocalExecutorBuilder`/`PoolBuilder` default
`io_memory = 10 << 20` = 10 MiB PER EXECUTOR (src/executor/mod.rs:74), pre-
registered with io_uring (IORING_REGISTER_BUFFERS) at startup which PINS/faults it
resident (sys/uring.rs:1306+). Confirmed empirically: empty RSS scaled 17.5/28.6/
40.0/51.7 MiB for 1/2/3/4 shards = ~11 MiB/shard. The network fast path uses
yolo_recv/send (plain syscalls) NOT the registered pool — only DMA file I/O
(persistence snapshots) draws from it, and it FALLS BACK TO THE HEAP when exhausted
(sys/uring.rs:143), so shrinking is safe (floor 64 KiB, sys/uring.rs:1304).
Added `[runtime] io_memory_kib` (default 512). lib.rs: `.io_memory(kib*1024)` on the
pool builder before on_all_shards. RESULT: 1-shard 17.5→8.1 MiB (Mosquitto's 7.6
parity!), 4-shard 51.7→13.1 (~1.6 MiB/shard). ZERO throughput/latency/parked-floor/
battery regression (verified: saturating QoS1 79.4k unchanged, 3-shard 359k, parked
0.68, battery 12/12). BONUS: the 10MiB×N pinned pool was almost certainly the WSL
multi-shard io_uring ENOMEM gotcha (RLIMIT_MEMLOCK); 512KiB×N fits easily — 4 shards
now boot clean on the WSL box.

WIN 2 — spin_before_park (Workstream B). glommio `spin_before_park(Duration)` on the
pool builder (executor/mod.rs:819) busy-polls completions before parking the reactor,
removing the io_uring park/unpark round-trip from single-message latency. CAVEAT
(from source): silently disabled under Unbound placement (executor/mod.rs:1157) —
only works under MaxSpread/MaxPack (our default). Added `[runtime] spin_before_park_us`
(default 0 = off; spinning burns idle CPU so opt-in). A/B at 50us: RTT p50 37.3→27.1us
(-27%), mean 41.4→30.1 — BEATS Mosquitto's 31.9us p50. The latency lever for
latency-critical steadily-busy shards.

Workstream C (active marginal +1.3 KiB): ATTRIBUTED to the two per-connection BytesMut
buffers (read + coalesced-write) growing to read_chunk under traffic, retained below
BUFFER_RETAIN_MAX. Shrinking harder trades vs throughput (re-growth). Correct fix =
dispatcher mode's shared per-shard scratch buffer. NOT pursued standalone. NOTE:
per-conn cost unchanged (idle 6.24, active 7.56 KiB/conn) but TOTAL 500-conn active
footprint dropped 21.4→11.8 MiB (-45%) purely from the baseline win.

A0 DESIGN STUDY (Workstream A flagship) — COMPLETE, decision recorded in next-steps:
ESCALATE-PER-CONNECTION wins over escalate-per-operation. A connection lives as a
ring state struct (like a parked conn), dispatcher handles simple ops inline
(PING, QoS0/1 publish w/ socket room, PUBACK/PUBREC) with NO per-conn task/source;
escalates to today's Connection stack (verbatim) for blocking ops (window-full,
throttle, partial frame, SUBSCRIBE+retained, CONNECT, TLS/WS), returns to ring after
= park/unpark generalized from idle-only to every-idle-moment. Reuses v2.0.0 parking
machinery wholesale. Sized: kills 2.3KiB future + 1.7KiB task/source → live conn
~1.0-1.3 KiB (Mosquitto territory), removes task-wake from hot path (closes the -7%
saturating gap + active-buffer marginal). A1 (prototype behind a flag + gate BEFORE
default) / A2 (multishot RECV) / A3 (QoS2 + flip default) are the next release cycle.
Deliberately NOT shipping the rewrite this cycle — my own gate says prove on a
prototype first, and a clean memory-win release shouldn't carry a hot-path rewrite.

Research method (kept): parallel Explore agent dove the glommio 0.9 source for the
exact tuning API + defaults + safety (io_memory floor/fallback, spin's Unbound
caveat, pool-builder setter availability). Empirical baseline decomposition
(RSS per shard count) confirmed the io_memory hypothesis before touching code.
Config test io_memory_below_floor_is_rejected. 112 unit + 22 integration, clippy clean.

## Phase 17 — the five audit deficits: honest disposition + parser fuzzing (2026-07-08, v2.1.1)

User: "fix these asap" listing the 5 audit weaknesses (1 active mem 6×, 2 CPU/msg
1.7×, 3 saturating -7%, 4 cross-shard 2×, 5 no fuzzing). Did NOT fake-fix — 4 of 5
are not quick fixes and one is not a defect. Disposition:

- 1/2/3 = ONE root cause (task-per-connection + io_uring wake floor). MEASURED that
  tuning can't touch 2/3: spin_before_park sweep 0/20/50/100/200us left saturating
  QoS1 FLAT at 81-82k (spin only helps single-message latency where the reactor
  parks between msgs; under load it never parks). So 2/3 are the amortized
  per-io_uring_enter cost, only reducible by dispatcher mode + multishot RECV
  (Workstream A2). Not a knob problem. 1 (active mem) also = dispatcher mode. All
  three GATED behind the A1 prototype — do NOT rush the hot-path rewrite.
- 4 = STRUCTURAL, not a bug. Cross-shard delivery needs one mandatory cross-thread
  reactor wake over the mesh; removing it needs cross-core shared state = breaks the
  shared-nothing invariant the user requires. Recorded in scope.md as accepted trade.
- 5 = FIXED NOW. proptest fuzz harness in server/connection/tests.rs::fuzz (3 props:
  parse_packet_never_panics 3000 cases, connected_dispatch_never_panics 256,
  preconnect_dispatch 256). Adversarial input dist: random / packetish (valid header
  + varint len + random body) / concatenated. Runs in cargo test (CI-continuous, not
  spot-check). Deep-validated PROPTEST_CASES=50000 (50k parser + 3k dispatch) — NO
  findings (mqttbytes + our guards robust). proptest = dev-dep. Closes TESTING.md gap.

Released v2.1.1 (patch — test-only, no runtime change). 115 unit (112+3 fuzz) + 22
integration, clippy/fmt clean, battery 12/12.

GOTCHA (self-inflicted): tried to append the fuzz module via `wsl bash -lc` heredoc
— the double-shell mangled the Rust `Ok(Some)`/`(...)` and truncated the file +
spuriously invoked cargo. Recovered: head -846 to truncate the corrupt block, then
re-appended via the Write/Edit tool. RE-LEARNED the CLAUDE.md rule: never heredoc
Rust code through the double shell; use Write/Edit.

The honest headline for the user: the real fix for 1/2/3 is dispatcher mode (the
gated flagship, A0 study done Phase 16); 4 is architectural; 5 shipped. Offered to
take on dispatcher mode as the dedicated next effort rather than rush it.

## Phase 18 — dispatcher mode prototyped, found non-viable on glommio 0.9 (2026-07-08)

User: "do and finish it" (dispatcher mode, the flagship that would close audit items
1/2/3 = active-conn memory 6×, CPU/msg 1.7×, saturating -7%). Committed to building it
behind a flag with an integrity gate (ship only if battery green + no guarantee
weakened). PROTOTYPE STOPPED AT THE GATE — the A0 design assumption is wrong for
ACTIVE connections, and I proved it with a measurement rather than shipping a broken
high-latency path.

THE FINDING (real glommio 0.9 limit): dispatcher mode = serve active connections off a
per-shard raw io_uring ring (no per-conn glommio task/source) = the parking model
generalized from idle to active. But the parking ring reaps CQEs on an adaptive 1-25ms
TIMER TICK — fine for idle parked conns (latency-tolerant), CATASTROPHIC for active.
MEASURED (/tmp/disp/wakelat.py): a parked conn woken via the ring = p50 3.1ms / p90
9.8ms per wake, vs 0.3ms on glommio's live reactor (and 27-37us for real active RTT).
~10x worse, 3 orders of magnitude over active latency.

ROOT CAUSE: on glommio the efficient low-latency I/O wait and the per-connection memory
are THE SAME THING. glommio gives us-latency readiness ONLY for its own per-conn Source
(the ~1.7KiB we wanted to remove). A foreign io_uring can only be polled on a timer tick
(ms) or by spinning a core (glommio can't await a foreign eventfd — yolo_recv is recv(2),
ENOTSOCK, established Phase 14). Parking works ONLY because idle conns tolerate ms wakes.
There is no cheap-memory + low-latency point for ACTIVE conns on glommio 0.9.

CONSEQUENCE: active-conn memory (audit item 1) is ARCHITECTURALLY BOUNDED on glommio
0.9 — same class as the cross-shard tax (Phase 17). Items 2/3 (CPU/msg, saturating)
share the root (the task/wake model IS the efficient-wait mechanism) and are likewise
bounded — Phase 17 already measured that tuning (spin sweep) can't touch them. So
audit items 1/2/3 are all now reclassified: not quick fixes, bounded by the runtime.

OPTIONS documented in next-steps (none a clean win, none scheduled): (a) different
runtime with multiplexed reactor access (huge); (b) spin-mode dispatcher (core-burn,
only OK on a dedicated already-saturated core, wrong default); (c) FuturesUnordered
shared-task multiplex (removes N task futures, KEEPS N glommio sources → partial win
~7.3→~4-5KiB, not parity, real complexity). Best risk-adjusted if item 1 is ever
prioritized, but still not Mosquitto's 1.2KiB.

ACTIONS: reverted the [dispatcher] config scaffold (won't ship a non-functional flag);
tree back to v2.1.1 baseline + these doc updates. NO release (no shippable code change
— the deliverable is the proven finding). Recorded in next-steps Workstream A, scope.md
(active-conn memory bounded, alongside cross-shard tax), and the audit ledger. This is
the integrity gate working as designed: measured the wall, didn't ship through it.

Kept: /tmp/disp/wakelat.py (parked ring-wake vs live-reactor latency probe) — the
evidence for the finding, reusable to re-check on a future glommio.
