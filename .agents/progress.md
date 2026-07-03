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
