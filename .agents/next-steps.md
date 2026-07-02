# Rusquitto — What's Next (Phase 3: Hardening)

Phase 2 (pub/sub engine) is **complete and verified** — topic trie, SUBSCRIBE/UNSUBSCRIBE, PUBLISH at
QoS 0/1/2 (in + out), retained messages, and cross-shard routing via the glommio channel mesh all work.
See [progress.md](progress.md) for the build log and design decisions.

The remaining work is correctness/robustness hardening, roughly in priority order.

## 1. Cross-shard QoS backpressure

The mesh forwards with non-blocking `try_send_to` (drop-on-full), so cross-shard QoS > 0 is best-effort.
Make it reliable: async `send_to` with per-link flow control, or a bounded retry/queue. Touch
`broker/engine.rs::broadcast` and the drain task in `worker.rs`.

## 2. Persistent sessions & expiry ✅ (shard-local)

**Done.** `ShardState` now owns a `Session` per client id:

- `Session Expiry Interval` honoured — disconnect *suspends* the session (mailbox dropped, subscriptions
  retained in the trie, expiry deadline armed); `0` discards immediately, `0xFFFFFFFF` never expires. A
  per-shard timer task (`sweep_expired`) reclaims lapsed sessions.
- Resume on reconnect with the same Client ID (Clean Start `false`) → CONNACK `session_present = true`,
  subscriptions already armed, durable QoS state restored.
- Offline QoS > 0 messages buffered in `Session::offline_queue` (bounded) and flushed on resume.
- Unacked in-flight QoS 1/2 retransmitted with the DUP flag on resume (PUBREL resumed for released QoS 2).
- Session takeover (same Client ID, live connection) is generation-guarded so the displaced connection's
  cleanup can't clobber the new session.

**Remaining — cross-shard session resume.** `SO_REUSEPORT` may land a reconnecting client (new ephemeral
port) on a different shard, where its session doesn't exist. Needs a cross-shard session directory or an
MQTT 5 Server Reference redirect. Until then, resume is exact only within a shard (always, for
`runtime.shards = 1`). This overlaps with item 1 (cross-shard reliability) and the clustering goal.

## 3. Will messages ✅

**Done.** The CONNECT Will Message is stored as a ready-to-route `Publish` on the connection
(`connection.rs::handle_connect`) and fired in `run()` cleanup when the loop ends abnormally
(EOF / IO error / non-normal DISCONNECT reason). A normal DISCONNECT (`0x00`) clears it so it is suppressed;
reason `0x04` (Disconnect With Will Message) keeps it. Takeover does **not** fire the displaced connection's
will — `close_session` returns whether this connection still owned the session, and the will is gated on that.

Also fixed here: a bare `E0 00` (zero-length) DISCONNECT — the usual graceful close — was being framed as an
EOF and skipping `handle_disconnect`; it is now synthesized into a normal `Disconnect` packet so the will is
correctly suppressed.

**Remaining — Will Delay Interval.** Currently treated as `0` (the will fires immediately on abnormal
disconnect). Honouring a non-zero delay needs a timer that publishes the will after
`min(will_delay, session_expiry)` and is cancelled if the client reconnects first — the same machinery as the
session expiry sweep. Reuse `sweep_expired` / the per-shard timer task.

## 4. Authentication / ACL ✅

**Done.** `[auth]` config (`allow_anonymous` + `[[auth.users]]` username/password) builds a per-shard
`Authenticator` (`src/auth.rs`); `handle_connect` validates credentials before any session state and rejects
with CONNACK `BadUserNamePassword` (0x86) / `NotAuthorized` (0x87). Default config is open (anonymous, no users).

**Topic ACL** — each `[[auth.users]]` entry carries optional `publish` / `subscribe` topic-filter allow-lists
(`None`/omitted = unrestricted). `handle_connect` records the authenticated username; `handle_publish` denies
with PUBACK/PUBREC `NotAuthorized` (0x87) for QoS 1/2 and drops QoS 0; `handle_subscribe` denies per filter
with SubAck `NotAuthorized` (0x87) and doesn't arm the trie; an unauthorized will topic is dropped at CONNECT.
Anonymous clients are unrestricted.

**Remaining:**
- **Hashed passwords** — replace plaintext comparison with a salted hash (e.g. SHA-256 / Argon2); adds a
  hashing dependency.
- **ACL for anonymous clients** — currently anonymous is all-or-nothing (unrestricted); could add a default
  anonymous ACL if needed.

## 5. CONNECT capability negotiation ✅

**Done.** CONNACK advertises the full server capability set — Receive Maximum (`max_inflight`), Maximum
Packet Size (`max_payload_size`), Maximum QoS (when < 2), Retain Available, wildcard/subscription-id/shared
availability, and Topic Alias Maximum (0) — alongside the existing server keep-alive and assigned client id.

Client limits are stored and **enforced on the outbound path** (`connection.rs`):

- **Receive Maximum** bounds the unacked QoS 1/2 window (`min(client receive-max, max_inflight)`). Deliveries
  over the window are held in `pending_outbound` and released by `drain_pending` as PUBACK/PUBCOMP free slots.
  Held messages are preserved across a suspend (merged into the session's offline queue in `close_session`).
- **Maximum Packet Size** — an outbound PUBLISH larger than the client's limit is dropped (in-flight slot
  rolled back) rather than sent.

**Remaining:** inbound Receive Maximum enforcement (limit concurrent inbound QoS 1/2 the *server* accepts) and
Topic Alias support (we advertise 0, i.e. none accepted inbound, and send none outbound).

## 6. Subscription options & shared subscriptions — options ✅, shared remaining

**Subscription options done.** `mqttbytes` decodes them on each `SubscribeFilter`; the trie's `Subscription`
now carries `nolocal` + `retain_as_published`, and `insert` returns whether the subscription is new.
- **No Local** — `route` takes the publisher's client id (threaded through `deliver_local` / `fan_out`, `None`
  for mesh-forwarded and broker-internal publishes) and skips a matching subscriber that is the publisher.
- **Retain As Published** — `Delivery` carries a per-subscriber `retain` flag (`was_retained &&
  retain_as_published`); `send_publish` sets it, so live delivery keeps the retain bit only for RAP subs.
- **Retain Handling** — `handle_subscribe` replays retained on `OnEverySubscribe`, only when new on
  `OnNewSubscribe`, never on `Never`. When a client has overlapping filters, routing uses the options of its
  highest-QoS match.

**Remaining — shared subscriptions** (`$share/{group}/{filter}`): deliver each message to one member of the
group (load balancing) instead of all. Needs group-aware entries in the trie/route and a per-group picker.

## 7. Observability & ops — graceful shutdown ✅, rest remaining

**Graceful shutdown done.** `main` registers a SIGTERM/SIGINT handler (`signal-hook`) that sets a shared
`Arc<AtomicBool>`; each shard's accept loop races `accept()` against a 500 ms tick and breaks when the flag is
set, so `init()` returns, the executor pool unwinds, and `main` returns normally — flushing the non-blocking
log guards (previously a signal killed the process mid-write, losing buffered logs). Exits with code 0.

**`$SYS` metrics done.** `src/metrics.rs` — an `Arc<Metrics>` of relaxed atomics (clients connected/total,
messages + bytes in/out, uptime) shared across shards; mesh peer 0 publishes retained `$SYS/broker/...` topics
every `[sys].interval` seconds. Note: glommio executor ids are **1-based**, so shard election uses the 0-based
mesh `peer_id()`, not `executor().id()`.

**Connection draining done.** On shutdown each shard calls `ShardState::shutdown_connections` (drops every
session's mailbox), which wakes each connection via its already-handled `Outgoing(None)` path; the connection
sees the shutdown flag set, sends DISCONNECT `ServerShuttingDown` (0x8B), suppresses its will, and runs its
normal cleanup (session suspends per expiry). The shard then waits (bounded by `SHUTDOWN_GRACE = 5 s`) for the
live-connection count to reach 0 before returning. No per-connection timers — the wakeup reuses the mailbox.

**Remaining:**
- Documented `RLIMIT_MEMLOCK` requirement (io_uring buffer registration `ENOMEM` under load — see progress.md).

## Code map for the above

- Session/QoS state: `src/server/connection.rs`
- Routing / retain / mesh: `src/broker/engine.rs`
- Subscription matching: `src/broker/topic_trie.rs`
- Config knobs: `src/config.rs` (add fields under `[limits]` / a new `[auth]` section)
