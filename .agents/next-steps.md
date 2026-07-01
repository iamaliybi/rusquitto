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

## 6. Subscription options & shared subscriptions

`No Local`, `Retain As Published`, `Retain Handling`, and `$share/...` group subscriptions.

## 7. Observability & ops

`$SYS` metrics topics, connection/throughput counters, graceful shutdown on SIGTERM, and a documented
`RLIMIT_MEMLOCK` requirement (io_uring buffer registration `ENOMEM` under load — see progress.md).

## Code map for the above

- Session/QoS state: `src/server/connection.rs`
- Routing / retain / mesh: `src/broker/engine.rs`
- Subscription matching: `src/broker/topic_trie.rs`
- Config knobs: `src/config.rs` (add fields under `[limits]` / a new `[auth]` section)
