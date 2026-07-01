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

## 3. Will messages

Store the CONNECT will; publish it on ungraceful disconnect (EOF/error), suppress it on clean DISCONNECT.

## 4. Authentication / ACL

Username/password (and/or enhanced auth) at CONNECT; topic-level publish/subscribe authorization. Wire ACL
checks into `handle_publish` / `handle_subscribe`, return proper reason codes.

## 5. CONNECT capability negotiation

Act on client properties (`Receive Maximum` flow-control quota, `Maximum Packet Size`, `Topic Alias
Maximum`) and advertise the matching server capabilities in CONNACK. `mqttbytes` already decodes them;
today we only advertise server keep-alive.

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
