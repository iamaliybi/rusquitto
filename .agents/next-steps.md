# Rusquitto — What's Next (Phase 3: Hardening)

Phase 2 (pub/sub engine) is **complete and verified** — topic trie, SUBSCRIBE/UNSUBSCRIBE, PUBLISH at
QoS 0/1/2 (in + out), retained messages, and cross-shard routing via the glommio channel mesh all work.
See [progress.md](progress.md) for the build log and design decisions.

The remaining work is correctness/robustness hardening, roughly in priority order.

## 1. Cross-shard QoS backpressure

The mesh forwards with non-blocking `try_send_to` (drop-on-full), so cross-shard QoS > 0 is best-effort.
Make it reliable: async `send_to` with per-link flow control, or a bounded retry/queue. Touch
`broker/engine.rs::broadcast` and the drain task in `worker.rs`.

## 2. Persistent sessions & expiry

Sessions are currently clean-only; in-flight QoS state is dropped on disconnect. Implement:

- Honour `Session Expiry Interval` — move to a "suspended" state with an expiry timer instead of dropping.
- Resurrection on reconnect with the same Client ID (re-attach subscriptions + in-flight window).
- Retransmission of unacked QoS 1/2 messages (with the DUP flag) on reconnect.

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
