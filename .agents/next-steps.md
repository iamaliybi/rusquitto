# Rusquitto — What's Next (Phase 3: Hardening)

Phase 2 (pub/sub engine) is **complete and verified** — topic trie, SUBSCRIBE/UNSUBSCRIBE, PUBLISH at
QoS 0/1/2 (in + out), retained messages, and cross-shard routing via the glommio channel mesh all work.
See [progress.md](progress.md) for the build log and design decisions.

The remaining work is correctness/robustness hardening, roughly in priority order.

## 1. Cross-shard QoS backpressure ✅ (Phase 3l)

**Done.** A QoS 1/2 publish forwarded to another shard now uses the awaiting mesh `send_to` (backpressure)
instead of the drop-on-full `try_send_to`, so a full mesh link makes the publisher wait rather than dropping.
The mesh `Senders` are held in an `Rc` (`ShardState::mesh_senders`) so `Connection::fan_out` can clone the
handle, drop the `ShardState` borrow, and `await` the send; the PUBACK/PUBREC is written only after the forward
returns, so the guarantee holds cross-shard. QoS 0 keeps `try_send_to` (fire-and-forget). No deadlock: each
shard's mesh drain task never blocks (it only routes to local unbounded mailboxes), so it keeps freeing peer
links while connection tasks await. Wills reuse the same reliable path; only `$SYS` metric publishes remain
best-effort (QoS 0, retained). Verified: `mesh_capacity = 4`, a 200-message QoS 1 burst, 2 shards — all four
subscribers (two on the publisher's shard, two cross-shard) received all 200 with zero loss.

## 2. Persistent sessions & expiry ✅ (cross-shard)

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

**Cross-shard session resume — done (Phase 3j).** `SO_REUSEPORT` can land a reconnecting client (new
ephemeral port) on a different shard than the one holding its session. Rather than redirect the client (all
shards share one bind address, so there is nothing to redirect to), the *session* migrates: the mesh now
carries a `MeshMsg::Control` variant with a `SessionControl { Claim, Handoff }` protocol. On a non-clean
CONNECT that finds no local session, the shard broadcasts a `Claim`; the owning peer replies with a `Handoff`
carrying the whole session (subscriptions pulled from the trie via `TopicTrie::take_client`, in-flight QoS
state, and the offline queue as owned data), which the new shard installs. Clean Start broadcasts a discard.
Verified across a 2-shard broker (a client bouncing between shards resumed with its offline queue intact every
time). Best-effort under mesh overload (drop-on-full), overlapping item 1; cross-shard *takeover* of a live
connection drops it without migrating in-flight state.

## 3. Will messages ✅

**Done.** The CONNECT Will Message is stored as a ready-to-route `Publish` on the connection
(`connection.rs::handle_connect`) and fired in `run()` cleanup when the loop ends abnormally
(EOF / IO error / non-normal DISCONNECT reason). A normal DISCONNECT (`0x00`) clears it so it is suppressed;
reason `0x04` (Disconnect With Will Message) keeps it. Takeover does **not** fire the displaced connection's
will — `close_session` returns whether this connection still owned the session, and the will is gated on that.

Also fixed here: a bare `E0 00` (zero-length) DISCONNECT — the usual graceful close — was being framed as an
EOF and skipping `handle_disconnect`; it is now synthesized into a normal `Disconnect` packet so the will is
correctly suppressed.

**Will Delay Interval — done (Phase 3m).** `Connection` captures the will's `delay_interval`; `run()` cleanup
publishes immediately when `min(will_delay, session_expiry) == 0`, else arms it on the suspended session
(`ShardState::arm_will` → `Session.pending_will = (will, deadline)`). `sweep_expired` now returns the wills that
have come due (delay elapsed, or the session expired first) and the per-shard timer task publishes them
(best-effort mesh forward). `open_session` clears `pending_will` on resume, so a reconnect within the delay
cancels the will. Verified: delay 3s not delivered at 1.5s, delivered once at 4s; reconnect within the delay
cancels it.

## 4. Authentication / ACL ✅

**Done.** `[auth]` config (`allow_anonymous` + `[[auth.users]]` username/password) builds a per-shard
`Authenticator` (`src/auth.rs`); `handle_connect` validates credentials before any session state and rejects
with CONNACK `BadUserNamePassword` (0x86) / `NotAuthorized` (0x87). Default config is open (anonymous, no users).

**Topic ACL** — each `[[auth.users]]` entry carries optional `publish` / `subscribe` topic-filter allow-lists
(`None`/omitted = unrestricted). `handle_connect` records the authenticated username; `handle_publish` denies
with PUBACK/PUBREC `NotAuthorized` (0x87) for QoS 1/2 and drops QoS 0; `handle_subscribe` denies per filter
with SubAck `NotAuthorized` (0x87) and doesn't arm the trie; an unauthorized will topic is dropped at CONNECT.
Anonymous clients are unrestricted.

**Hashed passwords — done (Phase 3m).** `[[auth.users]]` accepts `password_hash` (lowercase-hex SHA-256) as an
alternative to plaintext `password` (config validates exactly one is set and that the hash is 64 hex chars).
`auth::Credential { Plain | Sha256 }` verifies by hashing the supplied password (`sha2` dependency). Verified
end-to-end (good login accepted, wrong rejected).

**Remaining:**
- **Stronger hashing** — SHA-256 is unsalted and fast; a salted slow KDF (Argon2/bcrypt) would be better.
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

**Inbound Receive Maximum + inbound Topic Aliases — done (Phase 3m).** CONNACK now advertises a Topic Alias
Maximum (`Connection::INBOUND_TOPIC_ALIAS_MAX = 16`). `handle_publish` resolves inbound aliases up front
(register on topic+alias, substitute on empty-topic+alias) via a per-connection `inbound_aliases` map; an
out-of-range or unknown alias → DISCONNECT `TopicAliasInvalid` (0x94). The QoS 2 path enforces the advertised
inbound Receive Maximum (`incoming_qos2.len() >= max_inflight` on a new pkid → DISCONNECT
`ReceiveMaximumExceeded` 0x93). Topic-alias resolution verified end-to-end.

**Remaining:** *outbound* topic aliases (the server assigning aliases on the publishes it sends) and
subscription identifiers.

## 6. Subscription options & shared subscriptions ✅

**Subscription options done.** `mqttbytes` decodes them on each `SubscribeFilter`; the trie's `Subscription`
now carries `nolocal` + `retain_as_published`, and `insert` returns whether the subscription is new.
- **No Local** — `route` takes the publisher's client id (threaded through `deliver_local` / `fan_out`, `None`
  for mesh-forwarded and broker-internal publishes) and skips a matching subscriber that is the publisher.
- **Retain As Published** — `Delivery` carries a per-subscriber `retain` flag (`was_retained &&
  retain_as_published`); `send_publish` sets it, so live delivery keeps the retain bit only for RAP subs.
- **Retain Handling** — `handle_subscribe` replays retained on `OnEverySubscribe`, only when new on
  `OnNewSubscribe`, never on `Never`. When a client has overlapping filters, routing uses the options of its
  highest-QoS match.

**Shared subscriptions — done (Phase 3k).** `$share/{group}/{filter}` is parsed in `handle_subscribe` /
`handle_unsubscribe` (`parse_shared_filter` → effective filter + group; malformed → `TopicFilterInvalid`).
`TopicTrie::Subscription` gained `share_group`, and entries are keyed by `(client_id, share_group)` so a client
can hold both an ordinary and a shared sub on one filter. `route` buckets shared matches by group and delivers
to one member via a per-group round-robin cursor (`shared_cursor`), preferring connected members; ordinary subs
still each get a copy. Retained messages are not replayed to shared subs; CONNACK now advertises
`shared_subscription_available = 1`. Verified single-shard (5/5 round-robin split, exactly-once, coexists with
an ordinary sub, unsubscribe redirects). **Load balancing is per-shard** (each shard picks among its local
members) — globally-coordinated single delivery across shards is future work (overlaps items 1 & 2).

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

**`RLIMIT_MEMLOCK` — documented (Phase 3m).** The README Requirements section now calls out raising it
(`ulimit -l unlimited` / `LimitMEMLOCK=infinity`) to avoid the io_uring buffer-registration `ENOMEM` under load.

## Code map for the above

- Session/QoS state: `src/server/connection.rs`
- Routing / retain / mesh: `src/broker/engine.rs`
- Subscription matching: `src/broker/topic_trie.rs`
- Config knobs: `src/config.rs` (add fields under `[limits]` / a new `[auth]` section)
