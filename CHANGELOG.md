# Changelog

All notable changes to rusquitto are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html): from 1.0 on, the major
version bumps for breaking changes, the minor for features, and the patch for fixes.

## [1.0.0] - 2026-07-03

First production release. Adds a WebSocket transport, a production security pass,
and a memory optimization, on top of a restructured, SOLID-leaning codebase.

### Added

- **WebSocket transport (`:1884`).** Browser and Node clients can speak MQTT over
  WebSocket (RFC 6455 server handshake, `mqtt` subprotocol, binary frames) without
  a TCP bridge. Enabled by default; `[server] websocket` / `websocket_port` control
  it. Introduces a `ByteStream` transport abstraction so the MQTT state machine is
  written once and runs over both TCP and WebSocket.
- **Connection hardening.** The first packet must be CONNECT and only one is allowed
  (closing a pre-auth PUBLISH/SUBSCRIBE bypass); a socket that never sends CONNECT is
  dropped after `limits.connect_timeout` — including a stalled **WebSocket handshake**,
  which is bounded by the same timeout; an idle connection is dropped at 1.5× the
  negotiated keep-alive.
- **Topic reservation and validation.** Client PUBLISHes to `$`-prefixed topics
  (e.g. spoofing `$SYS`) or to wildcard/empty/NUL topics are rejected, and malformed
  SUBSCRIBE filters are refused per-filter.
- **Resource caps** (`[limits]`): `max_session_expiry`, `max_subscriptions_per_client`,
  `max_retained_messages` (per shard), a bounded per-connection outbound queue, and
  client-id length/charset validation. The per-connection outbound **mailbox is
  bounded**, so a subscriber that stops reading its socket can't force unbounded broker
  memory growth (excess deliveries to that stuck consumer are dropped). WebSocket
  control frames are validated (≤125 bytes, unfragmented) per RFC 6455.
- **Topic/filter depth cap** (128 levels). The subscription trie is walked recursively,
  so an unbounded-depth topic could overflow the executor stack (an uncatchable abort)
  and a deep SUBSCRIBE could balloon trie memory; both are now rejected up front.
- **Panic-safe connection accounting.** The live-connection count is released through
  an RAII guard, so a task that panics can't leak a slot and eventually wedge the shard.

### Changed

- **Constant-time credential comparison**, plus a throwaway hash for unknown users so
  authentication timing doesn't reveal whether a username exists. Server-assigned
  client ids are now unguessable (per-process random + counter) rather than sequential.
- **Topic-trie memory:** trie levels are keyed by interned `Rc<str>` segments, so a
  segment that recurs across many filters is stored once.
- **Project layout:** split into cohesive layers — `lib.rs` + thin `main.rs`,
  `telemetry/`, `transport/`, `broker/{mesh,session,shard,topics}`, and a pure
  `protocol` module — with the dev-only `mosquitto` bin removed.

## [0.6.1] - 2026-07-03

Internal quality pass — no behavior or configuration change.

### Changed

- **Engine unit tests** — the routing and session core is now covered by unit
  tests (fan-out and QoS downgrade, subscription-identifier accumulation, No Local,
  Retain As Published, shared-group round-robin, retained store/clear, session
  open/resume/destroy, generation guard, and the expiry/will sweep). 24 tests total.
- **Clean `cargo clippy`** — boxed the large `Event::Incoming` variant, derived
  `Config`'s `Default`, and collapsed a nested `if`.
- **`SubOptions` struct** replaces the long positional argument lists on
  `subscribe` / `TopicTrie::insert` (removing two `too_many_arguments` allows and
  the transposable adjacent `bool`s); `min_qos` is no longer duplicated.

## [0.6.0] - 2026-07-03

### Changed

- **`[runtime]` now takes a direct `cores` count instead of `shards` + `cpu_fraction`.**
  Set `cores = N` to run on N CPU cores (the broker pins one shard per core, so
  this is also the shard count); omit it to use every online core. A value above
  the online core count is clamped down with a warning. The old `shards` and
  `cpu_fraction` keys are removed — configs using them are now rejected.
- **Single reference config.** The `rusquitto.toml` and `rusquitto.default.toml`
  examples are replaced by one `rusquitto.config.toml` that lists every property
  with its default and a concise one-line comment.

## [0.5.0] - 2026-07-03

### Added

- **Subscription identifiers** — a client may attach a Subscription Identifier to a
  SUBSCRIBE; the broker stores it and echoes it on every matching PUBLISH, so the
  client can tell which subscription produced a message. When several of a client's
  subscriptions match one publish, all their identifiers are delivered. CONNACK now
  advertises subscription-identifier support.

### Fixed

- A delivered PUBLISH no longer carries the publisher's Topic Alias (that property
  is scoped to the publisher's connection); it is stripped on the way out.

## [0.4.0] - 2026-07-03

### Added

- **Cross-shard QoS > 0 backpressure** — a QoS 1/2 publish forwarded to another
  shard is now sent with an awaiting mesh `send_to` instead of the old drop-on-full
  `try_send_to`, so a full mesh link makes the publisher wait (its PUBACK/PUBREC is
  written only after the message is accepted on every shard) rather than silently
  dropping the message. The at-least/exactly-once guarantee now holds *across*
  shards, not just within one. QoS 0 stays fire-and-forget.
- **Shared subscriptions** (`$share/{group}/{filter}`) — members of a group split
  the load: each matching message is delivered to exactly one member, chosen
  round-robin (preferring connected members), while ordinary subscribers still each
  get their own copy. Retained messages are not replayed to shared subscriptions,
  and CONNACK now advertises shared-subscription support. *(Load balancing is
  per-shard; see the README limitations.)*
- **Will Delay Interval** — a Will Message with a non-zero delay is now published
  after `min(will delay, session expiry)` seconds instead of immediately, and is
  cancelled if the client reconnects within the delay.
- **Inbound Receive Maximum** — the broker now enforces the Receive Maximum it
  advertises: a client that exceeds the concurrent unacknowledged QoS 2 quota is
  disconnected with reason `0x93` (Receive Maximum exceeded).
- **Inbound topic aliases** — CONNACK advertises a Topic Alias Maximum and the
  broker resolves aliases on inbound PUBLISH (registering topic↔alias mappings and
  substituting the topic for alias-only publishes); an invalid alias is rejected
  with `0x94`.
- **Hashed passwords** — `[[auth.users]]` accepts a `password_hash` (lowercase-hex
  SHA-256) as an alternative to plaintext `password`, so the config need not store
  the secret in the clear.

## [0.3.0] - 2026-07-03

### Added

- **Cross-shard session resume** — when a client reconnects and the kernel's
  `SO_REUSEPORT` load balancing lands it on a different shard than the one holding
  its session, that session is now migrated to the new shard over the channel
  mesh instead of being treated as fresh. Subscriptions, unacknowledged in-flight
  QoS 1/2 state, and the offline message queue all move with it, so a resume is
  seamless regardless of which core the client hashes to. A Clean Start connect
  discards any session cluster-wide. Single-shard brokers are unaffected (there
  are no peers to migrate from).

## [0.2.0] - 2026-07-03

The broker grew from a basic pub/sub engine into a hardened MQTT 5 broker.
All changes are additive; there are no breaking changes to existing behavior.

### Added

- **Persistent sessions & expiry** — honors the Session Expiry Interval: a
  disconnect suspends the session (keeping subscriptions), a reconnect with the
  same Client ID resumes it (CONNACK `session_present`), QoS > 0 messages
  published while offline are queued and flushed on resume, and unacknowledged
  in-flight QoS 1/2 messages are retransmitted with the DUP flag. Session
  takeover on Client ID reuse is handled.
- **Will messages** — published on abnormal disconnect, suppressed on a normal
  DISCONNECT, and never fired for a connection displaced by a takeover.
- **CONNECT/CONNACK capability negotiation** — CONNACK advertises the server's
  capabilities; the client's Receive Maximum (windowed outbound in-flight limit)
  and Maximum Packet Size (oversized outbound publishes dropped) are enforced.
- **Authentication** — optional username/password via the `[auth]` config, with
  the proper CONNACK reason codes on failure.
- **Topic ACL** — per-user `publish` / `subscribe` topic-filter allow-lists.
- **`$SYS` metrics** — retained `$SYS/broker/...` topics (uptime, clients,
  messages, bytes) published on a configurable interval (`[sys]`).
- **Graceful shutdown** — on SIGTERM/SIGINT the broker stops accepting, sends
  connected clients a `ServerShuttingDown` DISCONNECT, suspends their sessions,
  flushes logs, and exits cleanly.
- **Subscription options** — No Local, Retain As Published, and Retain Handling.

## [0.1.0] - 2026-06-30

### Added

- Initial release: thread-per-core MQTT 5 broker on glommio (io_uring,
  `SO_REUSEPORT`). CONNECT/CONNACK, PUBLISH at QoS 0/1/2 (in and out),
  SUBSCRIBE/UNSUBSCRIBE, PINGREQ/PINGRESP, DISCONNECT; topic-trie wildcard
  matching (`+` / `#`); retained messages; cross-shard routing over a glommio
  channel mesh; structured `tracing` logging; and TOML configuration with a CLI.

[1.0.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.0.0

[0.6.1]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.6.1

[0.6.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.6.0

[0.5.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.5.0

[0.4.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.4.0

[0.3.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.3.0

[0.2.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.2.0

[0.1.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.1.0
