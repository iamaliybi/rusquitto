# Changelog

All notable changes to rusquitto are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) (pre-1.0: the minor
version is bumped for new features, the patch version for fixes).

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

[0.5.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.5.0

[0.4.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.4.0

[0.3.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.3.0

[0.2.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.2.0

[0.1.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.1.0
