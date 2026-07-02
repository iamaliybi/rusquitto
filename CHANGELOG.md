# Changelog

All notable changes to rusquitto are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) (pre-1.0: the minor
version is bumped for new features, the patch version for fixes).

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

[0.2.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.2.0
[0.1.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.1.0
