# rusquitto

A high-performance **MQTT 5.0 broker** written in Rust on a strict **thread-per-core** architecture,
powered by [`glommio`](https://github.com/DataDog/glommio) and Linux `io_uring`. Every CPU core runs an
isolated, shard-local executor — no `Mutex`, no `RwLock`, no work-stealing.

> **Status:** functional pub/sub broker. Connect, subscribe (with wildcards), publish at QoS 0/1/2,
> retained messages, and cross-shard routing all work. Not yet production-hardened — see
> [Limitations](#limitations).

## Features

- **MQTT 5.0** over TCP: CONNECT/CONNACK, PUBLISH, SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP, DISCONNECT.
- **QoS 0/1/2**, both inbound (receiver-side) and outbound (sender-side) — full PUBACK and PUBREC→PUBREL→PUBCOMP
  handshakes, with exactly-once delivery for QoS 2.
- **Topic wildcards** `+` (single level) and `#` (multi level) via a topic trie; `$`-topics are excluded from wildcard
  matches.
- **Retained messages** — stored, replayed to new subscribers, cleared by empty payload, replicated across shards.
- **Persistent sessions** — honours the Session Expiry Interval: a disconnect *suspends* the session (keeping its
  subscriptions), a reconnect with the same Client ID and Clean Start `false` resumes it (CONNACK `session_present`),
  QoS > 0 messages published while offline are queued and flushed on resume, and unacknowledged in-flight QoS 1/2
  messages are retransmitted with the DUP flag. Session takeover on Client ID reuse is handled.
- **Cross-shard session resume** — because `SO_REUSEPORT` may load-balance a reconnecting client onto a *different*
  core than the one holding its session, the session is **migrated across the channel mesh** to wherever the client
  lands: its subscriptions, in-flight QoS 1/2 state, and offline queue all move with it, so resume is seamless on any
  core. A Clean Start connect discards the session cluster-wide.
- **Will messages** — the CONNECT Will is published when a client drops abnormally (EOF, network error, or a
  DISCONNECT that requests it) and suppressed on a normal DISCONNECT; a session takeover never fires the
  displaced connection's will. A **Will Delay Interval** delays the will by `min(will delay, session expiry)`
  seconds and is cancelled if the client reconnects within the delay.
- **CONNECT capability negotiation** — CONNACK advertises the server's Receive Maximum, Maximum Packet Size,
  Maximum QoS, Retain Available, Topic Alias Maximum, and wildcard/shared/subscription-identifier availability.
  The client's **Receive Maximum** (a windowed outbound in-flight limit) and **Maximum Packet Size** (oversized
  outbound publishes are dropped) are enforced; the broker also enforces its **own** inbound Receive Maximum
  (a client exceeding the concurrent-QoS-2 quota is disconnected with `0x93`), and resolves **inbound topic
  aliases** so clients can shrink repeated topics.
- **Authentication & ACL** — optional username/password at CONNECT via the `[auth]` config (`allow_anonymous`
  plus a list of users). Passwords may be stored as plaintext `password` or a SHA-256 `password_hash`. Failures
  are rejected with the proper CONNACK reason code (`0x86` bad credentials, `0x87` anonymous not allowed). Each
  user may carry `publish` / `subscribe` topic-filter allow-lists: a denied publish is dropped (QoS 0) or
  Not-Authorized-acked (QoS 1/2), and a denied subscribe gets a Not Authorized reason code. Defaults are open,
  so nothing is required until you configure it.
- **Cross-shard routing** over a `glommio` channel mesh, so a publisher and subscriber on different cores still reach
  each other. QoS 1/2 forwards apply **backpressure** (an awaiting mesh send), so the delivery guarantee holds across
  shards — the publisher waits rather than dropping when a mesh link is full. QoS 0 stays fire-and-forget.
- **Thread-per-core, shared-nothing**: `SO_REUSEPORT` kernel load balancing, one `io_uring` ring and one `LocalExecutor`
  per shard, lock-free shard-local state.
- **Structured logging** (`tracing`): non-blocking file appenders, daily rotation, a dedicated error log, per-connection
  spans tagged with `client_id`, and redaction of passwords and payloads.
- **Subscription options** — MQTT 5 **No Local**, **Retain As Published**, and **Retain Handling**
  (`OnEverySubscribe` / `OnNewSubscribe` / `Never`) are all honored on the SUBSCRIBE path.
- **Subscription identifiers** — a SUBSCRIBE's Subscription Identifier is stored and echoed on every matching
  PUBLISH so the client can tell which subscription produced a message; all matching identifiers are delivered
  when several of a client's subscriptions match.
- **Shared subscriptions** — `$share/{group}/{filter}` groups load-balance: each matching message goes to just
  one member of the group (round-robin, preferring connected members), while ordinary subscribers still each get
  a copy. Retained messages are not replayed to shared subscriptions. *(Load balancing is per-shard — see
  [Limitations](#limitations).)*
- **`$SYS` metrics** — the broker publishes retained `$SYS/broker/...` topics (uptime, connected/total clients,
  messages and bytes in/out) on a configurable interval, so you can monitor it over MQTT by subscribing to
  `$SYS/#`.
- **Graceful shutdown** on `SIGTERM` / `SIGINT`: shards stop accepting, connected clients are sent a
  `ServerShuttingDown` DISCONNECT and their sessions suspended cleanly, the process exits with code 0, and
  buffered logs are flushed instead of being lost to an abrupt kill.
- **TOML configuration** with a typed, validated schema and a CLI.

## Requirements

- **Linux** with a kernel supporting `io_uring` (**5.8+**).
- A Rust toolchain (2024 edition).
- **`RLIMIT_MEMLOCK`** high enough for `io_uring` buffer registration. The default (often 64 KiB) is fine for
  light use, but under connection bursts a low limit surfaces as a `glommio` `ENOMEM`. Raise it before running:
  `ulimit -l unlimited` (shell) or `LimitMEMLOCK=infinity` (systemd unit).
- For the example clients below: `mosquitto-clients` (`mosquitto_pub` / `mosquitto_sub`).

## Quick start

```bash
# Build
cargo build --release

# Run with a config file (the path is a positional argument)
cargo run --release rusquitto.config.toml
```

> **Note:** the config path is positional, so `cargo run rusquitto.config.toml` works directly.
> Don't write `cargo run --config ...` — `--config` is a *Cargo* flag and Cargo will intercept it.
> Run the built binary the same way: `./rusquitto rusquitto.config.toml`.

By default the broker binds `127.0.0.1:1883` and is **silent in the terminal** (logs go to files under
`logs/`). To watch logs live, set `enable_terminal = true` under `[logging]`, or `tail -f logs/rusquitto.log`.

### Try it with mosquitto

```bash
# Subscribe (terminal 1)
mosquitto_sub -h 127.0.0.1 -p 1883 -V 5 -t 'home/+/temp' -q 1

# Publish (terminal 2)
mosquitto_pub -h 127.0.0.1 -p 1883 -V 5 -t 'home/kitchen/temp' -m '21.5' -q 1

# Retained message — delivered to anyone who subscribes later
mosquitto_pub -h 127.0.0.1 -p 1883 -V 5 -t 'home/kitchen/temp' -m '21.5' -q 1 -r
```

## Configuration

The broker takes exactly one argument: the path to a `.toml` file. Every field is optional and falls back
to a documented default; unknown keys are rejected to catch typos.

- [`rusquitto.config.toml`](rusquitto.config.toml) — the full reference: every property, its default, and a
  one-line description. Copy it and edit what you need.

The schema has four sections:

| Section     | Controls                                                                    |
|-------------|-----------------------------------------------------------------------------|
| `[server]`  | `bind`, `port`, `listen_backlog`                                            |
| `[runtime]` | `cores` (CPU cores / shard count), CPU `placement`, `mesh_capacity`          |
| `[logging]` | `level`, `dir`, log/error file names, `enable_terminal`, `format`           |
| `[limits]`  | `max_connections_per_shard`, `max_payload_size`, `max_qos`, `keep_alive`, … |

`RUST_LOG` overrides `logging.level` at startup.

## Architecture

```text
   MQTT clients ──TCP──►  Linux kernel  ──SO_REUSEPORT hash──►  per-shard sockets
                                                               │      │      │
                                                          Core 0  Core 1  Core 2   (pinned)
                                                          io_uring ring · LocalExecutor
                                                          shard-local state (no locks)
                                                               └──────┴──────┘
                                                          inter-shard channel mesh
```

Each core owns its connections, subscription trie, client registry, and retain table exclusively. When a
publish needs to reach a subscriber on another core, it crosses the lock-free channel mesh rather than
sharing memory. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design, and
[`docs/MQTT_IMPLEMENTATION.md`](docs/MQTT_IMPLEMENTATION.md) / [`docs/MQTT_PACKETS.md`](docs/MQTT_PACKETS.md)
for the protocol details.

## Limitations

Deliberately out of scope for now (tracked in `.agents/progress.md`):

- **Under heavy connection bursts** you may hit a `glommio` io_uring `ENOMEM` from a low `RLIMIT_MEMLOCK`; raise it
  (`ulimit -l unlimited` or `LimitMEMLOCK=infinity`). *(Cross-shard QoS 1/2 publishes and Wills are now reliable —
  the mesh forward applies backpressure instead of dropping. Only broker-internal `$SYS` metric publishes still use
  best-effort sends, which is fine since they are QoS 0 and retained.)*
- **Cross-shard session migration is best-effort under mesh overload.** A reconnecting client that lands on a
  different shard triggers a session `Claim` over the channel mesh; the owning shard hands the session back. The
  mesh uses non-blocking sends (drop-on-full), so under a saturated mesh a claim or hand-off could be dropped and
  the reconnect would fall back to a fresh session (the stranded one then expires on its old shard). In normal
  operation this is seamless (verified across a 2-shard broker); it shares the backpressure limitation above.
  A cross-shard *takeover* of a still-live connection drops the old connection without migrating its in-flight
  state.
- **Shared-subscription load balancing is per-shard.** Each shard picks one of *its* local group members for a
  matching message, so if a group's members are spread across shards (via `SO_REUSEPORT`), the message reaches
  one member per shard rather than exactly one across the cluster. Fully single-delivery for `runtime.shards = 1`
  or when a group's members share a shard; globally-coordinated shared delivery is future work (overlaps the
  cross-shard items above).
- **No outbound topic aliases.** The broker accepts *inbound* topic aliases but never assigns aliases on the
  publishes it sends (CONNACK advertises none outbound). Delayed wills are forwarded to peer shards best-effort
  (the sweep timer uses a non-blocking mesh send).
- **Passwords: plaintext or SHA-256.** A `password_hash` avoids storing the secret in the clear, but there is no
  salting or a slow KDF (Argon2/bcrypt) yet, and no enhanced (SASL-style) authentication. Anonymous clients
  bypass ACL (they are unrestricted). Protect the config file with restrictive permissions regardless.

## Releases

Version history is in [CHANGELOG.md](CHANGELOG.md); each release is tagged and published on
[GitHub Releases](https://github.com/iamaliybi/rusquitto/releases). The project follows semantic
versioning (pre-1.0: minor for features, patch for fixes).

## License

MIT — see [LICENSE](LICENSE).
