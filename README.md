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
  messages are retransmitted with the DUP flag. Session takeover on Client ID reuse is handled. *(Shard-local — see
  [Limitations](#limitations).)*
- **Will messages** — the CONNECT Will is published when a client drops abnormally (EOF, network error, or a
  DISCONNECT that requests it) and suppressed on a normal DISCONNECT; a session takeover never fires the
  displaced connection's will. *(Will Delay Interval is treated as immediate — see [Limitations](#limitations).)*
- **CONNECT capability negotiation** — CONNACK advertises the server's Receive Maximum, Maximum Packet Size,
  Maximum QoS, Retain Available, and wildcard/shared/subscription-identifier availability. The client's
  **Receive Maximum** (a windowed outbound in-flight limit, with held messages drained as acks arrive) and
  **Maximum Packet Size** (oversized outbound publishes are dropped) are enforced.
- **Authentication & ACL** — optional username/password at CONNECT via the `[auth]` config (`allow_anonymous`
  plus a list of users). Failures are rejected with the proper CONNACK reason code (`0x86` bad credentials,
  `0x87` anonymous not allowed). Each user may carry `publish` / `subscribe` topic-filter allow-lists: a denied
  publish is dropped (QoS 0) or Not-Authorized-acked (QoS 1/2), and a denied subscribe gets a Not Authorized
  reason code. Defaults are open, so nothing is required until you configure it.
- **Cross-shard routing** over a `glommio` channel mesh, so a publisher and subscriber on different cores still reach
  each other.
- **Thread-per-core, shared-nothing**: `SO_REUSEPORT` kernel load balancing, one `io_uring` ring and one `LocalExecutor`
  per shard, lock-free shard-local state.
- **Structured logging** (`tracing`): non-blocking file appenders, daily rotation, a dedicated error log, per-connection
  spans tagged with `client_id`, and redaction of passwords and payloads.
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
- For the example clients below: `mosquitto-clients` (`mosquitto_pub` / `mosquitto_sub`).

## Quick start

```bash
# Build
cargo build --release

# Run with a config file (the path is a positional argument)
cargo run --release rusquitto.default.toml
```

> **Note:** the config path is positional, so `cargo run rusquitto.default.toml` works directly.
> Don't write `cargo run --config ...` — `--config` is a *Cargo* flag and Cargo will intercept it.
> Run the built binary the same way: `./rusquitto rusquitto.default.toml`.

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

- [`rusquitto.toml`](rusquitto.toml) — a practical example.
- [`rusquitto.default.toml`](rusquitto.default.toml) — the full reference: every property with its type and default.

The schema has four sections:

| Section     | Controls                                                                    |
|-------------|-----------------------------------------------------------------------------|
| `[server]`  | `bind`, `port`, `listen_backlog`                                            |
| `[runtime]` | shard count (`shards` / `cpu_fraction`), CPU `placement`, `mesh_capacity`   |
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

- **Cross-shard QoS > 0 is best-effort.** The mesh uses non-blocking sends (drop-on-full), so the
  at-least/exactly-once guarantee holds *within* a shard but is best-effort *across* shards. Under heavy
  connection bursts you may also hit a `glommio` io_uring `ENOMEM` from a low `RLIMIT_MEMLOCK`; raise it
  (`ulimit -l unlimited` or `LimitMEMLOCK=infinity`).
- **Session resume is shard-local.** Sessions are stored per shard, keyed by Client ID. `SO_REUSEPORT`
  hashes each connection's TCP 4-tuple to a shard, and a reconnecting client uses a new ephemeral port, so
  it may land on a *different* shard where its suspended session doesn't exist (and is treated as fresh).
  Resume is reliable when the client rehashes to the same shard, and **always exact for
  `runtime.shards = 1`**. A cross-shard session directory / MQTT 5 Server Reference redirect is future work.
- **Will Delay Interval is not yet honoured** — a will fires immediately on abnormal disconnect rather than
  after the requested delay. (Will messages themselves work; only the *delay* is unimplemented.)
- **Negotiation is outbound-only.** The client's Receive Maximum and Maximum Packet Size are enforced, but the
  server does not yet enforce an *inbound* Receive Maximum quota, and Topic Aliases are unsupported (CONNACK
  advertises a Topic Alias Maximum of 0).
- **Passwords are plaintext** in the config file (protect it with permissions); there is no hashed-password
  support or enhanced (SASL-style) authentication yet. Anonymous clients bypass ACL (they are unrestricted).

## License

MIT — see [LICENSE](LICENSE).
