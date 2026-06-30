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
- **Cross-shard routing** over a `glommio` channel mesh, so a publisher and subscriber on different cores still reach
  each other.
- **Thread-per-core, shared-nothing**: `SO_REUSEPORT` kernel load balancing, one `io_uring` ring and one `LocalExecutor`
  per shard, lock-free shard-local state.
- **Structured logging** (`tracing`): non-blocking file appenders, daily rotation, a dedicated error log, per-connection
  spans tagged with `client_id`, and redaction of passwords and payloads.
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
- **No persistent sessions or retransmission** — sessions are treated as clean; in-flight QoS state is
  dropped on disconnect.
- **No authentication / ACL**, **no will messages**, and **no CONNECT capability negotiation** beyond
  advertising the server keep-alive.

## License

MIT — see [LICENSE](LICENSE).
