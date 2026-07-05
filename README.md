# rusquitto

A high-performance **MQTT 5.0 broker** written in Rust on a strict **thread-per-core** architecture,
powered by [`glommio`](https://github.com/DataDog/glommio) and Linux `io_uring`. Every CPU core runs an
isolated, shard-local executor — no `Mutex`, no `RwLock`, no work-stealing.

> **Status: 1.0.** A production MQTT 5 broker over TCP and WebSocket, with a full
> QoS 0/1/2 pipeline, persistent and cross-shard sessions, authentication/ACL, and a
> hardened connection state machine. See [Limitations](#limitations) for the edges
> that remain out of scope.

## Features

- **MQTT 5.0** over **TCP (`:1883`) and WebSocket (`:1884`)**: CONNECT/CONNACK, PUBLISH, SUBSCRIBE/SUBACK,
  UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP, DISCONNECT. The WebSocket transport (RFC 6455, `mqtt` subprotocol,
  binary frames) lets browser clients connect without a TCP bridge.
- **TLS termination** (rustls) for **`mqtts://` (`:8883`) and `wss://` (`:8884`)** — opt-in via `[tls]`. Only
  **TLS 1.3 and 1.2** are offered, with strong **AEAD + ECDHE** cipher suites (forward secrecy; no CBC/RC4/3DES);
  older protocols and weak ciphers are structurally impossible. Layered behind the same `ByteStream` seam, so the
  MQTT engine is reused unchanged over any of TCP / WS / TLS / WS-over-TLS.
- **QoS 0/1/2**, both inbound (receiver-side) and outbound (sender-side) — full PUBACK and PUBREC→PUBREL→PUBCOMP
  handshakes, with exactly-once delivery for QoS 2.
- **Topic wildcards** `+` (single level) and `#` (multi level) via a topic trie; `$`-topics are excluded from wildcard
  matches.
- **Retained messages** — stored, replayed to new subscribers, cleared by empty payload, replicated across shards.
  Optionally **persisted to disk** (`[persistence]`): the retained set is snapshotted (atomic write, `fdatasync`,
  via glommio's io_uring file I/O) and restored on startup, so "last known value" topics survive a restart.
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
- **Resource guards** — bounded read buffers and outbound mailboxes, per-shard and per-IP connection caps, payload
  and subscription/retained caps, plus an optional **per-connection PUBLISH rate limit** (`limits.max_message_rate`).
  The rate limit *throttles* (paces the client to its budget, applying backpressure) rather than dropping, which
  bounds how much CPU a single noisy publisher can draw on its pinned core — relevant because a connection is served
  entirely by the one shard that accepted it.
- **Overload handling** (`[overload]`) — because a connection is pinned to one shard and there is no work-stealing,
  each shard tracks its **reactor scheduling delay** (saturation, exposed at `$SYS/broker/load/max-scheduling-delay-ms`)
  and acts on it: a **stall WARN**, optional **admission control** (reject new connections while overloaded so the
  client's retry may land on a cooler core), and optional **load shedding** (close a batch of connections so they
  reconnect and `SO_REUSEPORT` rehashes them elsewhere — the thread-per-core way to rebalance, by moving the
  *connection* since the compute can't move). Background housekeeping (`$SYS`, session sweep, shedding) runs in a
  low-priority glommio task queue so it yields to client-serving work under load.
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
- **Hardened connection handling** — the first packet on a connection must be CONNECT (and only one is
  allowed), so nothing is published or subscribed before authentication; a socket that never sends CONNECT is
  dropped after `connect_timeout`; an idle connection is dropped at 1.5× the negotiated keep-alive. Client
  PUBLISHes to `$`-prefixed topics (e.g. spoofing `$SYS`) and to wildcard/empty/NUL topics are rejected, as
  are malformed SUBSCRIBE filters. Credential checks are constant-time and run a throwaway hash for unknown
  users so timing doesn't reveal which usernames exist; server-assigned client ids are unguessable. Resource
  caps bound session expiry, per-client subscriptions, per-shard retained topics, and per-connection buffering.
- **Memory-conscious topic index** — trie levels are keyed by interned `Rc<str>` segments, so a name that
  recurs across many filters (e.g. `a` in `a/b`, `a/c`, `x/a`) is stored once.
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

The schema has five sections:

| Section     | Controls                                                                                     |
|-------------|----------------------------------------------------------------------------------------------|
| `[server]`  | `bind`, `port`, `websocket` / `websocket_port`, `listen_backlog`                             |
| `[runtime]` | `cores` (CPU cores / shard count), CPU `placement`, `mesh_capacity`                           |
| `[logging]` | `level`, `dir`, log/error file names, `enable_terminal`, `format`                            |
| `[limits]`  | connection/packet sizing, QoS, keep-alive, plus the security caps (`connect_timeout`, `max_session_expiry`, `max_subscriptions_per_client`, `max_retained_messages`, `max_message_rate`) |
| `[auth]`    | `allow_anonymous` and `[[auth.users]]` (credentials + `publish`/`subscribe` ACLs)            |

`RUST_LOG` overrides `logging.level` at startup.

### Connecting over WebSocket

With `[server] websocket = true` (the default), browser and Node clients can connect on `:1884`:

```js
// mqtt.js
const client = mqtt.connect("ws://127.0.0.1:1884/mqtt");
```

### TLS (`mqtts` / `wss`)

Set `[tls] enabled = true` and point `cert_file` / `key_file` at a PEM certificate chain (leaf first) and its
private key (PKCS#8, PKCS#1, or SEC1). A native TLS listener then runs on `:8883`, and — with `[tls] websocket =
true` — a WebSocket-over-TLS listener on `:8884`:

```toml
[tls]
enabled = true
cert_file = "certs/server.pem"
key_file  = "certs/server.key"
```

```sh
# native MQTT over TLS
mosquitto_pub -V mqttv5 -h host -p 8883 --cafile ca.pem -t demo -m hi
# browser client
mqtt.connect("wss://host:8884/mqtt")
```

Only TLS 1.3 and 1.2 are accepted, with AEAD + ECDHE cipher suites only. There is no client-certificate (mTLS)
authentication — clients authenticate at the MQTT layer over the encrypted link. TLS terminates in-process, so a
reverse proxy is optional rather than required.

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

Known edges, deliberately out of scope for 1.0 (tracked in `.agents/progress.md`):

- **Under heavy connection bursts** you may hit a `glommio` io_uring `ENOMEM` from a low `RLIMIT_MEMLOCK`; raise it
  (`ulimit -l unlimited` or `LimitMEMLOCK=infinity`). *(Cross-shard QoS 1/2 publishes and Wills are reliable —
  the mesh forward applies backpressure instead of dropping. Only broker-internal `$SYS` metric publishes still use
  best-effort sends, which is fine since they are QoS 0 and retained.)*
- **No client-certificate (mTLS) authentication.** TLS termination for `mqtts://` / `wss://` is built in (rustls,
  TLS 1.3/1.2, strong AEAD suites), but the server does not request or verify client certificates — clients
  authenticate at the MQTT layer (username/password) over the encrypted link. Certificate rotation requires a
  restart (the cert is loaded once at startup).
- **Cross-shard session migration is best-effort under mesh overload.** A reconnecting client that lands on a
  different shard triggers a session `Claim` over the channel mesh; the owning shard hands the session back. The
  mesh uses non-blocking sends (drop-on-full), so under a saturated mesh a claim or hand-off could be dropped and
  the reconnect would fall back to a fresh session (the stranded one then expires on its old shard). In normal
  operation this is seamless (verified across a 2-shard broker); it shares the backpressure limitation above.
  A cross-shard *takeover* of a still-live connection drops the old connection without migrating its in-flight
  state.
- **Shared-subscription load balancing is per-shard.** Each shard picks one of *its* local group members for a
  matching message, so if a group's members are spread across shards (via `SO_REUSEPORT`), the message reaches
  one member per shard rather than exactly one across the cluster. Fully single-delivery for `runtime.cores = 1`
  or when a group's members share a shard; globally-coordinated shared delivery is future work (overlaps the
  cross-shard items above).
- **No outbound topic aliases.** The broker accepts *inbound* topic aliases but never assigns aliases on the
  publishes it sends (CONNACK advertises none outbound). Delayed wills are forwarded to peer shards best-effort
  (the sweep timer uses a non-blocking mesh send).
- **Passwords: plaintext or SHA-256.** A `password_hash` avoids storing the secret in the clear, but there is no
  salting or a slow KDF (Argon2/bcrypt) yet, and no enhanced (SASL-style) authentication. Anonymous clients
  bypass ACL (they are unrestricted). Protect the config file with restrictive permissions regardless.
- **Persistence is snapshot-based, not a write-ahead log.** With `[persistence] enabled`, both the retained set and
  suspended sessions (subscriptions, in-flight QoS 1/2 state, and offline queue) are snapshotted to disk and restored
  on startup — a graceful restart preserves them fully, and a crash preserves them up to the last snapshot
  (`snapshot_interval`), so updates in the final window are lost. Restored sessions come back **suspended**: a
  reconnecting client resumes one directly if it lands on the holding shard, or the cross-shard `Claim`/`Handoff`
  migrates it to wherever `SO_REUSEPORT` places the client — inheriting the same best-effort-under-mesh-overload caveat
  as live cross-shard resume. Sessions are shard-local (one file per shard); if `runtime.cores` shrinks between runs,
  peer 0 loads the sessions orphaned on now-absent shards so none are lost.

## Development

```sh
cargo build            # debug build
cargo test             # unit tests (broker routing, connection state machine, config, auth, topics…)
cargo clippy --all-targets -- -D warnings
cargo fmt --all        # format to the repo's rustfmt.toml
```

The connection state machine is unit-tested over an in-memory `ByteStream` mock, so the full MQTT handshake
and packet handling run without sockets (see `src/server/connection/tests.rs`).

Enable the shared git hook so formatting, lint, and the test suite run before every commit:

```sh
./.githooks/install.sh     # sets core.hooksPath to .githooks
```

The hook only runs when Rust sources are staged; bypass it in an emergency with `git commit --no-verify`.

## Releases

Version history is in [CHANGELOG.md](CHANGELOG.md); each release is tagged and published on
[GitHub Releases](https://github.com/iamaliybi/rusquitto/releases). The project follows
[semantic versioning](https://semver.org): from 1.0 on, the major version bumps for breaking changes, the
minor for features, and the patch for fixes.

## License

MIT — see [LICENSE](LICENSE).
