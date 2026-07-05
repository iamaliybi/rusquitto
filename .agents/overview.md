# Rusquitto — Project Overview

**Type:** MQTT 5.0 Broker
**Language:** Rust (Edition 2024)
**Architecture:** Thread-per-core, Shared-Nothing
**Runtime:** Glommio (io_uring, Linux 5.8+)
**Author:** Ali Yaghoubi
**Status:** Functional broker — sessions, will, negotiation, auth+ACL, `$SYS`, shutdown, subscription options — Phase 3a–3i.
**Last updated:** 2026-07-03

See [progress.md](progress.md) for the detailed implementation log, decisions, and gotchas.

---

## What Works Now

| Feature                                      | Status                             |
|----------------------------------------------|------------------------------------|
| TCP socket creation (SO_REUSEPORT)           | ✅                                  |
| Per-core async accept loop                   | ✅                                  |
| MQTT 5.0 packet parsing (fragmentation-safe) | ✅                                  |
| CONNECT / CONNACK                            | ✅ (advertises server keep-alive)   |
| PINGREQ / PINGRESP                           | ✅                                  |
| DISCONNECT                                   | ✅                                  |
| PUBLISH routing (local + cross-shard)        | ✅                                  |
| SUBSCRIBE / SUBACK                           | ✅                                  |
| UNSUBSCRIBE / UNSUBACK                       | ✅                                  |
| QoS 1 (PUBACK), in + out                     | ✅                                  |
| QoS 2 (PUBREC/PUBREL/PUBCOMP), in + out      | ✅                                  |
| Topic trie (`+` / `#` wildcards)             | ✅ `src/broker/topic_trie.rs`       |
| Client registry                              | ✅ shard-local `ShardState`         |
| Retain table                                 | ✅ replicated across shards         |
| Inter-shard channels                         | ✅ glommio `channel_mesh`           |
| Structured logging (tracing)                 | ✅ `src/logger.rs`                  |
| CLI + TOML config                            | ✅ `src/config.rs`                  |
| Session persistence / expiry                 | ✅ suspend/resume + expiry sweep    |
| Disk persistence (retained + sessions)       | ✅ `[persistence]`, `src/persistence/` — snapshot on interval + shutdown |
| Offline message queueing (QoS > 0)           | ✅ buffered while suspended         |
| In-flight retransmission (DUP) on resume     | ✅ QoS 1/2                          |
| Session takeover (Client ID reuse)           | ✅ generation-guarded              |
| Will messages                                | ✅ fire on abnormal, suppress clean |
| CONNECT/CONNACK capability negotiation       | ✅ advertises server caps           |
| Flow control (Receive Maximum, outbound)     | ✅ windowed in-flight + pending     |
| Maximum Packet Size (outbound)               | ✅ oversized dropped                |
| Authentication (username/password)           | ✅ `src/auth.rs`, `[auth]` config   |
| Topic ACL (per-user publish/subscribe)       | ✅ allow-lists in `[[auth.users]]`  |
| Graceful shutdown (SIGTERM/SIGINT)           | ✅ drains conns (DISCONNECT), flushes logs |
| `$SYS` metrics topics                        | ✅ `src/metrics.rs`, `[sys]` config |
| Subscription options (No Local, RAP, RH)     | ✅ enforced in trie + routing       |
| Cross-shard QoS > 0 guarantees               | ⚠️ best-effort (drop-on-full mesh) |
| Cross-shard session resume                   | ⚠️ shard-local only (see below)    |
| Will Delay Interval                          | ⚠️ treated as 0 (immediate)        |
| Hashed passwords                             | ❌ planned (plaintext for now)      |

---

## Build & Run

The broker requires a config file path as a **positional** argument:

```bash
cargo build --release
cargo run --release rusquitto.config.toml   # NOT `--config` (Cargo intercepts that flag)
```

- `rusquitto.config.toml` — the single reference config (every property + default, one-line comments).
- Silent terminal by default (`logging.enable_terminal = false`); logs go to `logs/`.
- The separate stress-test binary: `cargo run --bin mosquitto` → runs `scripts/mosquitto.sh` (needs mosquitto-clients).

Broker binds `127.0.0.1:1883` by default.

---

## Known limitation: cross-shard session resume

Sessions live in shard-local `ShardState`, keyed by client id. `SO_REUSEPORT` assigns a connection to a
shard by hashing its TCP 4-tuple, so a reconnecting client — which uses a **new ephemeral port** — may land
on a *different* shard, where its suspended session does not exist (it is treated as a fresh session there).
Resume is therefore reliable only when the client rehashes to the same shard. A robust fix needs a
cross-shard session directory or a redirect (MQTT 5 Server Reference), tracked in
[next-steps.md](next-steps.md). For single-shard deployments (`runtime.cores = 1`) resume is always exact.
