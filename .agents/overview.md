# Rusquitto — Project Overview

**Type:** MQTT 5.0 Broker
**Language:** Rust (Edition 2024)
**Architecture:** Thread-per-core, Shared-Nothing
**Runtime:** Glommio (io_uring, Linux 5.8+)
**Author:** Ali Yaghoubi
**Status:** Functional pub/sub broker — Phase 2 complete. Phase 3 (hardening) pending.
**Last updated:** 2026-06-30

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
| Cross-shard QoS > 0 guarantees               | ⚠️ best-effort (drop-on-full mesh) |
| Auth / ACL                                   | ❌ planned                          |
| Will messages                                | ❌ planned                          |
| Session persistence / expiry                 | ❌ planned (clean sessions only)    |
| Flow control (receive maximum)               | ❌ planned                          |

---

## Build & Run

The broker requires a config file path as a **positional** argument:

```bash
cargo build --release
cargo run --release rusquitto.default.toml   # NOT `--config` (Cargo intercepts that flag)
```

- `rusquitto.toml` — practical example config; `rusquitto.default.toml` — full reference.
- Silent terminal by default (`logging.enable_terminal = false`); logs go to `logs/`.
- The separate stress-test binary: `cargo run --bin mosquitto` → runs `scripts/mosquitto.sh` (needs mosquitto-clients).

Broker binds `127.0.0.1:1883` by default.
