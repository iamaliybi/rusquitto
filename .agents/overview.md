# Rusquitto — Project Overview

**Type:** MQTT 5.0 Broker  
**Language:** Rust (Edition 2024)  
**Architecture:** Thread-per-core, Shared-Nothing  
**Runtime:** Glommio (io_uring, Linux 5.8+)  
**Author:** Ali Yaghoubi  
**Status:** Phase 1 complete; Phase 2 (Pub/Sub) not started  
**Date Analysed:** 2026-06-29

---

## What Works Now

| Feature                                      | Status              |
|----------------------------------------------|---------------------|
| TCP socket creation (SO_REUSEPORT)           | ✅                   |
| Per-core async accept loop                   | ✅                   |
| MQTT 5.0 packet parsing (fragmentation-safe) | ✅                   |
| CONNECT / CONNACK handshake                  | ✅ (no negotiation)  |
| PINGREQ / PINGRESP                           | ✅                   |
| DISCONNECT                                   | ✅ (no will message) |
| PUBLISH routing                              | ❌ unimplemented!()  |
| SUBSCRIBE / SUBACK                           | ❌ unimplemented!()  |
| UNSUBSCRIBE / UNSUBACK                       | ❌ unimplemented!()  |
| QoS 1 (PUBACK)                               | ❌ unimplemented!()  |
| QoS 2 (PUBREC/PUBREL/PUBCOMP)                | ❌ unimplemented!()  |
| Topic Trie                                   | ❌ not started       |
| Client registry                              | ❌ not started       |
| Retain table                                 | ❌ not started       |
| Inter-shard channels                         | ❌ not started       |
| Auth / ACL                                   | ❌ not started       |
| Session persistence                          | ❌ not started       |

---

## Build & Run

```bash
cargo build --release          # build broker
cargo run --release            # runs mosquitto.rs → scripts/mosquitto.sh
bash scripts/mosquitto.sh      # stress test (needs mosquitto-clients)
```

Broker listens on `127.0.0.1:1883`.
