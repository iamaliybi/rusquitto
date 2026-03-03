# High-Performance MQTT 5.0 Broker

A low-level, high-performance MQTT 5.0 broker implemented in Rust. This project abandons heavy asynchronous frameworks
in favor of a strict **thread-per-core** architecture powered by `glommio` and Linux `io_uring`.

## Current State: Phase 1 (Core Network & Parsing)

This project is currently in active development. The present codebase implements the foundational TCP and parsing
layers. It is **not** yet a fully functional Pub/Sub router.

**Implemented:**

* Raw TCP listener utilizing `io_uring` for zero-overhead I/O.
* Strict thread-per-core task execution (Shared-Nothing architecture).
* Efficient nested-loop packet parsing handling TCP stream fragmentation.
* Partial MQTT 5.0 Control Packet decoding (CONNECT, PINGREQ, DISCONNECT).

**Pending:**

* Global Broker State Management (Client Registry).
* Topic routing trie and wildcard matching.
* Inter-shard communication via MPSC channels.
* QoS 1 and QoS 2 state flows.

## Architecture

This broker is designed around hardware sympathy. It avoids cross-thread synchronization locks (`Mutex`, `RwLock`)
entirely for connection handling.

1. **Worker Shards:** Each CPU core runs an isolated executor (`LocalExecutor`).
2. **Memory Management:** Stack-allocated temporary I/O buffers (`temp_buf`) act as fast transport from the kernel,
   feeding into heap-allocated assembly buffers (`BytesMut`) only when necessary to construct complete MQTT frames.
3. **Protocol Parsing:** Relies on `mqttbytes` for strict MQTT 5.0 specification compliance and DoS protection (e.g.,
   maximum packet size enforcement).

## Running Locally

Ensure you are on a Linux machine with a kernel supporting `io_uring` (Kernel 5.8+).

```bash
cargo build --release
cargo run --release
```