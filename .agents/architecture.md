# Rusquitto — Architecture Notes

## Thread-per-Core Design

Each online CPU core gets exactly one OS thread pinned to it. Glommio's `LocalExecutor` runs on that thread; all tasks
spawned with `spawn_local()` stay on that core forever. No work-stealing, no cross-core data access → zero locks needed.

```
CPU Core N  ←→  OS Thread N  ←→  Glommio LocalExecutor N
                                  ├── TcpListener (own socket)
                                  ├── Connection task A
                                  ├── Connection task B
                                  └── ...
```

**CPU allocation:** 75% of detected cores given to Glommio (`(total * 3) / 4`).  
**Placement:** `PoolPlacement::MaxSpread` — spread across physical cores (NUMA-aware).

---

## Network Ingress: SO_REUSEPORT

All shards bind the *same* address/port (`127.0.0.1:1883`). The kernel hashes the TCP 4-tuple and distributes SYN
packets across the listening sockets — fair, hardware-level load balancing with no central dispatcher.

Key socket options set in `src/net/socket.rs`:

```rust
socket.set_reuse_address(true)
socket.set_reuse_port(true)     // multiple binds to same addr:port
socket.set_nonblocking(true)
socket.listen(4096)
```

---

## I/O: Linux io_uring (via Glommio)

- Completion-based (not readiness-based like epoll)
- Batch syscalls: submit many I/O ops in one `io_uring_enter`
- Per-shard ring buffers → zero cross-core kernel synchronisation
- Requires Linux ≥ 5.8

---

## Connection Lifecycle

```
accept() → spawn_local(Connection::new(stream, shard_id))
              ↓
           Connection::run()   (async loop)
              ↓
           read_packet()        assemble bytes → mqttbytes::v5::read()
              ↓
           process_packet()     dispatch on Packet variant
              ↓
           handle_*()           per-type handler
```

### Packet Buffer Strategy

- `temp_buf: [u8; 2048]` — stack-allocated, reused each read (hot path)
- `self.buffer: BytesMut` — heap-allocated, grows as needed across reads
- Handles both fragmentation (small reads) and coalescing (multiple packets per read)

---

## Inter-Shard Communication (NOT YET IMPLEMENTED)

Designed in docs, wired nowhere:

- SPSC lock-free queues (crossbeam) — one per shard pair
- Needed for: publish routing, client registry lookup, retain distribution
- Future: extend same channel design to multi-machine clustering

---

## Key Files

| File                       | Role                                         |
|----------------------------|----------------------------------------------|
| `src/main.rs`              | Entry: CPU detection, executor pool launch   |
| `src/server/worker.rs`     | Per-shard init: socket bind, accept loop     |
| `src/server/connection.rs` | MQTT protocol FSM, all packet handlers       |
| `src/net/socket.rs`        | Low-level socket creation with SO_REUSEPORT  |
| `src/net/tcp_listener.rs`  | Glommio TcpListener wrapper                  |
| `src/broker/engine.rs`     | Empty stub (broker state goes here)          |
| `src/bin/mosquitto.rs`     | Binary: runs `scripts/mosquitto.sh` via bash |
