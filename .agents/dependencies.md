# Rusquitto — Dependencies

## Direct Dependencies (Cargo.toml)

| Crate                | Version | Purpose                                                                                                                             |
|----------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------|
| `glommio`            | 0.9.0   | Thread-per-core async runtime; io_uring; CPU affinity; `LocalExecutor`, `TcpStream`, `TcpListener`, `channel_mesh`, `local_channel` |
| `socket2`            | 0.6.2   | Low-level socket control: `SO_REUSEPORT`, `SO_REUSEADDR`, non-blocking, listen backlog                                              |
| `bytes`              | 1.11    | `BytesMut` growable buffer for assembling fragmented MQTT packets                                                                   |
| `mqttbytes`          | 0.6     | MQTT 5.0 encode/decode; `mqtt_v5::read()` parses raw bytes into `Packet` enum                                                       |
| `futures-lite`       | 2.6     | `AsyncReadExt` / `AsyncWriteExt` traits + `FutureExt::or` (select loop) on Glommio `TcpStream`                                      |
| `tracing`            | 0.1     | Structured logging facade; per-connection spans, events                                                                             |
| `tracing-subscriber` | 0.3     | Subscriber/layers (`env-filter`, `json`, `fmt`); EnvFilter + reload                                                                 |
| `tracing-appender`   | 0.2     | Non-blocking, daily-rotating file appenders                                                                                         |
| `clap`               | 4       | CLI (derive API); single positional `<CONFIG>` arg                                                                                  |
| `serde`              | 1       | `Deserialize` for the config tree (derive)                                                                                          |
| `toml`               | 1.1     | Decodes the `.toml` config file                                                                                                     |

## Key Types by Source

| Type                               | Crate         | Used In                                      |
|------------------------------------|---------------|----------------------------------------------|
| `LocalExecutorPoolBuilder`         | glommio       | `src/main.rs` — pool init                    |
| `PoolPlacement::MaxSpread`         | glommio       | `src/main.rs` — CPU spread                   |
| `CpuSet`                           | glommio       | `src/main.rs` — core detection               |
| `TcpStream`                        | glommio::net  | `src/server/connection.rs`                   |
| `TcpListener`                      | glommio::net  | `src/net/tcp_listener.rs`                    |
| `Socket`                           | socket2       | `src/net/socket.rs`                          |
| `BytesMut`                         | bytes         | `src/server/connection.rs` — packet assembly |
| `Packet` (enum, 14 variants)       | mqttbytes::v5 | `src/server/connection.rs` — dispatch        |
| `Connect`, `ConnAck`, `Publish`, … | mqttbytes::v5 | individual handlers                          |

## Notable Transitive Deps

- `crossbeam-*` — lock-free data structures (used internally by glommio channels)
- `signal-hook` — Unix signal handling
- `libc` / `nix` — syscall bindings needed by Glommio

## Inter-shard channels: resolved

Originally planned as raw `crossbeam` SPSC queues. **Implemented instead via glommio's own
`channels::channel_mesh`** (a full mesh of shared channels) — it integrates with the glommio executor and
avoids hand-rolling cross-executor synchronization. `local_channel` is used for per-connection mailboxes.
