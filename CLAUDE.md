# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`rusquitto` is a high-performance **MQTT 5.0 broker** written in Rust (edition 2024) on a strict
**thread-per-core, shared-nothing** architecture, powered by [`glommio`](https://github.com/DataDog/glommio)
and Linux `io_uring`. Every online CPU core runs one pinned OS thread with its own `LocalExecutor`, socket,
and shard-local state. There are **no `Mutex`/`RwLock` and no work-stealing** — this is a hard invariant, not
a style preference. Cross-core communication happens only over a glommio `channel_mesh`.

## Environment & build

This repo lives on a Linux filesystem accessed from Windows over WSL. `glommio`/`io_uring` **only build and
run on Linux**, so all cargo commands must run *inside* WSL, not on the Windows host:

```sh
wsl -d ubuntu -- bash -lc "cd /var/www/rust-ubuntu/rusquitto && cargo build"
```

`.cargo/config.toml` pins the target to `x86_64-unknown-linux-gnu`, so debug binaries land at
`target/x86_64-unknown-linux-gnu/debug/rusquitto` (not the usual `target/debug/`). Cargo aliases are defined
there too: `cargo f` (fmt), `cargo t` (test), `cargo r` / `cargo rr` (run / run --release).

Common commands:

```sh
cargo build                                   # debug build
cargo test                                    # full unit-test suite
cargo test --package rusquitto <name>         # run a single test / module (e.g. `connection::tests`)
cargo clippy --all-targets -- -D warnings     # lint; the pre-commit hook treats warnings as errors
cargo fmt --all                               # format to rustfmt.toml (hard tabs, width 120)
```

Running the broker takes the config path as a **positional** argument — never `--config` (Cargo intercepts
that flag as its own):

```sh
cargo run --release rusquitto.config.toml     # correct
cargo run --config rusquitto.config.toml      # WRONG — Cargo eats --config
```

The broker is **silent in the terminal by default** (logs go to `logs/`); set `[logging] enable_terminal =
true` or `tail -f logs/rusquitto.log` to watch it.

### WSL gotchas (learned the hard way)

- **Test with a single shard** (`[runtime] cores = 1`). The default multi-shard config hits a glommio
  io_uring `ENOMEM` because `RLIMIT_MEMLOCK` (`ulimit -l`) is low in the non-interactive WSL shell and
  `ulimit -l unlimited` does not take effect there.
- The double shell (Windows → WSL bash) mangles `$VAR`, command substitution, and heredocs. For anything
  non-trivial, **write a script file** with the Write tool, then `sed -i 's/\r$//' file && bash file`
  (strip CRLF first, or bash chokes on the carriage returns).
- To stop a running broker use `pkill -x rusquitto` — a bare `pkill -f debug/rusquitto` matches its own
  command line and self-kills.

The pre-commit hook (`.githooks/pre-commit`, enabled via `./.githooks/install.sh` or `git config
core.hooksPath .githooks`) runs fmt-check + clippy + test, but **only when `.rs` files are staged**. Bypass
with `git commit --no-verify` only in emergencies.

## Architecture

The crate is layered; each layer depends only on those beneath it (see the module docs in `src/lib.rs`):

```
config → protocol → transport → auth → broker → server → telemetry
```

`lib.rs::run()` is the composition root: it inits logging, builds one shared read-only `tls::ServerConfig`
and one `channel_mesh`, then `LocalExecutorPoolBuilder::on_all_shards` spawns `server::shard::run_shard` per core.

Two things named "shard", on purpose: **`server::shard`** is the shard's *runtime* (bind listeners, accept
loop, background tasks, drain), **`broker::shard::ShardState`** is the shard's *data* (sessions, trie,
retain). Modules are **file-based** (`foo.rs` beside `foo/`), not `mod.rs`.

**Key seams to understand before making changes:**

- **`transport::ByteStream`** (`src/transport.rs`) is a dependency-inversion seam: an async bidirectional
  byte stream. TCP satisfies it directly; WebSocket wraps a TCP stream in an RFC 6455 frame codec; TLS
  (rustls) wraps *either*. `Connection` is written **once** against `ByteStream`, so the entire MQTT engine
  is reused unchanged across TCP / WS / TLS / WS-over-TLS (`wss://`). Add a transport by implementing this
  trait — do not special-case transports inside the connection code.

- **`server::shard`** (`src/server/shard.rs` + `src/server/shard/`) is the per-shard runtime. `shard.rs`'s
  `run_shard` orchestrates; the concerns are split: `accept.rs` (accept loop, `ConnCounts`/`ConnSlot`
  accounting, admission control, `Listeners`), `serve.rs` (transport-stack dispatch + the `boxed_*` seams),
  `maintenance.rs` (persistence restore/snapshot, mesh drain, load probe, session sweep, shedding). The
  clonable `ConnCtx` bundle carries the per-connection handles.

- **`broker::shard::ShardState`** (`src/broker/shard.rs`) is the per-shard *data*: sessions, retained
  table, and subscription trie. It is `Rc<RefCell<>>`-shared between all connections on the shard — **safe
  precisely because no other core touches it**. Its concerns are split across sibling files: `shard.rs`
  (session lifecycle: open/close/suspend/expire), `shard/routing.rs` (one publish → per-subscriber
  deliveries), and `shard/mesh.rs` (cross-shard forwarding + session migration). The one-message-in-flight
  `Delivery`/`Mailbox` types live in `broker/delivery.rs` (the broker's delivery lingua franca), the mesh
  wire vocabulary (`MeshMsg`, `SessionControl`, `SharedEvent`) in `broker/messages.rs`.

- **`server::connection`** (`src/server/connection.rs` + `src/server/connection/`) is the per-connection
  state machine and all MQTT packet handlers, split by concern (`connect`, `publish`, `subscribe`,
  `control`, `delivery`, `ratelimit`). Durable QoS/session state lives *in the connection* while online
  (hot path, no shared borrow) and rests in the `ShardState` session only between connections.

- **Cross-shard routing** goes over `broker::messages` (`MeshMsg` on a glommio `channel_mesh`). A PUBLISH fans
  out to local subscribers, then is forwarded to peer shards which each run their own local match — no shard
  reads another's memory. QoS 1/2 forwards apply backpressure (awaiting send); `$SYS` metric publishes use
  best-effort non-blocking sends.

- **Session migration:** `SO_REUSEPORT` may rehash a reconnecting client (new ephemeral port) onto a
  *different* shard than the one holding its suspended session. The new shard issues a `Claim` over the mesh
  and the old shard hands the session (subs, in-flight QoS state, offline queue) back. Reliable within a
  shard and always exact for `runtime.cores = 1`.

- **`persistence`** (`src/persistence/`) snapshots durable state to disk (atomic write + `fdatasync` via
  io_uring `BufferedFile`) and restores it on startup, when `[persistence]` is enabled: the **retained set**
  (one shared file, all shards hold identical copies) and **suspended sessions** (one `sessions-<n>.mqtt`
  per shard — subs, in-flight QoS state, offline queue). Snapshot-based (periodic + on graceful shutdown),
  not a WAL, so a crash loses updates since the last snapshot.

## Configuration

The broker takes exactly one arg: a `.toml` path. Every field is optional with a documented default;
**unknown keys are rejected** (catches typos). `rusquitto.config.toml` is the annotated reference — copy and
edit it. Sections: `[server]`, `[runtime]` (`cores`, `placement`, `mesh_capacity`), `[logging]`, `[limits]`,
`[auth]` (`allow_anonymous` + `[[auth.users]]` with publish/subscribe ACLs), `[tls]`, `[persistence]`,
`[overload]`, `[sys]`. `RUST_LOG` overrides `logging.level` at startup. Config parsing/validation lives in
`src/config.rs`.

## Conventions

- **Preserve the shared-nothing invariant.** No `Mutex`/`RwLock`, no `std::thread`, nothing crossing shards
  except via the mesh. Shard-local state is single-threaded `Rc<RefCell<>>` on purpose. This is now
  **mechanically enforced**: `clippy.toml` disallows those types/methods, and the pre-commit hook runs
  clippy with `-D warnings`, so a violation fails the commit. Deliberately-threaded test harnesses
  (`examples/alloc_probe.rs`, the `stresser` example) opt out with a file-level `#![allow(...)]`.
- The connection state machine is unit-tested over an **in-memory `ByteStream` mock**
  (`src/server/connection/tests.rs`), so the full MQTT handshake runs without real sockets. Add handler
  tests there rather than spinning up a broker. `examples/alloc_probe.rs` measures idle memory per
  connection; the `stresser` example (`stress/stresser.rs`, registered in `Cargo.toml`) is the throughput
  hammer.
- glommio executor ids are **1-based**; use the mesh `peer_id()` (0-based) when electing a single shard to
  do broker-wide work (e.g. publishing `$SYS`).
- rustfmt uses **hard tabs**, 4-space width, `max_width = 120`, and `use_small_heuristics = "Off"`.

## Working notes

`.agents/` holds the detailed engineering log: `overview.md` (feature matrix), `architecture.md` (with a
current Key Files table), `dependencies.md`, `next-steps.md` (roadmap), and `progress.md` (full
implementation history + gotchas). When in doubt, trust the actual tree and `src/lib.rs` for module
structure. Version history is in `CHANGELOG.md`.
