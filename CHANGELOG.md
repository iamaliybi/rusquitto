# Changelog

All notable changes to rusquitto are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html): from 1.0 on, the major
version bumps for breaking changes, the minor for features, and the patch for fixes.

## [2.1.1] - 2026-07-08

Hardening: property-based fuzzing of the MQTT parser and packet handlers, wired
into `cargo test`. Closes the last testing gap named in the audits. No runtime
behaviour change.

### Added

- **`proptest` fuzz harness** (`src/server/connection/tests.rs::fuzz`) — three
  properties over an adversarial input distribution (pure random bytes, single
  plausible-but-malformed packets, and concatenations for framing-boundary
  fuzzing): the frame parser never panics and every parse loop terminates; a
  fully-connected connection fed arbitrary bytes drives every handler (publish,
  subscribe, the QoS ack flows, ping) through the real dispatch seam without
  panicking; and a pre-CONNECT arbitrary packet is rejected cleanly. Runs in
  `cargo test`, so the parser is continuously fuzzed in CI rather than only
  spot-checked against the hand-curated `malformed` battery. Validated deep
  (`PROPTEST_CASES=50000` → 50 k parser + 3 k dispatch cases) with no findings —
  the mature `mqttbytes` codec plus the broker's own guards hold up.

### Notes on the remaining audit deficits

- **Active-connection memory (~6×), CPU-per-message (~1.7×), and the −7%
  saturating throughput share one root cause** — the task-per-connection execution
  model and glommio's io_uring per-wake floor. This release *measured* that runtime
  tuning cannot close them: a `spin_before_park` sweep (0–200 µs) left saturating
  QoS 1 flat at ~81k, because under load the reactor never parks between message
  batches. The only lever is cutting the syscall/wake count per message —
  connections on the shard's own ring plus `IORING_OP_RECV` multishot — which is
  the dispatcher-mode program (`.agents/next-steps.md` §1, A2), gated behind a
  prototype. It is not a tuning problem and is not chased with knobs.
- **The cross-shard delivery tax (~2× same-core) is structural**, not a defect: a
  publisher and subscriber on different shards incur one mandatory cross-thread
  reactor wake over the mesh. Removing it needs cross-core shared state, which the
  shared-nothing invariant forbids. Recorded as an accepted trade in
  `.agents/scope.md`.

## [2.1.0] - 2026-07-07

Memory &amp; latency optimization, aligned with thread-per-core: the empty-broker
footprint is **more than halved** and single-message latency can now **beat
Mosquitto's** — both from tuning the glommio runtime, with zero throughput
regression and no architectural change. Two new `[runtime]` knobs.

### Added

- **`[runtime] io_memory_kib`** (default **512**, was glommio's implicit 10 MiB
  *per core*). glommio pre-allocates and **pins** an io_uring registered-buffer
  pool per executor at startup — it is resident for the shard's whole life and was
  the dominant term in the empty-broker footprint. The network fast path
  (`recv`/`send`) never draws from it; only DMA file I/O (persistence snapshots)
  does, and that falls back to the heap when the pool is exhausted, so shrinking it
  is safe (glommio's 64 KiB floor is enforced). Measured, same box:
  - Empty-broker RSS, **1 shard: 17.6 → 8.1 MiB** (Mosquitto's 7.6 parity);
    **4 shards: 51.7 → 13.1 MiB** (~1.6 MiB/shard, was ~11).
  - **Zero regression**: saturating QoS 1 unchanged (79.4k/core, 359k on 3 shards),
    parked-idle floor unchanged (0.68 KiB/conn), adversarial battery still 12/12.
  - Bonus: the small pinned pool keeps multi-shard within a tight `RLIMIT_MEMLOCK`,
    so multi-shard now starts on memlock-constrained hosts (e.g. non-interactive
    WSL) that the 10 MiB×N default could push into io_uring `ENOMEM`.
- **`[runtime] spin_before_park_us`** (default **0** = off). Busy-polls io_uring
  completions for this many microseconds before parking the reactor, removing the
  park/unpark round-trip from single-message latency. Measured at 50 µs:
  **PUBLISH→PUBACK RTT p50 37 → 27 µs** — below Mosquitto's 31.9 µs. Opt-in because
  spinning trades idle CPU for the latency; suited to latency-critical, steadily-busy
  shards. Effective only under `max-spread`/`max-pack` placement (glommio disables
  spinning under `unbound`).

### Notes

- **Per-connection cost is unchanged** (idle 6.24, active 7.56 KiB/conn) — this
  release lowers the fixed baseline and unlocks cheap multi-shard, not the
  per-connection constant. Total footprint still improves markedly: a 500-connection
  active broker dropped 21.4 → 11.8 MiB (−45%) from the baseline win alone. Closing
  the per-connection constant is the dispatcher-mode program (`.agents/next-steps.md`
  §1), whose A0 design study landed this cycle; the rewrite itself is gated to the
  next release behind a prototype.

## [2.0.0] - 2026-07-07

The parked-connection idle path: idle connections now cost **0.68 KiB** of live
heap instead of 3.8 KiB — the connection-density gap identified in the v1.7/v1.10
audits, closed. A major version because the broker's default runtime behaviour
changes (parking is on by default), not because any wire or config compatibility
breaks: every v1.x config file loads unchanged.

### Added

- **Connection parking (`[parking]`, on by default)** — a plain-TCP connection
  that stays *fully idle* (no in-flight QoS state in either direction, no
  buffered bytes, no partial frame, everything flushed) for `idle_grace_secs`
  (default 30) has its per-connection task and io_uring read `Source` torn down;
  only its fd — armed as a oneshot `POLL_ADD` on a small per-shard **raw
  io_uring readiness ring** — and a boxed resume record remain. Any inbound byte
  (ingress) or a delivery routed to the parked client (egress, via a shard-local
  wake channel) resurrects it transparently; the client cannot tell the
  difference. Plain TCP only: TLS/WebSocket streams carry mid-stream codec state
  and never park. `parking.enabled = false` restores the exact v1.x path.
  - Measured (`alloc_probe`, 2000 idle connections, 1 shard): **0.68 KiB/conn
    live heap parked** vs 3.8 KiB/conn in v1.10.0 (5.6×); the Phase-0
    `park_probe` floor of 0.08 KiB is the fd-only bound, the rest is the resume
    record, registry slot, and suspended session snapshot.
  - Semantics preserved across the park, all covered by tests: QoS 0 *and* QoS 1/2
    deliveries queue and wake (a parked client is connected, not offline);
    keep-alive is enforced by the parking task's sweep against the deadline
    frozen at park time, Will included; session takeover, Clean Start, and
    cross-shard `Claim` close the dormant fd with takeover semantics (no Will);
    topic aliases in both directions, the packet-id allocator, and negotiated
    limits survive; shared-subscription member picks count parked members as
    online (anything else would desync the cross-shard pick); graceful shutdown
    drains parked fds with a best-effort DISCONNECT (0x8B) and converts their
    sessions to properly suspended before the final persistence snapshot.
  - Every removal is generation-guarded twice (io_uring `user_data` slab tag +
    broker session generation), so stale completions and takeover/wake races
    resolve as no-ops or quiet closes — verified by the park/unpark/re-park
    cycle tests and the new adversarial scenarios.
- **`$SYS/broker/parked-connections`** — gauge of currently parked connections
  (parked clients still count in `$SYS/broker/clients/connected`).
- **Four adversarial parking scenarios** in `stress/attack.py` (`park-all`):
  `park-herd` (park 500, wake all with one broadcast — 500/500 delivered),
  `park-thrash` (3 full park/unpark cycles × 500 conns via PINGREQ — 1500/1500),
  `park-takeover` (reconnect storm onto parked ids — 500/500 dormant fds
  closed), `park-dribble` (wake with one byte of a frame, then stall — 200/200
  reaped by the partial-frame guard after unpark). Full battery: **12/12**.
- **Tests**: 11 broker-layer parking tests (park/reattach/suspend generation
  guards, wake dedup, WAL and persistence exclusion, shared-group pick), 4
  connection-level tests (predicate truth table, park-after-grace,
  resume-replay-repark round trip, takeover-displaced resume), and 7
  integration tests over real sockets (egress wake across two park cycles,
  ingress PINGREQ/PUBLISH, parked keep-alive expiry firing the Will, EOF-while-
  parked firing the Will, takeover, `$SYS` gauge) — 111 unit + 22 integration.
- `examples/alloc_probe.rs` now also reports the **parked** live-heap floor.

### Changed

- **`io-uring` is now a runtime dependency** (was dev-only, for the spike): the
  per-shard readiness ring is a 1024-entry raw ring (~100 KiB locked memory per
  shard, created at startup; if creation fails — e.g. a tight `RLIMIT_MEMLOCK` —
  the shard logs a warning and runs with parking disabled rather than failing).
- **Live (unparked) idle connections measure ~5.0 KiB/conn** (was 3.8): the
  park-capable connection driver keeps the serve context across its await and
  carries the grace/activity bookkeeping. Idle fleets spend all but the first
  `idle_grace_secs` in the parked state, so steady-state density improves ~5.6×
  despite this; with `parking.enabled = false` the v1.x path (and its 3.8
  KiB/conn) is used verbatim.
- `shed_connections` documents that parked connections are deliberately never
  shed: shedding relieves reactor saturation (a CPU signal) and parked
  connections contribute ~zero CPU, so closing them would only disconnect the
  best-behaved clients and add reconnect load.

### Fixed

- **The per-PUBLISH `debug!` event cost ~38 µs/msg under the default log filter**
  (`info,rusquitto=debug`): the event was formatted and dispatched on the shard
  thread for every message, more than doubling per-message CPU (64.5 → 26.5
  µs/msg) and inflating every published benchmark — the audit's "QoS 1 far
  behind Mosquitto" reading was primarily this tax (plus a saturating-vs-
  ping-pong harness mismatch in the comparison). Demoted to `trace!` — wire-level
  per-message detail is what the trace level is for; the default filter now
  costs nothing measurable per message (all remaining `debug!` sites are
  per-connection lifecycle). Measured after the fix, same box, identical
  harnesses, Mosquitto 2.0.18 with `set_tcp_nodelay true`:
  - 200-connection publisher→ack: QoS 0 **328k vs 149k** msg/s (2.2×), QoS 1
    **36.1k vs 32.7k** (+11%), QoS 2 **21.6k vs 17.4k** (+24%) — rusquitto
    leads every tier.
  - Saturating QoS 1 (Rust hammer): 1 shard 77.6k vs 87.1k (−11%, the
    runtime's per-wake floor — see below); **3 shards 328.9k, 3.8× Mosquitto's
    single-threaded ceiling**.
  - publish→deliver p50: 43.4 µs same-core (was 60), 60.9 µs cross-shard
    (was 93); 1-connection PUBLISH→PUBACK RTT p50 37.5 µs (was 51.1).
- Added **`examples/wake_probe.rs`** — a bare glommio echo loop measuring the
  runtime's per-wake floor (~30 µs RTT / 21.5 µs CPU on this box), which
  attributes the remaining single-message gap vs epoll-based brokers to the
  runtime rather than the MQTT layer (which adds only ~5 µs CPU over the
  floor). The residual is tracked with candidate fixes in
  `.agents/next-steps.md` §1.

### Notes on the ~0.7 KiB parked floor

`ParkedConn` + boxed `ResumeState` (~0.2 KiB) + the suspended-style session
snapshot and registry/index entries make up the measured 0.68 KiB. The 0.1 KiB
aspiration from the spike excluded everything but the fd and a minimal struct;
the production number carries the full negotiated MQTT state (aliases, Will,
limits, packet-id allocator) so the resume is exact. Kernel socket memory is
outside RSS either way.

## [1.10.0] - 2026-07-06

Mutual-TLS identity: a verified client certificate can now carry a per-device MQTT
identity for ACLs.

### Added

- **`[tls] cert_cn_as_username`** — when mutual TLS is enabled, use a verified
  client certificate's subject **Common Name** as the MQTT username, so
  `[[auth.users]]` ACLs apply per device. A cert-verified client that sends no
  username is authenticated by the certificate and keyed on its CN; a client that
  also sends an explicit username is checked against `[auth]` the usual way (the
  certificate only gated the transport). Default `false` — a cert-verified client
  with no username remains anonymous for ACLs, as before. Adds the `x509-parser`
  dependency (rustls verifies the chain but does not expose the parsed subject).
  Verified end-to-end: with a client CN mapped to a `[[auth.users]]` entry whose
  publish ACL is `sensors/01/#`, an in-scope publish is delivered and an
  out-of-scope one is refused.

## [1.9.2] - 2026-07-06

Test coverage: a real end-to-end integration suite, plus documentation of the
whole testing strategy.

### Added

- **`tests/integration.rs` — 15 end-to-end tests** that boot a real broker
  in-process and drive it over real TCP sockets with a minimal MQTT 5 client
  (built on `mqttbytes`), covering flows the unit tests (over an in-memory mock
  stream) can't: CONNACK, QoS 0/1/2 full handshakes, QoS downgrade-to-granted,
  retained replay + clear, `+`/`#` wildcards, unsubscribe, persistent-session
  offline-queue replay, will-on-abrupt-disconnect, malformed-frame survival, auth
  (bad password / anonymous rejection / success), ACL enforcement, cross-shard
  delivery, and shared-subscription exactly-once delivery across shards. Brokers
  are lazily started and shared per configuration, so the suite adds ~2 s to
  `cargo test` and runs in CI.
- **`TESTING.md`** — the full testing strategy, layer by layer: unit
  (mock-stream state machine), integration (in-process broker), the adversarial
  battery, crash-recovery and mTLS harnesses, soak, and the memory/throughput
  probes — with how to run each and the known gaps (no parser fuzzing yet; `wss`
  not exercised end-to-end).

### Changed

- **Logging init is now idempotent** (`try_init` instead of `init`), so starting
  more than one broker in a single process — embedding, or the integration suite —
  is a no-op on the second call rather than a panic.

## [1.9.1] - 2026-07-06

Robustness plus a measurement: the ack-bound throughput and cross-shard
single-message latency items from the audit were investigated and found to be at
their floor, with no application-level headroom.

### Changed

- **`TCP_NODELAY` is now set explicitly on every accepted socket**, not left to
  kernel inheritance from the listener. MQTT is request/response, so Nagle
  coalescing a small PUBACK/PUBLISH would cost a round-trip of latency — setting
  the option per-connection guarantees it regardless of platform or version.
- **Single-shard brokers skip the mesh fan-out path.** `fan_out` no longer clones
  the (absent) peer-senders handle or runs the self-only loop when there are no
  peers; it goes straight to local delivery.

### Notes on the audit items (measured, not guessed)

Single-shard QoS 1 request-response is **55 µs p50** (76 µs p90, 128 µs p99), and
`TCP_NODELAY` was already effective (no Nagle artifacts) — so the per-request cost
is the `mqttbytes` parse plus the socket round-trip, at parity with a mature C
broker; the change above is a portability guarantee, not a latency change here.
The cross-shard single-message tax is one cross-thread reactor wake
(glommio-internal). Neither has application-level headroom; going lower is the
below-glommio work tracked in `next-steps.md` §1. **These are robustness/cleanup
changes, not a throughput or latency win.**

## [1.9.0] - 2026-07-06

Cross-shard reliability and hot-path efficiency: the mesh control plane is now
loss-free under overload, and the cross-shard and QoS delivery paths do less work
per message.

### Changed

- **Mesh control plane is now reliable under overload.** Session `Claim`/`Handoff`
  (migration) and shared-subscription `Join`/`Leave` (membership) were best-effort
  (`try_send_to`, drop-on-full). A drop under sustained saturation could desync the
  replicated shared-subscription membership view — risking transient double- or
  zero-delivery — or silently lose a migrating client's session. They now go
  through a per-shard **reliable outbox**: enqueuing is synchronous and never
  drops, and a foreground task drains it with the awaiting `send_to` (mesh
  backpressure), in FIFO order so a `Join` can't be reordered past a later `Leave`.
  The best-effort data plane (`$SYS`, QoS 0 publishes) is unchanged; control volume
  is low, so the outbox stays small even under data-plane saturation. Verified on a
  3-shard broker: shared-subscription delivery is exactly-once across members on
  different shards (60/60, no loss, no duplicates).
- **Inbound mesh receiver batch-drains.** After a blocking `recv` wakes, it drains
  every already-queued message via `poll_once` without yielding, so a peer's
  forwarded burst is handled in a single wake instead of one reactor reschedule per
  message — cutting cross-shard scheduling overhead and CPU under load.
- **QoS 1/2 delivery clones the PUBLISH once, not twice.** `send_publish` kept a
  working copy *and* an in-flight retransmit copy. It now takes the retransmit copy
  only when outbound topic-aliasing could clear the topic; on the common
  non-aliasing path the message is moved into the in-flight table after a
  successful write — one fewer `Publish` clone (topic + properties) per QoS 1/2
  delivery, which scales with fan-out. The in-flight entry is recorded post-write,
  simplifying the rollback paths.

## [1.8.1] - 2026-07-06

Idle-connection memory: a safe reduction, plus the measurement that grounds the
larger density work.

### Changed

- **Topic-alias tables are now lazily boxed** (`Option<Box<AliasTables>>`). A
  connection that never registers an inbound alias and is never assigned an
  outbound one holds 8 bytes here instead of two `HashMap`s, so the idle /
  non-aliasing common case pays nothing. Idle heap drops **3.87 → 3.7 KiB/conn**
  (`alloc_probe`), with no change to the aliasing path.

### Added

- **`examples/park_probe.rs`** — a feasibility spike (io-uring dev-dependency,
  not used by the broker) that decomposes and attacks the idle floor. It shows
  `alloc_probe`'s ~1.9 KiB/conn is glommio's per-connection task + io_uring read
  `Source` (not a buffer — the stream is `NonBuffered`), and that parking an idle
  fd as a 48-byte struct on one shared `IORING_OP_POLL_ADD` ring — no
  per-connection task — costs **0.06 KiB heap / 0.08 KiB RSS** (a ~46× reduction,
  an order of magnitude under Mosquitto), with the wake path proven for all
  2000/2000 fds. The staged plan for the parked idle path is in
  `.agents/next-steps.md`.

## [1.8.0] - 2026-07-05

Durability and transport-security release: a session write-ahead log, mutual TLS
with certificate hot-reload, and a hardening of the last adversarial gap.

### Added

- **Session/queued-message write-ahead log** (`[persistence] wal_flush_ms`,
  default 200 ms; `0` disables). Persistence was snapshot-based, so a crash lost
  every session that suspended — and every QoS > 0 message queued to a suspended
  session — since the last snapshot (`snapshot_interval`). A new per-shard,
  append-only, group-committed WAL (`persistence/wal.rs`) records session
  upserts/removes and offline-queue growth as they happen and replays them over
  the snapshot on startup, shrinking the crash window from `snapshot_interval`
  (default 300 s) to the flush interval. Records are last-writer-wins per client
  id; a torn trailing record from a crash mid-append is detected and skipped. A
  periodic checkpoint (a full session snapshot) truncates the log so it stays
  bounded. Retained messages remain snapshot-only. Verified end-to-end: a
  `kill -9` between snapshots, then restart, restores the suspended session and
  redelivers its queued message.
- **Mutual TLS (client-certificate authentication)** (`[tls] client_ca_file`,
  `require_client_cert`). A presented client certificate is verified against the
  configured CA; with `require_client_cert` a client without a trusted
  certificate fails the handshake. A cert-verified client that sends no MQTT
  username is authenticated by the certificate alone (so mTLS works with
  `allow_anonymous = false`).
- **Certificate hot-reload** (`[tls] reload_interval`, seconds; `0` = off). Each
  shard watches the cert/key/CA files and rebuilds its own acceptor when they
  change, so a rotated certificate reaches new handshakes without a restart.
  Existing connections keep the certificate they handshook with. Shard-local —
  no cross-core coordination, no lock (fits thread-per-core); a failed reload
  keeps the previous certificate and retries.

### Changed

- **Partial-frame stall guard** closes the last adversarial-battery gap (the
  suite's 15th case, a header-only truncated CONNECT). `Connection::event_loop`
  now bounds *any* incomplete frame by the handshake window (`connect_timeout`),
  not just a fully-silent socket — which also closes the previously **unbounded**
  post-CONNECT slow-loris where a client finished CONNECT with keep-alive
  disabled (`keep_alive = 0`) and then dribbled a partial packet header and
  stalled. Pre- and post-CONNECT stalls are now one invariant.

## [1.7.0] - 2026-07-05

Structural refactor from an architectural review — no behaviour, protocol, API,
config, or performance change (allocprobe idle memory, the throughput A/B, and
the full smoke battery are unchanged).

### Changed

- **The shared-nothing invariant is now mechanically enforced.** A new
  `clippy.toml` disallows `Mutex`/`RwLock`/`Condvar`/`std::sync::mpsc`/`Barrier`
  and `std::thread::{spawn,sleep}` with reasons pointing at the channel mesh; the
  pre-commit hook already runs `clippy -D warnings`, so a cross-thread lock or
  ad-hoc thread now fails the commit instead of silently breaking the model.
- **`server/worker.rs` (882 lines) split into `server/shard/`** by concern:
  `shard.rs` (`run_shard` orchestration — renamed from `init`), `accept.rs`
  (accept loop, connection accounting, admission control, listener binding),
  `serve.rs` (transport-stack dispatch), `maintenance.rs` (persistence
  restore/snapshot, mesh drain, load probe, session sweep, shedding). A new
  `ConnCtx` bundle collapses the seven positional arguments that were threaded
  through the serve path (removing four `#[allow(too_many_arguments)]`).
- **Clearer names**: `Connection.state` → `shard` (it is the *shard*-shared
  state, not connection state — the key thread-per-core distinction);
  `Connection.buffer`/`out` → `inbound`/`outbound`; `ShardState.mesh` →
  `mesh_tx` (it holds only senders); `broker/mesh.rs` → `broker/messages.rs`
  (the mesh *vocabulary*, distinct from `broker/shard/mesh.rs`, the *behaviour*);
  `connection/ack.rs` → `control.rs`; `examples/allocprobe.rs` → `alloc_probe.rs`.
- **`broker/session.rs` split**: the `Delivery`/`Mailbox` delivery types (used by
  routing, connection, and persistence) moved to a new `broker/delivery.rs`,
  separating the broker's delivery lingua franca from durable session state.
- **The last process-global mutable atom is gone**: the server-assigned client-id
  counter (`static NEXT_CLIENT_ID: AtomicU64`) is now a shard-local field on
  `ShardState` — the generated id already embeds the shard id, so per-shard
  counters stay broker-unique with zero cross-core traffic on the CONNECT path.
- **Module layout modernised**: all eight `foo/mod.rs` files migrated to the
  file-based `foo.rs`-beside-`foo/` form.
- **`stress/stresser.rs` is now a Cargo example** (`--example stresser`), so the
  throughput hammer gets `fmt`/`clippy`/CI coverage while remaining
  dependency-free and standalone-`rustc`-compilable.

## [1.6.5] - 2026-07-05

### Changed

- **Sub-4-KiB idle connections: 7.5 → 3.9 KiB RSS/conn** (originally 24.9).
  The connection state machine went from 3312 to 624 bytes resident:
  - The mesh forward in `fan_out` now tries the non-blocking send first and
    only a *full* link falls back to the awaiting (backpressure) send — boxed,
    so its machinery exists on the heap during congestion instead of occupying
    every connection forever. The `GlommioError<MeshMsg>` result (~230 B) is
    reduced to a flag before any await for the same reason. Delivery
    guarantees are unchanged: QoS > 0 still never drops on a full link.
  - The PUBLISH, PUBREL, and CONNECT handler futures are boxed through
    plain-fn seams: one small allocation per such packet buys ~2.4 KiB out of
    every connection's resident memory (throughput-checked against v1.6.0).
  - Parse and dispatch merged into one `process_one` (one `Packet`-sized slot,
    not two), the throttle sleep is boxed (exists only while pacing), the Will
    Message and each session's suspended snapshot / armed will are boxed
    (rare or absent-while-connected data no longer bloats every `Connection`
    and every sessions-table slot).
- **One less allocation per inbound PUBLISH**: the fan-out message is
  normalized in place instead of cloned, removing a topic-string allocation
  and copy from the hottest path.

## [1.6.0] - 2026-07-05

Small-host hardening: less memory per connection (process *and* kernel side),
post-burst memory returned to the OS, and an aarch64 binary for Graviton/t4g
deployment.

### Changed

- **Idle memory: 16.1 → 7.5 KiB per connection.** A heap-decomposition pass
  (the new `allocprobe` example — a size-class histogram allocator around an
  in-process broker) attributed ~13 KiB of every connection to its spawned
  task: the task future reserved space for the connection state machine of
  *every* transport branch at once (and, because temporaries in a statement
  containing `.await` live across the suspension, inline `Box::pin(fut).await`
  didn't help). Each transport pipeline is now boxed via a plain-function seam,
  so the long-lived task measures ~600 bytes and each connection heap-allocates
  only its own transport's state (~4.5 KiB for plain TCP). Adversarial
  stalled-subscriber burst: 31.1 → 22.4 KiB/conn. The remaining footprint is
  fully attributed (connection state machine + session/channel bookkeeping).
- **Post-burst memory is returned to the kernel.** glibc kept a burst's freed
  allocations resident in its arenas indefinitely (measured ~30–50 MB after a
  2000-connection burst fully disconnected). The maintenance tick now calls
  `malloc_trim` every 30 s (peer 0 only — it walks all arenas); verified: RSS
  fell from 51.0 MB to 20.3 MB at the first tick after a burst.

### Added

- **Kernel socket-buffer caps** — `[server] socket_recv_buffer` /
  `socket_send_buffer` (bytes, `0` = kernel default) set `SO_RCVBUF` /
  `SO_SNDBUF` on the listeners, inherited by every accepted socket. On
  memory-tight hosts this bounds *kernel-side* per-connection memory — which
  lives outside the broker's RSS — and caps the advertised TCP window.
  Verified via `ss` skmem on an accepted socket.
- **aarch64 builds** — releases now ship a `rusquitto-aarch64-unknown-linux-gnu`
  binary (glibc ≥ 2.31) for Graviton/t4g-class hosts, cross-compiled with
  `cargo zigbuild`; see the README's cross-compiling note.
- **`examples/allocprobe.rs`** — the reusable heap-decomposition probe behind
  the numbers above (no root or external profiler needed).

## [1.5.0] - 2026-07-05

The backlog-clearing release: every remaining item on the Phase-3 hardening
roadmap (`.agents/next-steps.md`) is done.

### Added

- **Globally-coordinated shared subscriptions** — a `$share/{group}/{filter}`
  message now reaches **exactly one member cluster-wide**, even when the group's
  members are spread across shards (previously one member *per shard*).
  Membership of connected members is replicated to every shard over the channel
  mesh (`Join`/`Leave` broadcasts on subscribe, unsubscribe, disconnect/suspend,
  resume, and migration); each shard applies the same deterministic content-hash
  pick to the same sorted member view, so all shards agree on the recipient with
  no coordination round-trip. Purely shard-local groups keep round-robin
  fairness, and suspended members may still queue QoS > 0 there. Membership
  broadcasts are best-effort under mesh overload (documented). Also enforces
  MQTT 5 §3.8.3.1: **No Local on a shared subscription is rejected** (it would
  desynchronize the cluster-wide pick; pre-existing persisted/migrated state is
  normalized). Verified end-to-end on a 2-shard broker: 6 members straddling
  both shards, 40 published messages, exactly 40 deliveries with every member
  receiving a share.
- **Outbound topic aliases** (MQTT 5) — the broker now assigns aliases on the
  publishes it *sends*, honouring the client's CONNECT Topic Alias Maximum
  (capped at 32 per connection to bound memory): the first publish of a topic
  registers an alias, and every repeat carries just the two-byte alias with an
  empty topic name. In-flight copies keep the full topic so a retransmit on a
  fresh connection (empty alias table) stays valid, and an alias registered by
  a packet that is then dropped (client Maximum Packet Size) is rolled back.
- **Argon2id password hashing** — `[[auth.users]]` `password_hash` now accepts
  an Argon2 PHC string (`$argon2id$...`; salted, memory-hard — the recommended
  form) alongside the legacy hex SHA-256. Parameters ride in the PHC string, so
  per-user settings work. Unknown-username checks burn the same Argon2 cost as
  a real verify when any user is Argon2-hashed, keeping the user-enumeration
  timing oracle closed. Note a verify deliberately costs ~10–50 ms on the
  accepting core per CONNECT attempt.
- **Anonymous-client ACL** — new `[auth]` `anonymous_publish` /
  `anonymous_subscribe` topic-filter allow-lists restrict what anonymous
  clients may do (omitted = unrestricted, as before; empty list = deny all),
  closing the "anonymous bypasses ACL" gap.

## [1.4.0] - 2026-07-05

### Changed

- **Per-connection memory diet** for high concurrency on small hosts (1–2 GB):
  the outbound mailbox no longer pre-allocates its ring (glommio's bounded
  channel reserved 320 KiB *per connection*; now an unbounded channel with the
  same drop-on-full bound enforced at the routing site — idle cost zero), the
  read assembly buffer starts empty and grows on demand with an adaptive
  512 B–8 KiB per-read reservation (reads land directly in the buffer, removing
  the fixed 2 KiB scratch copy), and oversized buffers are trimmed once idle so
  a burst's high-water mark isn't pinned forever. Measured (2000 connections,
  release build): idle RSS 24.9 → 16.1 KiB/conn, adversarial stalled-subscriber
  burst 86.9 → 31.1 KiB/conn, idle virtual 342.8 → 0.84 KiB/conn.
  `limits.initial_read_buffer` now defaults to `0` (grow on demand); a non-zero
  value still pre-sizes the buffer.
- **Coalesced writes**: every packet a wakeup produces (ack bursts, fan-out
  batches, resume retransmits) is encoded into one output buffer and written
  with a single `write_all` — one io_uring op, one TLS record, one WebSocket
  frame per batch instead of one per packet. The buffer flushes at 16 KiB
  mid-batch, which also caps the elastic memory a stalled consumer can pin.

### Added

- **Memory tooling** (`stress/`): `memprobe.py` measures resident/virtual
  memory per idle connection and under a stalled-subscriber burst;
  `soak.py` runs adversarial churn/flood/stall/recover cycles for a
  configurable duration, samples broker RSS, and fails on sustained growth
  (leak/fragmentation detection). Verified: RSS plateaus (+0.7 % over a
  14-cycle run) under repeated adversarial cycles.

## [1.3.0] - 2026-07-05

### Added

- **Retained-message persistence** (`[persistence]`, opt-in) — the retained set is
  snapshotted to disk and restored on startup, so "last known value" topics survive
  a restart. Every shard holds an identical retained copy, so one shard (peer 0)
  writes the snapshot and every shard reloads it on boot — no cross-shard
  coordination. The snapshot is the concatenated MQTT wire bytes of each retained
  PUBLISH behind a magic header (same codec as the network, so all v5 properties
  round-trip). Writes are atomic (temp file → `fdatasync` → rename) via glommio's
  io_uring `BufferedFile`, so they never block the reactor and a crash mid-write
  can't corrupt the previous snapshot. Snapshots run periodically
  (`snapshot_interval`) and on graceful shutdown. Verified end-to-end: retained
  messages survive a graceful restart and a `kill -9` (up to the last snapshot);
  non-retained messages and retained clears behave correctly across restart.

- **Session persistence** (`[persistence]`, same opt-in switch) — suspended
  (offline) sessions are now snapshotted to disk alongside retained messages and
  restored on startup, so a client with a non-zero Session Expiry Interval keeps
  its subscriptions, in-flight QoS 1/2 state, and offline message queue across a
  broker restart. Sessions are shard-local, so each shard persists its own
  `sessions-<n>.mqtt` file (nested length-prefixed encoding, PUBLISHes stored as
  MQTT wire bytes so all v5 properties round-trip); if `runtime.cores` shrinks
  between runs, peer 0 loads any orphaned session files so none are lost. Restored
  sessions come back suspended and resume directly or via the cross-shard
  `Claim`/`Handoff` migration, so they inherit the same best-effort-under-mesh
  caveat as live cross-shard resume. Writes reuse the same atomic io_uring codec as
  retained snapshots.

## [1.2.0] - 2026-07-04

### Added

- **Overload handling** (`[overload]`) — a per-shard subsystem for the single-hot-core
  case, modelled on Seastar/ScyllaDB. Each shard runs a lightweight probe that measures
  its **reactor scheduling delay** (how far behind normal-priority work runs — a
  saturation signal), smoothed into an EWMA and exposed at
  `$SYS/broker/load/max-scheduling-delay-ms`. On top of it:
  - **Scheduling groups**: background housekeeping (`$SYS`, session sweep, shedding)
    now runs in a low-share glommio task queue, so under load it yields to the
    client-serving work on the default queue instead of competing with it.
  - **Stall WARN** (`overload.stall_warn_ms`): logs while a shard stays overloaded
    (with hysteresis), and an info line when it recovers.
  - **Admission control** (`overload.admission_delay_ms`, opt-in): rejects new
    connections while a shard is overloaded, so the client's retry may hash onto a
    cooler core; existing connections are untouched.
  - **Load shedding** (`overload.shed_delay_ms` / `shed_batch`, opt-in): under
    sustained overload, closes a batch of connections per second so they reconnect
    from a new source port and `SO_REUSEPORT` rehashes them elsewhere — the
    thread-per-core way to rebalance (move the connection, since compute can't move).
  - New `$SYS/broker/load/{connections-shed,admission-rejected}` counters.

  Verified end-to-end by saturating one core (1500 subscribers + a flood publisher):
  the gauge climbed from ~0 to seconds of delay and recovered afterward, the stall
  WARN fired, admission control rejected new connections, and shedding closed
  connections in batches.
- **Per-connection PUBLISH rate limiting** (`limits.max_message_rate`, messages/sec,
  `0` = off). A token bucket (one-second burst, then paced to the rate) *throttles*
  an over-rate publisher — the connection sleeps for the computed delay, applying
  TCP backpressure — rather than dropping messages. In the thread-per-core model a
  connection is served entirely by the shard that accepted it, so this bounds how
  much CPU one noisy publisher can draw on its pinned core. Verified end-to-end: 30
  messages down one connection at a 10/s limit are delivered over ~2s with zero
  drops (vs ~0.2s unlimited).

## [1.1.0] - 2026-07-04

### Added

- **TLS termination (`mqtts://` `:8883`, `wss://` `:8884`).** Opt-in via the new
  `[tls]` config section (`enabled`, `port`, `websocket`, `websocket_port`,
  `cert_file`, `key_file`). Built on rustls with the `ring` provider, layered
  behind the existing `ByteStream` seam so the MQTT engine is reused unchanged;
  `WsStream` was made generic over its inner stream, so `wss://` is WebSocket over
  TLS with no duplicated protocol code. Security posture: **only TLS 1.3 and 1.2**
  are offered, restricted to a curated list of **AEAD + ECDHE** cipher suites
  (forward secrecy; no CBC/RC4/3DES/static-RSA). The TLS handshake is bounded by
  `connect_timeout` (a slow-loris guard), the shared `ServerConfig` is built once
  and fails startup fast on a bad cert/key, and all listener ports are validated
  to be distinct. No client-certificate (mTLS) auth — clients authenticate at the
  MQTT layer over the encrypted link. Verified end-to-end with `mosquitto` v5 over
  `mqtts` and an `openssl`-driven `wss` upgrade, including confirming TLS 1.0/1.1
  are rejected.

### Changed

- **Code structure.** Split the two largest files by responsibility (no behaviour
  change): `server/connection.rs` (1318 lines) → `connection/{mod,connect,publish,
  subscribe,ack,delivery}.rs`, and `broker/shard.rs` → `shard/{mod,routing,mesh,
  tests}.rs`. Folded repeated encode-then-write boilerplate into a `Connection::send`
  helper.
- **Tests (34 → 59).** Added a `ByteStream` mock harness that drives the connection
  state machine without sockets, config-validation tests, and rustls handshake
  tests (in-memory negotiation proving both TLS 1.3 and 1.2, and that only the
  curated cipher suites are offered).
- **Tooling.** A versioned `.githooks/pre-commit` runs `cargo fmt --check`, clippy
  (`-D warnings`), and the test suite before each commit (`./.githooks/install.sh`).

## [1.0.0] - 2026-07-03

First production release. Adds a WebSocket transport, a production security pass,
and a memory optimization, on top of a restructured, SOLID-leaning codebase.

### Added

- **WebSocket transport (`:1884`).** Browser and Node clients can speak MQTT over
  WebSocket (RFC 6455 server handshake, `mqtt` subprotocol, binary frames) without
  a TCP bridge. Enabled by default; `[server] websocket` / `websocket_port` control
  it. Introduces a `ByteStream` transport abstraction so the MQTT state machine is
  written once and runs over both TCP and WebSocket.
- **Connection hardening.** The first packet must be CONNECT and only one is allowed
  (closing a pre-auth PUBLISH/SUBSCRIBE bypass); a socket that never sends CONNECT is
  dropped after `limits.connect_timeout` — including a stalled **WebSocket handshake**,
  which is bounded by the same timeout; an idle connection is dropped at 1.5× the
  negotiated keep-alive.
- **Topic reservation and validation.** Client PUBLISHes to `$`-prefixed topics
  (e.g. spoofing `$SYS`) or to wildcard/empty/NUL topics are rejected, and malformed
  SUBSCRIBE filters are refused per-filter.
- **Resource caps** (`[limits]`): `max_session_expiry`, `max_subscriptions_per_client`,
  `max_retained_messages` (per shard), a bounded per-connection outbound queue, and
  client-id length/charset validation. The per-connection outbound **mailbox is
  bounded**, so a subscriber that stops reading its socket can't force unbounded broker
  memory growth (excess deliveries to that stuck consumer are dropped). WebSocket
  control frames are validated (≤125 bytes, unfragmented) per RFC 6455.
- **Topic/filter depth cap** (128 levels). The subscription trie is walked recursively,
  so an unbounded-depth topic could overflow the executor stack (an uncatchable abort)
  and a deep SUBSCRIBE could balloon trie memory; both are now rejected up front.
- **Panic-safe connection accounting.** The live-connection count is released through
  an RAII guard, so a task that panics can't leak a slot and eventually wedge the shard.
- **Per-IP connection cap** (`limits.max_connections_per_ip`, `0` = unlimited). Bounds
  how many concurrent connections one client IP may hold on a shard, limiting a
  single-source connection flood. Per-shard, and most useful for direct clients — behind
  a reverse proxy every connection shares the proxy IP, so rely on the proxy/network
  layer there. The broker also warns at startup when `keep_alive = 0`, since that
  disables idle-connection reaping.

### Changed

- **Constant-time credential comparison**, plus a throwaway hash for unknown users so
  authentication timing doesn't reveal whether a username exists. Server-assigned
  client ids are now unguessable (per-process random + counter) rather than sequential.
- **Topic-trie memory:** trie levels are keyed by interned `Rc<str>` segments, so a
  segment that recurs across many filters is stored once.
- **Project layout:** split into cohesive layers — `lib.rs` + thin `main.rs`,
  `telemetry/`, `transport/`, `broker/{mesh,session,shard,topics}`, and a pure
  `protocol` module — with the dev-only `mosquitto` bin removed.

## [0.6.1] - 2026-07-03

Internal quality pass — no behavior or configuration change.

### Changed

- **Engine unit tests** — the routing and session core is now covered by unit
  tests (fan-out and QoS downgrade, subscription-identifier accumulation, No Local,
  Retain As Published, shared-group round-robin, retained store/clear, session
  open/resume/destroy, generation guard, and the expiry/will sweep). 24 tests total.
- **Clean `cargo clippy`** — boxed the large `Event::Incoming` variant, derived
  `Config`'s `Default`, and collapsed a nested `if`.
- **`SubOptions` struct** replaces the long positional argument lists on
  `subscribe` / `TopicTrie::insert` (removing two `too_many_arguments` allows and
  the transposable adjacent `bool`s); `min_qos` is no longer duplicated.

## [0.6.0] - 2026-07-03

### Changed

- **`[runtime]` now takes a direct `cores` count instead of `shards` + `cpu_fraction`.**
  Set `cores = N` to run on N CPU cores (the broker pins one shard per core, so
  this is also the shard count); omit it to use every online core. A value above
  the online core count is clamped down with a warning. The old `shards` and
  `cpu_fraction` keys are removed — configs using them are now rejected.
- **Single reference config.** The `rusquitto.toml` and `rusquitto.default.toml`
  examples are replaced by one `rusquitto.config.toml` that lists every property
  with its default and a concise one-line comment.

## [0.5.0] - 2026-07-03

### Added

- **Subscription identifiers** — a client may attach a Subscription Identifier to a
  SUBSCRIBE; the broker stores it and echoes it on every matching PUBLISH, so the
  client can tell which subscription produced a message. When several of a client's
  subscriptions match one publish, all their identifiers are delivered. CONNACK now
  advertises subscription-identifier support.

### Fixed

- A delivered PUBLISH no longer carries the publisher's Topic Alias (that property
  is scoped to the publisher's connection); it is stripped on the way out.

## [0.4.0] - 2026-07-03

### Added

- **Cross-shard QoS > 0 backpressure** — a QoS 1/2 publish forwarded to another
  shard is now sent with an awaiting mesh `send_to` instead of the old drop-on-full
  `try_send_to`, so a full mesh link makes the publisher wait (its PUBACK/PUBREC is
  written only after the message is accepted on every shard) rather than silently
  dropping the message. The at-least/exactly-once guarantee now holds *across*
  shards, not just within one. QoS 0 stays fire-and-forget.
- **Shared subscriptions** (`$share/{group}/{filter}`) — members of a group split
  the load: each matching message is delivered to exactly one member, chosen
  round-robin (preferring connected members), while ordinary subscribers still each
  get their own copy. Retained messages are not replayed to shared subscriptions,
  and CONNACK now advertises shared-subscription support. *(Load balancing is
  per-shard; see the README limitations.)*
- **Will Delay Interval** — a Will Message with a non-zero delay is now published
  after `min(will delay, session expiry)` seconds instead of immediately, and is
  cancelled if the client reconnects within the delay.
- **Inbound Receive Maximum** — the broker now enforces the Receive Maximum it
  advertises: a client that exceeds the concurrent unacknowledged QoS 2 quota is
  disconnected with reason `0x93` (Receive Maximum exceeded).
- **Inbound topic aliases** — CONNACK advertises a Topic Alias Maximum and the
  broker resolves aliases on inbound PUBLISH (registering topic↔alias mappings and
  substituting the topic for alias-only publishes); an invalid alias is rejected
  with `0x94`.
- **Hashed passwords** — `[[auth.users]]` accepts a `password_hash` (lowercase-hex
  SHA-256) as an alternative to plaintext `password`, so the config need not store
  the secret in the clear.

## [0.3.0] - 2026-07-03

### Added

- **Cross-shard session resume** — when a client reconnects and the kernel's
  `SO_REUSEPORT` load balancing lands it on a different shard than the one holding
  its session, that session is now migrated to the new shard over the channel
  mesh instead of being treated as fresh. Subscriptions, unacknowledged in-flight
  QoS 1/2 state, and the offline message queue all move with it, so a resume is
  seamless regardless of which core the client hashes to. A Clean Start connect
  discards any session cluster-wide. Single-shard brokers are unaffected (there
  are no peers to migrate from).

## [0.2.0] - 2026-07-03

The broker grew from a basic pub/sub engine into a hardened MQTT 5 broker.
All changes are additive; there are no breaking changes to existing behavior.

### Added

- **Persistent sessions & expiry** — honors the Session Expiry Interval: a
  disconnect suspends the session (keeping subscriptions), a reconnect with the
  same Client ID resumes it (CONNACK `session_present`), QoS > 0 messages
  published while offline are queued and flushed on resume, and unacknowledged
  in-flight QoS 1/2 messages are retransmitted with the DUP flag. Session
  takeover on Client ID reuse is handled.
- **Will messages** — published on abnormal disconnect, suppressed on a normal
  DISCONNECT, and never fired for a connection displaced by a takeover.
- **CONNECT/CONNACK capability negotiation** — CONNACK advertises the server's
  capabilities; the client's Receive Maximum (windowed outbound in-flight limit)
  and Maximum Packet Size (oversized outbound publishes dropped) are enforced.
- **Authentication** — optional username/password via the `[auth]` config, with
  the proper CONNACK reason codes on failure.
- **Topic ACL** — per-user `publish` / `subscribe` topic-filter allow-lists.
- **`$SYS` metrics** — retained `$SYS/broker/...` topics (uptime, clients,
  messages, bytes) published on a configurable interval (`[sys]`).
- **Graceful shutdown** — on SIGTERM/SIGINT the broker stops accepting, sends
  connected clients a `ServerShuttingDown` DISCONNECT, suspends their sessions,
  flushes logs, and exits cleanly.
- **Subscription options** — No Local, Retain As Published, and Retain Handling.

## [0.1.0] - 2026-06-30

### Added

- Initial release: thread-per-core MQTT 5 broker on glommio (io_uring,
  `SO_REUSEPORT`). CONNECT/CONNACK, PUBLISH at QoS 0/1/2 (in and out),
  SUBSCRIBE/UNSUBSCRIBE, PINGREQ/PINGRESP, DISCONNECT; topic-trie wildcard
  matching (`+` / `#`); retained messages; cross-shard routing over a glommio
  channel mesh; structured `tracing` logging; and TOML configuration with a CLI.

[1.7.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.7.0

[1.6.5]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.6.5

[1.6.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.6.0

[1.5.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.5.0

[1.4.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.4.0

[1.3.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.3.0

[1.2.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.2.0

[1.1.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.1.0

[1.0.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v1.0.0

[0.6.1]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.6.1

[0.6.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.6.0

[0.5.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.5.0

[0.4.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.4.0

[0.3.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.3.0

[0.2.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.2.0

[0.1.0]: https://github.com/iamaliybi/rusquitto/releases/tag/v0.1.0
