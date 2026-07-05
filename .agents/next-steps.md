# Rusquitto — What's Next

The Phase 3 hardening roadmap shipped in full (through **v1.7.0**, the
architectural refactor). The full build history, with design decisions and
gotchas, is in [progress.md](progress.md); the feature matrix is in
[overview.md](overview.md); the v1.7.0 external audit + Mosquitto benchmark is
the reference for the weaknesses below.

## Scope — what this broker is (and is deliberately not)

These are settled product decisions, not backlog:

- **MQTT 5.0 only, permanently.** The parser is `mqttbytes::v5` exclusively.
  There is **no** MQTT 3.1.1 / 3.1 support and none is planned — a legacy client
  is mis-framed and dropped, by design. The audit flagged this as the largest
  *compliance* gap; we accept it as a *scope* choice. Do not add v3.
- **No plugin / extension system.** Auth, ACLs, persistence, and telemetry are
  built in and configured via TOML. We will not add a plugin ABI, scripting
  hooks, or a bridge-plugin mechanism.
- **Multi-machine clustering is out of scope here.** Owned by a separate plan;
  intentionally omitted from this backlog. (The mesh's shared-subscription
  membership replication and deterministic pick were built to survive that step,
  but the step itself is not tracked here.)

## Shipped in v1.8.0

- **Partial-frame stall guard** — closed the 15th adversarial case (a header-only
  truncated CONNECT) and its more dangerous post-CONNECT sibling (a mid-frame
  stall with keep-alive disabled, previously *unbounded*). Any incomplete frame
  must now complete within the handshake window, even when keep-alive is off.
- **Session/queued-message WAL** — a per-shard, append-only, group-committed
  write-ahead log (session upsert/remove + offline-queue growth), replayed over
  the snapshot on startup. Shrinks the crash window from `snapshot_interval` to
  `wal_flush_ms` (default 200 ms). Retained is still snapshot-only.
- **mTLS + certificate hot-reload** — client-certificate verification against a
  configured CA (required or optional); a cert-verified client with no MQTT
  username is authenticated by the certificate alone; the cert/key/CA files
  hot-reload into new handshakes without dropping live connections (per-shard,
  no cross-core coordination).

## Open weaknesses & review targets

None is committed; this is the honest list of where we are weak, in rough value
order.

- **mTLS cert-CN → username ACL mapping** *(deferred from v1.8.0)*. A
  cert-authenticated client with no MQTT username currently gets the *anonymous*
  ACLs. Mapping the certificate's subject CN (or a SAN) to an MQTT identity would
  let `[[auth.users]]` ACLs apply per-device. Needs an X.509 parsing dependency
  (rustls verifies but does not expose the parsed subject) — evaluate
  `x509-parser` against the crate's dependency budget.

From the v1.7.0 audit + benchmark:

1. **Per-connection memory density — the main scalability ceiling.** Idle is
   **3.7 KiB/conn vs Mosquitto's 0.76 KiB** (was 3.87; the topic-alias tables are
   now lazily boxed, `Option<Box<AliasTables>>`). The `alloc_probe` histogram
   decomposes that 3.7 KiB into three buckets:
   - **~1.6 KiB — our boxed connection future** (`sizeof(Connection)` + the
     `event_loop` frame, allocated by `boxed_run`). Partly shrinkable by boxing
     more cold-for-idle fields (the alias boxing shaved ~0.1 KiB; boxing the QoS
     maps + sharing `limits` via `Rc` would reach ~3.5). Diminishing returns.
   - **~1.9 KiB — glommio's per-connection task + io_uring `Source`(s)** for the
     parked read. **Not a buffer** — our `TcpStream` is `NonBuffered`, so option
     "shrink glommio buffers" has nothing to give. Only removable by dropping the
     per-idle task entirely.
   - **~0.3 KiB — smalls** (`client_id`, the mailbox channel node, the span).

   **The 5× gap is architectural**, not a tuning miss: task-per-connection vs.
   Mosquitto's one-event-loop + struct-per-fd. The only lever that closes it is a
   **parked idle path** (evaluated options 1–3 from the audit: slab-tasks and
   buffer-shrink are dead ends; the non-async idle path is the one with headroom).

   **Proven feasible + quantified** (`examples/park_probe.rs`): an idle fd held as
   a 48-B `ParkedConn` on one shared `IORING_OP_POLL_ADD` ring, no per-connection
   task, costs **0.06 KiB heap / 0.08 KiB RSS per connection — a ~46× reduction**,
   an order of magnitude under Mosquitto. The wake path is proven: 2000/2000 fds
   delivered a readiness completion naming their connection, and glommio streams
   expose `IntoRawFd`/`FromRawFd` so the fd hand-off works.

   **Staged build (the real project, ~v1.9.0):**
   - *Phase 1* — per-shard readiness ring: a raw io_uring for parked fds (glommio
     doesn't expose its `POLL_ADD`), driven by one glommio task awaiting the
     ring's eventfd. Plain-TCP only at first (TLS/WS carry mid-stream state).
   - *Phase 2* — park predicate + transition: in `event_loop`, when idle and
     `inflight`/`incoming_qos2`/`pending_outbound`/`inbound` are all empty and no
     partial frame is buffered (the 1.8.0 `partial_since` guard already tracks
     this), serialize into `ParkedConn`, register the fd, and return (task freed).
   - *Phase 3* — unpark: ingress readiness **and** egress (`route`→`deliver_to`
     targeting a parked client) both resurrect a task that rebuilds a `Connection`
     over the fd, drains, and re-parks. Egress-wake is the subtle part.
   - *Phase 4* — parked keep-alive expiry (task-less, via the sweep timer), a
     `$SYS/broker/parked-connections` gauge, and shedding/migration interaction.

   Risk: high (a second io_uring ring cooperating with glommio's reactor, plus the
   egress-wake correctness). Gate each phase on the `alloc_probe`/battery numbers.
2. **Cross-shard mesh reliability under overload.** Mesh control messages are
   best-effort (drop-on-full). Under sustained saturation, shared-subscription
   single-delivery and session migration can transiently double- or
   zero-deliver if a control message is dropped. Replace best-effort control
   with a bounded-reliable channel for membership/handoff (keep best-effort only
   for `$SYS`). Data-plane QoS 1/2 forwards already apply backpressure; this is
   about the *control* plane.
3. **No per-core throughput superiority on the ack-bound path.** Single-shard
   QoS 1 is ~76k msg/s vs Mosquitto's ~83k — per core the mature C event loop is
   marginally faster. Our ~4.6× edge is entirely multicore scaling. On a 1-vCPU
   host, Mosquitto wins. Worth profiling the QoS 1/2 ack path for the last bit of
   per-core parity.
4. **Cross-shard latency tax (~50 µs p50).** A publish that crosses shards pays
   the mesh forward + receiving-shard scheduling: p50 73 µs same-core vs 125 µs
   cross-shard. Inherent to the shared-nothing design (it buys the scaling in
   §3), but topology-aware subscriber placement or a faster mesh wakeup could
   trim it for cross-core pub/sub topologies.

The audit found **no race conditions and no memory leaks** — the shared-nothing
model makes intra-shard data races structurally impossible, and RSS returns to
baseline after churn (periodic `malloc_trim`). The items above are design
trade-offs and coverage gaps, not defects.
