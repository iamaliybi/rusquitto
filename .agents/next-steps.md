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

1. **Per-connection memory density — the main scalability ceiling.** Idle
   footprint is **3.87 KiB/conn vs Mosquitto's 0.76 KiB** (~5×). We took the
   connection state machine itself to ~600 B; the remaining floor is glommio's
   per-connection internals (~1.7 KiB task + stream/source allocations). Going
   lower means pooling/slabbing the async task, shrinking glommio source
   allocations, or a lightweight non-async path for idle connections — i.e.
   changes at or below the glommio boundary. This is the biggest lever on
   how many connections one box holds.
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
