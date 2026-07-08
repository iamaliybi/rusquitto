# Rusquitto — Scope & Product Decisions

Settled decisions about what this broker **is** and **is deliberately not**. These
are not backlog and not up for casual reversal — treat them as constraints when
proposing work. Open work lives in [next-steps.md](next-steps.md); build history
in [progress.md](progress.md).

## Do

- **MQTT 5.0, and only 5.0.** The parser is `mqttbytes::v5` exclusively. This is a
  deliberate scope choice, not a gap to close.
- **Configure everything via one TOML file.** Auth, ACLs, TLS/mTLS, persistence,
  overload handling, and telemetry are all built in and driven by config.
- **Preserve the thread-per-core, shared-nothing invariant.** No `Mutex`/`RwLock`,
  no `std::thread`, nothing crossing shards except over the glommio mesh. This is
  mechanically enforced (`clippy.toml` + the pre-commit hook).

## Don't

- **No MQTT 3.1.1 / 3.1 support.** A legacy client is mis-framed and dropped, by
  design. The v1.7.0 audit flagged this as the largest *compliance* gap; we accept
  it as a *scope* choice. Do not add v3.
- **No plugin / extension system.** No plugin ABI, no scripting hooks, no
  bridge-plugin mechanism. Extend the broker in-tree, configured via TOML.
- **No multi-machine clustering here.** Owned by a separate plan and intentionally
  out of this repo's backlog. (The mesh's shared-subscription membership
  replication and deterministic pick were built to survive that step, but the step
  itself is not tracked here.)
- **The cross-shard delivery tax is accepted, not a bug.** A publisher and
  subscriber on different shards incur one cross-thread reactor wake over the mesh
  (~2× same-core delivery latency, measured 76 vs 40 µs p50). This is the
  definitional cost of shared-nothing — removing it needs cross-core shared state,
  which the invariant above forbids. The mesh drain already batches (`poll_once`);
  the per-message wake itself is inherent. Not tracked as open work.
- **Active-connection memory (~6× Mosquitto) is bounded by glommio 0.9, not a
  quick fix.** Prototyped and measured (progress.md Phase 18): serving *active*
  connections off a per-shard raw io_uring ring — the "dispatcher mode" that would
  drop the per-connection task + source — imposes ms-scale per-message latency
  (measured p50 3.1 ms via the ring vs 0.3 ms on glommio's live reactor), because
  glommio delivers low-latency readiness only for its own per-connection `Source`
  (the very memory we'd remove) and cannot efficiently await a foreign ring.
  Cheap-memory and low-latency are the same mechanism on this runtime. Parking wins
  for *idle* connections (latency-tolerant); active connections have no such point.
  Accepted as an architectural bound, with future options (different runtime,
  spin-mode, `FuturesUnordered` partial) noted in next-steps but not scheduled.
