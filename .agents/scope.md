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
- **The per-connection memory & CPU costs are bounded by glommio 0.9 — accepted,
  not open work.** Three measured deficits vs Mosquitto share one root and one
  verdict: **live (unparked) connection heap** (~4.8 KiB vs ~1.2), **active-connection
  memory under traffic** (~7.6 KiB vs ~1.2), and **saturating QoS 1 per core**
  (~80k vs ~87k msg/s, −7%). All three are the task-per-connection execution model
  plus glommio's io_uring per-wake floor.

  The dispatcher-mode program (progress.md Phases 14–18) prototyped the fix — serve
  active connections off a per-shard raw io_uring ring, dropping the per-connection
  task (~2.3 KiB future) and reactor source (~1.7 KiB) — and **proved by measurement
  it is not viable on this runtime**: a connection woken through the raw ring costs
  **p50 3.1 ms / p90 9.8 ms per wake vs 0.3 ms on glommio's live reactor** (three
  orders of magnitude over the 27–37 µs active RTT). Root cause: glommio delivers
  microsecond-latency readiness *only* for its own per-connection `Source` — the very
  memory we would remove — and cannot efficiently `await` a foreign ring (`yolo_recv`
  is `recv(2)`, `ENOTSOCK` on an eventfd), so a raw ring can only be timer-tick-polled
  (ms) or core-burn-spun. **Cheap memory and low latency are the same mechanism on
  glommio 0.9.** Likewise, Phase 17 measured that runtime tuning (a `spin_before_park`
  sweep) does not move the saturating/CPU numbers — spin only helps single-message
  latency, where the reactor parks between messages; under load it never parks.

  Parking already reclaims the **idle** case (0.68 KiB/conn, below Mosquitto's floor)
  — latency-tolerant by definition — which is where the connection-density win lives.
  For *active* connections there is no cheap-memory + low-latency point on this
  runtime. Options that could revisit it, none scheduled and none reaching parity: a
  **different runtime** exposing multiplexed reactor access (the only path to true
  parity, a major change); a **spin-mode** dispatcher (core-burn, sane only on a
  dedicated already-saturated core); or a **`FuturesUnordered`** shared-task multiplex
  (removes the per-connection task futures but keeps the sources — a partial win,
  ~7.6 → ~5 KiB, at real complexity/fairness cost). Accepted as an architectural
  bound of the runtime; not tracked as open work.
