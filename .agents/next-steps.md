# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, …) → [scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

## 1. Mosquitto-parity program — close the remaining per-connection constants without breaking thread-per-core

![priority](https://img.shields.io/badge/priority-high-red)
![risk](https://img.shields.io/badge/risk-high-red)
![status](https://img.shields.io/badge/status-open%20(design%20study%20first)-blue)

**The scoreboard** (all measured 2026-07-07, same box, identical harnesses,
Mosquitto 2.0.18 with `set_tcp_nodelay true` — details in progress.md Phases
14–15). Where v2.0.0 already wins: parked idle **0.68 vs 1.18 KiB/conn** live
heap; every 200-conn throughput tier (QoS 0 2.2×, QoS 1 +11%, QoS 2 +24%);
3-shard saturating QoS 1 **3.8×** Mosquitto's ceiling. What still trails —
every line a *constant*, not a scaling property:

| Gap | rusquitto | mosquitto | root cause |
|---|---|---|---|
| Live (unparked) conn, heap | ~4.6 KiB | ~1.2 KiB | task future ~2.3 KiB + glommio task/read-Source ~1.7 KiB + state ~0.7 |
| Active conn under traffic, RSS | ~8.0 KiB | ~1.2 KiB | above + warm buffers/mailbox (+1.3 marginal vs their +0.02) |
| Per-wake CPU (ping-pong) | 26.5 µs (floor 21.5) | 15.5 µs | glommio io_uring park/unpark + task wake vs `epoll_wait` |
| Saturating QoS 1 per core | 77.6k msg/s | 87.1k | the amortized remainder of the wake floor |
| Empty-broker RSS | ~17.7 MiB | ~7.5 MiB | per-shard rings/executor + logging infra |

Mosquitto's constants come from one hand-tuned C event loop over plain per-fd
structs. The equivalent *within* thread-per-core/shared-nothing is not
task-per-connection — it is **loop-per-shard**: the invariant is "no state
crosses cores except the mesh", not "every connection owns a glommio task".
The parking ring (v2.0.0) already built half of that: a per-shard raw io_uring
registry holding fd + state with generation-tagged wakes. The endgame is to
promote it from an idle path to the *data plane*.

### Workstream A — dispatcher mode (the flagship; needs a design study first)

Serve plain-TCP connections as **state structs on the shard's readiness ring**
for their whole life, not just while parked: readiness wakes the shard
dispatcher, which parses + handles + replies **inline** (no task spawn, no
per-connection future, no per-connection reactor Source). The per-connection
task becomes the *escalation* path only (QoS backpressure waits, throttle
sleeps, TLS/WS which keep today's stack). Sized from the alloc_probe
decomposition: eliminates the ~2.3 KiB future + ~1.7 KiB task/Source →
**live conn ≈ 1.0–1.3 KiB, Mosquitto territory**, and removes task-spawn/wake
from the hot path (the −11% saturating gap and much of the +1.3 KiB active
marginal go with it).

- [ ] **A0 — design study**: inline-dispatch state machine for the simple ops
      (PINGREQ, QoS 0/1 PUBLISH with room in the socket buffer, PUBACK/PUBREC
      bookkeeping) with explicit escalation rules to a spawned task (window
      full, partial frame, throttle, SUBSCRIBE with retained replay, CONNECT).
      Decide: escalate-per-operation (dispatcher drives everything, blocking
      ops queue a continuation) vs escalate-per-connection (conn temporarily
      gets a task, then returns to the ring — the park/unpark machinery
      generalized). The second reuses v2.0.0 wholesale and is the likely
      winner. Prove memory + CPU on a prototype before committing.
- [ ] **A1 — multishot ingress**: with connections on our own ring, replace
      re-armed oneshot polls with `IORING_OP_RECV` multishot (data arrives in
      the CQE, no re-arm, no separate recv syscall) and batch replies per
      reap into single submits — this is where io_uring can *beat* epoll's
      syscall count per message, not just match it.
- [ ] **A2 — migrate QoS 2 + will/keep-alive bookkeeping** into dispatcher
      state; adversarial battery + full integration suite green throughout.

### Workstream B — runtime wake floor (tactical, independent of A)

`examples/wake_probe.rs` (bare glommio echo loop) puts the floor at ~30 µs RTT
/ 21.5 µs CPU per wake; our MQTT layer adds only ~5 µs on top.

- [ ] **`[runtime] spin_before_park`** config knob (off by default): spinning a
      few µs before parking removes the park/unpark round trip for
      latency-sensitive deployments; size the win with `wake_probe` first.
- [ ] Reactor/ring tuning (`LocalExecutorBuilder` preempt timer, `io_memory`,
      ring depth) — measure per-`io_uring_enter` cost on this kernel.
- [ ] Re-profile with `perf`/`strace` when installable, to attribute the floor
      (enter syscall vs task-wake bookkeeping vs timer maintenance) before
      touching anything.

### Workstream C — active-traffic marginal memory (+1.3 KiB vs +0.02)

- [ ] Attribute it precisely (mailbox channel allocation, BytesMut growth
      curve, in-flight table) with an alloc_probe variant that runs traffic;
      then shrink the biggest term. Candidate: allocation-free mailbox for the
      1-element common case (deliver directly into the connection's outbound
      when it is at the block point — dispatcher mode gets this for free).

### Workstream D — empty-broker baseline (~17.7 vs ~7.5 MiB)

- [ ] Decompose (glommio ring buffers per shard × io_memory, logging
      appenders, TLS tables) and trim what's free; matters only for tiny
      hosts, so priority-low within the program.

**Gates for every phase**: `wake_probe` floor, 1-conn ping-pong RTT + CPU/msg,
`stresser` saturating QoS 1 (1 + 3 shards), `active_mem`-style RSS under
matched load vs Mosquitto, `alloc_probe` live/parked, adversarial battery
12/12, full test suite — regressions in any dimension block the phase. The
shared-nothing invariant (no cross-core state, mesh-only) is non-negotiable in
every design.
