# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, …) → [scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

## 1. Mosquitto-parity program — the flagship remaining item: dispatcher mode

![priority](https://img.shields.io/badge/priority-high-red)
![risk](https://img.shields.io/badge/risk-high-red)
![status](https://img.shields.io/badge/status-A0%20study%20done%20·%20A1%2FA2%20open-blue)

**Scoreboard after v2.1.0** (measured 2026-07-07, same box, matched harnesses vs
Mosquitto 2.0.18 `set_tcp_nodelay true`). What v2.1.0 fixed and what remains:

| Gap                         | v2.0.0        | v2.1.0            | mosq   | status                                     |
|-----------------------------|---------------|-------------------|--------|--------------------------------------------|
| Empty-broker RSS (1 shard)  | 17.6 MiB      | **8.1 MiB**       | 7.6    | **CLOSED** — io_memory 10 MiB→512 KiB      |
| Empty-broker RSS (4 shard)  | 51.7 MiB      | **13.1 MiB**      | n/a    | **CLOSED** — ~1.6 MiB/shard (was ~11)      |
| Single-message RTT p50      | 37 µs         | **27 µs** (spin)  | 32     | **CLOSED** — `spin_before_park` beats mosq |
| Live (unparked) conn heap   | ~4.6 KiB      | ~4.6 KiB          | ~1.2   | open — task-per-conn model (→ dispatcher)  |
| Active conn under traffic   | ~7.6 KiB      | ~7.6 KiB          | ~1.2   | open — per-conn buffers (→ dispatcher)     |
| Saturating QoS 1 per core   | 79.7k msg/s   | 79.4k             | 85.5k  | open (−7%) — runtime wake floor amortized  |

Two of the five constants closed in v2.1.0 with zero throughput regression. The
remaining three — live/active per-connection memory and the per-core saturating
deficit — share **one root cause**: the task-per-connection execution model. The
protocol engine itself is already Mosquitto-lean (~0.7 KiB state); the cost is the
~2.3 KiB async-task future + ~1.7 KiB glommio task &amp; io_uring source per live
connection. Closing all three at once is Workstream A.

### Workstream A — dispatcher mode (loop-per-shard data plane)

**A0 — design study: COMPLETE (progress.md Phase 16).** Decision: **escalate-per-
connection**, reusing the v2.0.0 parking machinery wholesale. A connection lives
as a state struct on the shard's readiness ring (like a parked conn today), but
the shard dispatcher handles its *simple* ops inline on readiness — PINGREQ,
QoS 0/1 PUBLISH with socket-buffer room, PUBACK/PUBREC bookkeeping — with **no
per-connection task and no per-connection reactor source**. A connection escalates
to a spawned task (today's `Connection` stack, verbatim) only for blocking ops:
window-full backpressure, throttle sleep, partial-frame reassembly, SUBSCRIBE with
retained replay, CONNECT/auth, and all of TLS/WS. On completion it returns to the
ring — the park→unpark transition generalized from idle-only to every-idle-moment.
Sized from `alloc_probe`: eliminates the ~2.3 KiB future + ~1.7 KiB task/source →
**live conn ≈ 1.0–1.3 KiB, Mosquitto territory**, and removes task-spawn/wake from
the hot path (the −7% saturating gap and the active-buffer marginal go with it,
since inline handling delivers into a shared per-shard scratch buffer, not
per-connection ones).

- [ ] **A1 — prototype + gate.** Build the inline dispatch path for the simple ops
      behind a `[dispatcher] enabled` flag; prove the live/active memory floor and
      the saturating-QoS-1 CPU on the real benchmark battery **before** making it
      the default. This is the "prove on a prototype" gate my own notes demanded.
- [ ] **A2 — multishot ingress.** With connections on our ring, replace re-armed
      oneshot polls with `IORING_OP_RECV` multishot (data in the CQE, no re-arm, no
      separate recv syscall) and batch replies per reap — where io_uring can *beat*
      epoll's syscall count per message, not just match it.
- [ ] **A3 — migrate QoS 2 + will/keep-alive** into dispatcher state; battery +
      full integration suite green throughout; flip the default.

### Workstream B — runtime wake floor (SHIPPED in v2.1.0, follow-ups optional)

- [x] **`[runtime] spin_before_park_us`** knob — busy-poll before parking; measured
      RTT p50 37→27 µs (beats Mosquitto's 32). Off by default (idle-CPU trade).
- [ ] Optional: `preempt_timer` / `ring_depth` tuning — minor; measure per-
      `io_uring_enter` cost with `perf` (not installable on the WSL box yet) first.

### Workstream C — active-traffic marginal memory (ATTRIBUTED)

- [x] Attributed: the +1.3 KiB active marginal is the two per-connection `BytesMut`
      buffers (read + coalesced-write) that grow to `read_chunk` under traffic and
      are retained below `BUFFER_RETAIN_MAX`. Shrinking them more aggressively trades
      against throughput (re-growth). **Correct close is dispatcher mode** (A), which
      uses a shared per-shard scratch buffer — folded into Workstream A, not pursued
      standalone (the risk/reward of buffer-thrashing a live connection is wrong).

### Workstream D — empty-broker baseline (SHIPPED in v2.1.0)

- [x] **`[runtime] io_memory_kib`** (default 512, was glommio's 10 MiB). Baseline
      17.6→8.1 MiB (1 shard, Mosquitto parity), 51.7→13.1 (4 shard). Network path
      never used the pool; persistence DMA falls back to heap when exhausted. Bonus:
      the small pinned pool fixes multi-shard `ENOMEM` on tight-`RLIMIT_MEMLOCK` hosts.

**Gates for every A phase**: `wake_probe` floor, 1-conn ping-pong RTT + CPU/msg,
`stresser` saturating QoS 1 (1 + 3 shards), `active_mem` RSS vs Mosquitto,
`alloc_probe` live/parked, adversarial battery 12/12, full test suite — a
regression in any dimension blocks the phase. Shared-nothing (no cross-core state,
mesh-only) is non-negotiable in every design.
