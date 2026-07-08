# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, …) → [scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

## 0. Optimization backlog — `route()` per-subscriber allocation (documented, not scheduled)

![priority](https://img.shields.io/badge/priority-low-lightgrey)
![risk](https://img.shields.io/badge/risk-medium-yellow)
![status](https://img.shields.io/badge/status-documented-blue)

The v2.1.2 audit's performance pass found the hot path already tight (Rc fan-out,
in-place PUBLISH normalization, single-shard mesh skip, interner off the publish
path, coalesced writes, boxed cold handlers — all optimal). The one remaining win:
`broker/shard/routing.rs::route` clones each matching subscriber's `client_id`
into the per-publish `best`/`groups` maps (`sub.client_id.clone()`), a short-string
heap allocation **per subscriber per message** that scales with fan-out width.

- [ ] The clone exists only to end the `self.trie` borrow before touching
      `self.sessions`. Removing it needs a **disjoint-field-borrow restructure** of
      `route` + `deliver_to` (destructure `self` into `{trie, sessions, shared_cursor,
      shared_remote, unpark_tx, wal}`, key `best` by `&str` borrowed from the trie,
      and make `deliver_to` a free fn over those fields). Also reuse `best` as a
      scratch field to drop its per-publish `HashMap` allocation.
- **Deliberately not done in v2.1.2**: this is the core delivery path (QoS downgrade,
  No-Local, sub_id union, the deterministic shared-sub global pick, offline queue,
  parking wake, WAL). Restructuring its borrow topology to save a short-string alloc
  is not worth the regression risk **without a wide-fan-out benchmark proving the
  win** — the throughput harness publishes to no-subscriber topics, so it doesn't
  exercise this. Gate on such a benchmark + the routing unit tests + integration
  QoS/shared-sub suite before attempting. (Higher-risk sibling: `send_publish`
  clones the whole `Publish` per delivery — a `write_publish` helper encoding from
  the borrowed `Rc` would cut the QoS-0 topic alloc but duplicates wire-encoding;
  only with the same benchmark gate.)

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

### Workstream A — dispatcher mode: PROTOTYPED, found NON-VIABLE on glommio 0.9

**A0 — design study (progress.md Phase 16)** proposed serving active connections as
state structs on the shard's readiness ring (parking generalized from idle to every
moment), sized to bring a live connection to ~1.0–1.3 KiB by dropping the ~2.3 KiB
task future + ~1.7 KiB glommio source.

**A1 — prototype: STOPPED at the viability gate (progress.md Phase 18, 2026-07-08).**
The A0 assumption is **wrong for active connections**, proven by measurement. The
raw per-shard ring reaps completions on an adaptive **1–25 ms timer tick** (fine for
*idle* parked connections — latency-tolerant by definition). Serving *active*
connections through it was measured at **p50 3.1 ms / p90 9.8 ms per wake vs 0.3 ms
on glommio's live reactor** (`/tmp/disp/wakelat.py`) — ~10× worse, and three orders
of magnitude above the 27–37 µs active connections get today.

Root cause — a real glommio 0.9 limit: **the efficient, low-latency I/O wait and the
per-connection memory are the same thing.** glommio delivers µs-latency readiness
only for *its own* per-connection `Source` (the ~1.7 KiB we wanted to remove); a
foreign io_uring can only be polled on a timer tick (ms latency) or by burning a core
spinning (established earlier: glommio cannot `await` a foreign eventfd —
`yolo_recv` is `recv(2)`, `ENOTSOCK`). Parking sidesteps this *only* because idle
connections tolerate ms wakes. There is no cheap-memory + low-latency point for
*active* connections on this runtime.

**Conclusion: active-connection memory (audit item 1) is architecturally bounded on
glommio 0.9**, in the same class as the cross-shard tax — not a quick fix. Reclassified
in the audit ledger. Options, none a clean win, none scheduled:

- **Different runtime / reactor access** — a runtime that lets one task await
  readiness on many fds (epoll-style) would remove the per-connection task future
  while keeping efficient waits. Largest change; out of scope for now.
- **Spin-mode dispatcher** — a core-burning poll loop is acceptable *only* on a
  dedicated core already saturated with active work; wrong default for mostly-idle
  fleets (where parking already wins). Niche opt-in at best.
- **Shared-task `FuturesUnordered` multiplex** — one task awaiting all connections'
  reads removes the N task futures but keeps N glommio sources: a *partial* memory
  win (~7.3 → ~4–5 KiB, not to Mosquitto's 1.2) at real complexity/fairness cost.
  Best risk-adjusted option if the memory item is ever prioritized; still not parity.

The audit's **CPU-per-message and −7% saturating** (items 2/3) share this root and
are likewise bounded — no knob or safe rewrite closes them on glommio 0.9.

### Workstream B — runtime wake floor (spin SHIPPED; the rest needs dispatcher mode)

- [x] **`[runtime] spin_before_park_us`** knob — busy-poll before parking; measured
      RTT p50 37→27 µs (beats Mosquitto's 32). Off by default (idle-CPU trade).
- [x] **MEASURED (2026-07-08): tuning cannot close the CPU/throughput deficits.**
      `spin_before_park` sweep 0/20/50/100/200 µs left saturating QoS 1 flat at
      81–82k (spin only helps single-message *latency*, where the reactor parks
      between messages — under load it never parks, so there is nothing to spin
      past). Conclusion: the audit's **CPU-per-message (25.5 vs 15 µs)** and
      **saturating −7%** are the amortized per-`io_uring_enter` cost under load and
      are **only** reducible by cutting the syscall/wake count per message — i.e.
      connections on our own ring + `IORING_OP_RECV` multishot, which is
      **Workstream A2**. Not a tuning problem; do not chase it with knobs.

### Cross-shard delivery tax — STRUCTURAL, not a defect (documented decision)

The audit's "cross-shard delivery ~2× same-core (76 vs 40 µs)" is the definitional
cost of shared-nothing: a publisher on shard A reaching a subscriber on shard B
requires exactly one cross-thread reactor wake over the mesh. It cannot be removed
without cross-core shared state, which is the one invariant the project will not
break. Marginal mitigations (the mesh drain already batches with `poll_once`) are
in place; the per-message cross-thread wake itself is inherent. Recorded in
[scope.md](scope.md) as an accepted trade, not tracked as open work.

### Hardening — parser fuzzing (SHIPPED)

- [x] **`proptest` fuzz harness** (`server/connection/tests.rs::fuzz`), in
      `cargo test`: `parse_packet` + full connected/pre-connect dispatch over an
      adversarial input distribution (random / plausible-malformed / concatenated).
      Validated deep (50 k parser + 3 k dispatch cases), no findings. Closes the
      TESTING.md gap. Deeper `cargo-fuzz` libFuzzer target over `parse_packet`
      remains an optional follow-up.

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
