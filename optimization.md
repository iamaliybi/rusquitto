# Optimization Log

A complete record of every performance, CPU, and memory optimization attempted on
rusquitto — what was analyzed, what shipped, and (just as important) what was tried
and **discarded, reverted, or proven non-viable**, with the reasoning in each case.

This is a companion to `.agents/progress.md` (full build history) and `.agents/scope.md`
(settled architectural bounds). It exists so that no future effort re-walks a dead end
expecting a different result.

## Guiding principle

Every optimization here was **gated on measurement**. The recurring discipline: build
a probe that isolates the cost, measure a baseline, apply the change, measure the
delta, and ship **only if the win is real** — separating *"correct and cleaner"*
(ship-worthy on its own) from *"faster"* (a claim that must be proven). Several
attempts below were fully implemented, measured to be no better, and reverted. That is
the process working, not failing.

---

## Measurement methodology & tooling

The tools built along the way, because most wrong turns came from measuring the wrong
thing:

- **`examples/alloc_probe.rs`** — a histogram global allocator wrapping an in-process
  broker. Measures *idle heap per connection* by allocation size class. This is what
  revealed that idle cost was dominated by a single large task-future allocation, not
  by buffers.
- **`examples/wake_probe.rs`** — a bare glommio echo loop with no MQTT layer. Isolates
  the **runtime's per-wake floor** (~30 µs RTT / 21.5 µs CPU per wake) from
  application cost, so the MQTT engine's marginal cost (~5 µs CPU over the floor)
  could be measured honestly.
- **`/proc/<pid>/stat` utime+stime around N operations** — CPU-per-message. The
  decisive signal throughout was **CPU/msg ≈ wall/msg**: when the broker is never
  idle, the cost is per-wake work, not waiting.
- **Wide fan-out benchmark** (`stress`-style: 1000 subscribers on one topic, publisher
  hammering QoS 0, shard CPU-saturated) — exercises the per-subscriber delivery path,
  which the throughput harness (publishes to no-subscriber topics) does not.
- **`stress/soak.py`** — leak-detection soak harness (asserts RSS stays flat over many
  connect/disconnect cycles).

**Hard-won measurement lessons:**

1. **A per-message log event is a hot-path cost.** Every early benchmark carried a
   `debug!` per PUBLISH under the default `info,rusquitto=debug` filter — formatted and
   dispatched on the shard thread for *every message* = **~38 µs/msg of CPU tax** that
   Mosquitto never paid. This silently poisoned *every* published number. Fix: demote
   to `trace!`; pin `logging.level = "error"` in all bench configs. Always grep for
   `debug!` in per-message paths before benchmarking.
2. **Source-level slot removal does not shrink an async state machine.** `rustc`
   conservatively reserves stack slots for everything that lives across an `.await`;
   deleting a local at the source (even a 208-byte `Publish`) leaves the future layout
   unchanged. Shrinking a future requires **structural** changes (boxing across a
   plain-fn seam), not deleting variables.
3. **Benchmark at the widest / most-favorable case.** If an optimization can't show a
   win where it should be maximal, it shows nothing anywhere.

---

## Memory optimizations

### ✅ Shipped

| # | Change | Result | Reasoning |
|---|--------|--------|-----------|
| M1 | **Unbounded mailbox + length guard** (v1.4.0) | 320 KiB → ~0 virtual/conn | glommio's *bounded* `local_channel` pre-allocates its ring: `MAILBOX_CAPACITY 8192 × 40 B Delivery` = **320 KiB virtual per connection**, resident after any burst and never returned. Switched to `new_unbounded()` (allocates nothing when idle) with a `MAILBOX_LIMIT = 256` drop-on-full guard via `LocalSender::len()` — same DoS bound, none of the pre-allocation. |
| M2 | **Lazy adaptive read buffer** (v1.4.0) | idle read buffer 0 B | `initial_read_buffer` default → 0; grow a 512 B–8 KiB chunk into a `BytesMut` tail only under traffic, truncate back after. An idle connection holds no read buffer. |
| M3 | **Per-connection write coalescing** (v1.4.0) | fewer syscalls, bounded burst memory | One `write_all` per event-loop wakeup (drain-parse → drain-mailbox → flush → block), `FLUSH_THRESHOLD 16 KiB`. Cut stalled-subscriber burst memory 86.9 → 31.1 KiB/conn. |
| M4 | **Box transport pipelines via plain-fn seams** (v1.6.0) | task future **13144 B → 600 B**; idle 16.1 → 7.5 KiB/conn | The killer finding from `alloc_probe`: the spawned task future reserved a slot for *every* transport branch's connection future at once. Inlining `Box::pin(fut).await` does **not** help (see dead-end D1). The fix is boxing each transport *pipeline* behind a plain (non-async) function seam (`boxed_run`, `boxed_serve_ws/_tls/_wss`) so only the taken branch's future is materialized. |
| M5 | **`malloc_trim(0)` on the maintenance tick** (v1.6.0) | post-burst RSS 51 → 20.3 MB | The glibc arena holds freed pages after a connection burst. Trim every 30 sweeps on one shard returns them to the kernel. |
| M6 | **`SO_RCVBUF` / `SO_SNDBUF` caps on listeners** (v1.6.0) | bounded per-socket kernel memory | `[server] socket_recv_buffer/socket_send_buffer`, applied pre-listen and inherited by accepted sockets. |
| M7 | **Box rare per-connection data** (v1.6.5) | sessions-table slot ~4× smaller | `Connection.will`, `Session.pending_will`, `Session.snapshot` → `Option<Box<_>>`, `None` while the client is connected (it holds the live copy). A connected session's table slot pays nothing for suspended-only state. |
| M8 | **In-place PUBLISH normalization** (v1.6.5) | one alloc+copy removed per PUBLISH | Normalize the wire retain flag / topic in place in `handle_publish` (QoS/pkid captured before the transform) instead of cloning a new `Publish`. Same semantics, one fewer topic-`String` allocation per message. |
| M9 | **Lazily-boxed topic-alias tables** (v1.8.1) | idle 3.87 → 3.7 KiB/conn | `Option<Box<AliasTables>>`: a non-aliasing connection pays 8 bytes, not two `HashMap`s. |
| M10 | **Connection parking (idle path)** (v2.0.0) | **idle 3.8 → 0.68 KiB/conn** (below Mosquitto's ~0.76 floor) | The big one. After `idle_grace_secs`, an idle plain-TCP connection's glommio task **and** io_uring read `Source` are torn down; only the raw fd (held on a per-shard raw `io_uring` `POLL_ADD` ring) and a boxed `ResumeState` remain. This is the *only* mechanism that closed the 5× idle-density gap — see dead-ends D2/D3 for why the alternatives couldn't. |
| M11 | **`io_memory_kib` runtime knob** (v2.1.0) | empty-broker RSS **17.5 → 8.1 MiB** (1 shard, = Mosquitto parity); 51.7 → 13.1 (4 shards) | Root-caused the empty-broker baseline gap: glommio pins **10 MiB of io_uring registered buffers per executor** at startup (`IORING_REGISTER_BUFFERS`, faulted resident). The network fast path uses `yolo_recv/send` (plain syscalls), *not* the pool — only DMA file I/O draws from it, and it falls back to the heap when exhausted. So shrinking to a 512 KiB default is safe (64 KiB floor). **Bonus:** the 10 MiB × N pinned pool was the WSL multi-shard `io_uring` `ENOMEM` (`RLIMIT_MEMLOCK`) gotcha — 512 KiB × N now boots clean. |
| M12 | **Bound topic-trie / interner / shared-cursor growth** (v2.1.2) | closes three unbounded-growth vectors | Also a security fix (memory-DoS) — see `security.md`. The trie now prunes dead nodes on removal; a periodic GC reclaims interned segments (`strong_count == 1`) and stale shared-group cursors. |
| M13 | **`route()` keys fan-out maps by borrowed `&str`** (v2.2.1) | one `String` alloc/subscriber/message removed (~15M/s at 1000-wide) | Shipped as an **allocation reduction and borrow-structure cleanup with an explicit *no performance claim*** — see the honest write-up in dead-end D6. It is correct and clean, which earned the ship; it is not faster, which is stated plainly. |

### ❌ Dead ends & reverted memory work

- **D1 — Inline `Box::pin(fut).await` to shrink the task future.** Doesn't work:
  statement temporaries and moved-from bindings (`TlsStream`/`WsStream`) still occupy
  await-spanning slots, so the future's size is unchanged. Only boxing behind a
  **plain-fn seam** (M4) actually shrinks it.
- **D2 — Slab-allocate tasks / shrink buffers to reach sub-4 KiB idle** (investigated
  v1.8.1). Dead end: `alloc_probe` decomposition showed idle cost was ~1.6 KiB (our
  boxed connection future) + ~1.9 KiB (glommio task + io_uring `Source`, **not** a
  buffer — `TcpStream` is `NonBuffered`, confirmed in glommio source) + ~0.3 KiB
  smalls. Buffers were **already lazy / zero-idle** (M2). There was nothing left to
  shrink at the source level; only removing the task+Source (i.e. parking, M10) could
  close the gap.
- **D3 — Source-level struct-slot elimination for sub-4 KiB** (v1.6.5). Removed a
  208 B `Publish` slot at the source; the future layout was **byte-for-byte
  unchanged**. Confirmed the rule that `rustc` reserves await-spanning slots
  conservatively. The ignored test `probe_future_tree` documents the breakdown
  (`run()` 3312 → `process_packet` 2440 → `handle_publish` 1648 → `fan_out` 1192). A
  true hand-rolled hot-loop state machine would be needed — deemed not worth the
  complexity for the remaining bytes.
- **D4 — Shrink the active-traffic marginal buffer (+1.3 KiB/conn)** (v2.1.0,
  Workstream C). Attributed to the two per-connection `BytesMut` buffers (read +
  coalesced-write) growing under traffic and retained below `BUFFER_RETAIN_MAX`.
  Shrinking harder just trades against throughput (constant re-growth). The correct
  fix is a shared per-shard scratch buffer — which only the dispatcher rewrite would
  enable, and that proved non-viable (D5). **Not pursued standalone.**
- **D6 — `route()` per-subscriber allocation as a *throughput* win** (v2.2.1). The
  refactor in M13 was expected to speed up wide fan-out by eliminating the
  per-subscriber `client_id` clone. **It does not.** Measured on the widest,
  most-allocation-favorable case (1000 subscribers, ~15M allocs/s eliminated):
  baseline **64.48 µs/publish** vs refactored **64.0** (64.9 / 64.5 / 62.6) —
  identical within ±3.5% run-to-run noise. The per-delivery cost (64.9 ns) is
  dominated by the mailbox `try_send` + `Delivery` construction + `Rc` clone, not the
  short-string allocation (glibc's tcache absorbs it below the noise floor). It was
  **reverted** first (honoring the "ship only on a proven win" gate), then re-applied
  and shipped **as a cleanup only, with no perf claim**. Lesson: an allocation that
  scales with fan-out can still be immaterial when the per-op cost is channel/queue-
  bound.

### ⚠️ Accepted bound (not open work)

- **Active-connection memory (~7.6 KiB vs Mosquitto's ~1.2).** Bounded by glommio 0.9
  — see the dispatcher section below and `.agents/scope.md`.

---

## Throughput & CPU optimizations

### ✅ Shipped

| # | Change | Result | Reasoning |
|---|--------|--------|-----------|
| T1 | **Remove the per-message `debug!` logging tax** (v2.0.0, Phase 15) | **~38 µs/msg CPU removed**; CPU/msg 64.5 → 26.5 µs | The single most impactful CPU finding. See methodology lesson #1. |
| T2 | **Boxing seams also raised throughput** (v1.6.5) | **+15% msg/s** (49.9k → 57.2k A/B) | `fan_out` does `try_send_to` first and only boxes the `send_to` future on `WouldBlock`; per-packet handlers boxed individually. Smaller hot future = better instruction-cache behavior. |
| T3 | **Batch-drain the inbound mesh receiver** (v1.9.0) | 1 wake per forwarded burst, not 1 reschedule/msg | After `recv()` wakes, drain all queued mesh messages via `poll_once(receiver.recv())` without yielding. |
| T4 | **Clone `Publish` once, not twice, on QoS 1/2 delivery** (v1.9.0) | one deep clone removed per delivered message | The retransmit copy is cloned only when the peer's topic-alias max > 0 (aliasing may clear the topic); otherwise the message is *moved* into the in-flight window after a successful write. |
| T5 | **Skip the mesh path on a single-shard broker** (v1.9.1) | one `Rc` clone saved per publish | `fan_out` short-circuits when `mesh_peers() == 0`. |

### ⚠️ Investigated and found already at the floor (no change shipped as a win)

- **Per-core QoS 1/2 ack path** (v1.9.1). Measured single-shard QoS 1 PUBLISH→PUBACK
  RTT p50 = 55 µs / p90 = 76 µs with a **tight tail** (no Nagle artifacts →
  `TCP_NODELAY` already effective). Per-request cost = `mqttbytes` parse + socket
  round-trip, at C-broker parity. Shipped two *genuine-but-perf-neutral* robustness
  changes (explicit `set_nodelay(true)` per accepted socket; the single-shard mesh
  skip T5) and **told the user honestly these are cleanup, not a speed win.** Do not
  re-chase this — it is at the floor.

### ⚠️ Accepted bound (not open work)

- **Saturating per-core QoS 1 throughput (~80k vs Mosquitto's ~87k, −7%)** and
  **CPU/msg (~1.7× Mosquitto).** Both are the amortized cost of one `io_uring_enter`
  per wake under the task-per-connection model. **Runtime tuning cannot touch them:**
  a `spin_before_park` sweep at 0/20/50/100/200 µs left saturating QoS 1 **flat at
  81–82k** (Phase 17) — spin only helps single-message latency, where the reactor
  parks *between* messages; under load it never parks. Only a dispatcher rewrite +
  multishot RECV could reduce the per-wake count, and that proved non-viable (below).

---

## Latency optimizations

### ✅ Shipped

| # | Change | Result | Reasoning |
|---|--------|--------|-----------|
| L1 | **`spin_before_park_us` runtime knob** (v2.1.0) | RTT p50 **37.3 → 27.1 µs** (−27%), **beats Mosquitto's 31.9** | `spin_before_park(Duration)` busy-polls io_uring completions before parking the reactor, removing the park/unpark round-trip from single-message latency. Opt-in (default 0) because spinning burns idle CPU. **Caveat from the glommio source:** silently disabled under `Unbound` placement — only works under `MaxSpread`/`MaxPack` (the default). |
| L2 | **Explicit `TCP_NODELAY`** (v1.9.1) | portable/robust; no change on Linux | Set explicitly per accepted socket rather than relying on listener inheritance. Honest: robustness, not a measured win. |

### ⚠️ Accepted bound (not open work)

- **Cross-shard delivery tax (~2× same-core: 76 vs 40 µs p50).** A publisher and
  subscriber on different shards incur one mandatory cross-thread reactor wake over the
  mesh. This is the *definitional* cost of shared-nothing — removing it needs
  cross-core shared state, which the invariant forbids. The mesh drain already batches;
  the per-message wake itself is inherent. Recorded in `.agents/scope.md`.

---

## The dispatcher-mode program — the big rewrite that was prototyped and rejected

This is the most important negative result in the project. Three of the measured
deficits vs Mosquitto — **live-connection heap** (~4.8 KiB), **active-connection
memory** (~7.6 KiB), and **saturating QoS 1** (−7%) — share one root: the
**task-per-connection execution model** plus glommio's io_uring per-wake floor. The
proposed fix was *dispatcher mode*: serve **active** connections off a per-shard raw
`io_uring` ring (dropping the ~2.3 KiB per-connection task future and ~1.7 KiB reactor
`Source`), handling simple operations inline and escalating to the full `Connection`
stack only for blocking ones — i.e. **generalizing the v2.0.0 parking model from idle
to active.**

- **A0 — design study** (v2.1.0, Phase 16): completed. Chose *escalate-per-connection*
  over *escalate-per-operation*. Sized the win at ~1.0–1.3 KiB per live connection
  (Mosquitto territory). Deliberately **did not ship the rewrite** — the self-imposed
  gate was "prove it on a prototype first."
- **A1 — prototype + measurement** (Phase 18): built behind a flag, then **stopped at
  the gate after measuring it is non-viable on glommio 0.9.** The parking ring reaps
  completions on an adaptive **1–25 ms timer tick** — fine for idle (latency-tolerant)
  connections, catastrophic for active ones. Measured (`/tmp/disp/wakelat.py`): a
  connection woken through the raw ring costs **p50 3.1 ms / p90 9.8 ms** vs **0.3 ms**
  on glommio's live reactor — three orders of magnitude over the 27–37 µs active RTT.

**Root cause (the load-bearing insight): on glommio 0.9, cheap memory and low latency
are the same mechanism.** glommio delivers microsecond-latency readiness *only* for its
own per-connection `Source` — the very ~1.7 KiB we wanted to remove. It cannot
efficiently `await` a foreign ring (`yolo_recv` is `recv(2)`, which returns `ENOTSOCK`
on an eventfd), so a raw ring can only be timer-tick-polled (ms) or core-burn-spun.
Parking works *only* because idle connections tolerate millisecond wakes. There is no
cheap-memory + low-latency point for *active* connections on this runtime.

**Outcome:** reverted the config scaffold (no non-functional flag shipped), **no
release** (the deliverable is the proven finding), and reclassified all three deficits
in `.agents/scope.md` as **accepted architectural bounds of glommio 0.9**. The
`wakelat.py` probe is kept as the evidence and to re-check on a future glommio.

**Options that could revisit it** (none scheduled, none reaching parity): a different
runtime with multiplexed reactor access (the only true-parity path, a major change); a
spin-mode dispatcher (core-burn, sane only on a dedicated already-saturated core); or a
`FuturesUnordered` shared-task multiplex (removes the per-connection task futures but
keeps the sources — a partial ~7.6 → ~5 KiB win at real complexity/fairness cost).

---

## Summary

**What moved the needle:** killing the per-message `debug!` tax (T1), boxing transport
pipelines behind plain-fn seams (M4/T2), the `io_memory` knob (M11), connection parking
for idle density (M10), and `spin_before_park` for latency (L1). These took the broker
from *behind* Mosquitto to *leading it on every Python-harness tier* (QoS 0 2.2×,
QoS 1 +11%, QoS 2 +24%) and to **3.8× Mosquitto's ceiling at 3 shards**, with
empty-broker RSS and idle-connection density at or below Mosquitto's.

**What didn't, and why it matters that we know:** the dispatcher rewrite (bounded by the
runtime), sub-4 KiB idle via source-level slot removal (bounded by `rustc`), the active
marginal buffer (needs the dispatcher), and the `route()` allocation as a speed win
(channel-bound, not alloc-bound). Each was measured, not assumed — which is why they are
recorded as bounds rather than left as tempting open TODOs.
