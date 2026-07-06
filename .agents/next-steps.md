# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, …) → [scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

## 1. Close the glommio per-wake floor gap (single-message latency + per-core QoS 1/2)

![priority](https://img.shields.io/badge/priority-medium-yellow)
![risk](https://img.shields.io/badge/risk-medium-yellow)
![status](https://img.shields.io/badge/status-open-blue)

The v2.0.0 performance investigation (progress.md Phase 15) found the QoS 1/2
"gap vs Mosquitto" in the audit was mostly a **hot-path logging artifact** (fixed:
the per-PUBLISH `debug!` cost ~38 µs/msg under the default filter). After the fix,
rusquitto leads Mosquitto on every 200-connection throughput tier and 3-shard
saturating QoS 1 is 3.8× Mosquitto's ceiling — but two *runtime-structural*
residuals remain, both measured and attributed:

- **Per-wake cost.** `examples/wake_probe.rs` (a bare glommio echo loop, no MQTT)
  measures the runtime floor at **~30 µs RTT / ~21.5 µs CPU per wake** vs
  Mosquitto's full-MQTT 32.8 µs RTT / 15.5 µs CPU on epoll. Our MQTT layer adds
  only ~5 µs CPU over that floor — the floor itself (io_uring park/unpark + task
  wake vs `epoll_wait` return) is what keeps single-message latency ~5 µs and
  ping-pong CPU ~11 µs behind. It amortizes under load (batch drains), which is
  why saturating throughput is far less affected.
- **Saturating per-core QoS 1: ~11% behind** (77.6k vs 87.1k msg/s, Rust hammer,
  50 conns, 1 shard) — the amortized remainder of the same floor.

Candidate approaches, in rough order of value/effort:

- [ ] **`[runtime] spin_before_park`** — expose glommio's spin-before-park as a
      config knob for latency-sensitive deployments: spinning a few µs before
      parking removes the park/unpark round trip from the ping-pong path at the
      cost of idle CPU. Off by default (the CPU trade is wrong for most fleets);
      measure the RTT delta with `wake_probe` first to size the win.
- [ ] **Reactor/ring tuning** — glommio `LocalExecutorBuilder` preempt timer and
      ring parameters; check whether a smaller `io_memory`/ring depth reduces the
      per-`io_uring_enter` cost on this kernel.
- [ ] **Batch more per wake** — the event loop already drains inbound + mailbox
      per wake; verify with `wake_probe`-style counters that a saturated shard
      averages ≫1 message per reactor wake, and profile the drain loop if not.
- [ ] Re-profile with `perf`/`strace` when available (not installed on the WSL
      box) to attribute the floor between `io_uring_enter`, task-wake
      bookkeeping, and timer maintenance before touching anything.

Gate any change on A/B: `wake_probe` (floor), `rtt.py`-style 1-conn ping-pong,
`stresser` saturating QoS 1 (1 + 3 shards), and the adversarial battery.
