# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, the glommio-0.9 memory/CPU bounds, …) →
[scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

## 1. Optimization backlog — `route()` per-subscriber allocation (measured: no win, closed)

![priority](https://img.shields.io/badge/priority-low-lightgrey)
![risk](https://img.shields.io/badge/risk-medium-yellow)
![status](https://img.shields.io/badge/status-closed--no--win-lightgrey)

The message hot path is already tight (Rc fan-out, in-place PUBLISH normalization,
single-shard mesh skip, interner off the publish path, coalesced writes — all
optimal). The one candidate remaining allocation was `broker/shard/routing.rs::route`
cloning each matching subscriber's `client_id` into the per-publish `best`/`groups`
maps (`sub.client_id.clone()`), a short-string heap allocation **per subscriber per
message** that scales with fan-out width.

**Prototyped and measured — no win, not shipped.** The disjoint-field-borrow
restructure was implemented in full (destructure `self` into `{trie, sessions,
shared_cursor, shared_remote, unpark_tx, wal}`, key `best`/`groups` by `&str`
borrowed from the trie, `deliver_to` extracted to a free fn over those fields) and
gated on a purpose-built wide-fan-out benchmark (`stress`-style: 1000 subscribers on
one topic, publisher hammering QoS 0, shard CPU-saturated). Full suite stayed green
(120 unit + 23 integration, incl. shared-sub exact-once, cross-shard, No Local,
sub_id echo); clippy/fmt clean.

- **Baseline (owned `String` keys):** 64.48 µs/publish.
- **Refactored (`&str` keys, zero per-subscriber alloc):** 64.9 / 64.5 / 62.6 µs
  (mean ≈ 64.0) — **identical within ±3.5 % run-to-run noise.**

At 1000-wide fan-out (the most allocation-favorable case possible — ~15.5 M
`client_id` allocs/sec eliminated) the win is below the harness noise floor: the
64.9 ns/delivery cost is dominated by the channel `try_send` + `Delivery`
construction + `Rc` clone, not the short-string alloc, which glibc's tcache absorbs.
If it doesn't surface at 1000-wide it surfaces nowhere. **Reverted** to keep the
delivery path minimal; recorded here so it is not re-attempted expecting a win.
(Same-gate sibling, also not worth it: the `send_publish` `Publish` clone.)

---

*No open items remain. The per-connection **memory and CPU** items that once
lived here — live (unparked) connection heap (~4.8 KiB), active-connection memory
(~7.6 KiB), and the −7 % saturating QoS 1 per core — are **not open work**: the
dispatcher-mode program (progress.md Phases 14–18) prototyped the fix and proved,
by measurement, that it is an **architectural bound of the glommio 0.9 runtime**
(serving active connections off a raw readiness ring costs ms-scale wake latency —
3.1 ms vs 0.3 ms on glommio's reactor — because the runtime's efficient low-latency
I/O wait and the per-connection memory are the same mechanism). They are recorded as
accepted constraints in [scope.md](scope.md); parking already reclaims the **idle**
case to 0.68 KiB/conn, which is where the density win actually lives.*
