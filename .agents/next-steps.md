# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, the glommio-0.9 memory/CPU bounds, …) →
[scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

## 1. Optimization backlog — `route()` per-subscriber allocation (optional, benchmark-gated)

![priority](https://img.shields.io/badge/priority-low-lightgrey)
![risk](https://img.shields.io/badge/risk-medium-yellow)
![status](https://img.shields.io/badge/status-documented-blue)

The message hot path is already tight (Rc fan-out, in-place PUBLISH normalization,
single-shard mesh skip, interner off the publish path, coalesced writes — all
optimal). The one remaining allocation: `broker/shard/routing.rs::route` clones each
matching subscriber's `client_id` into the per-publish `best`/`groups` maps
(`sub.client_id.clone()`), a short-string heap allocation **per subscriber per
message** that scales with fan-out width.

- [ ] Eliminating it needs a **disjoint-field-borrow restructure** of `route` +
      `deliver_to` (destructure `self` into `{trie, sessions, shared_cursor,
      shared_remote, unpark_tx, wal}`, key `best` by `&str` borrowed from the trie,
      make `deliver_to` a free fn over those fields; reuse `best` as a scratch field).
- **Gate before attempting**: a wide-fan-out benchmark proving the win (the throughput
  harness publishes to no-subscriber topics, so it does not exercise this), plus the
  routing unit tests + integration QoS/shared-sub suite. Not worth the delivery-path
  regression risk without that proof. (Higher-risk sibling: the `send_publish`
  `Publish` clone — a `write_publish` helper encoding from the borrowed `Rc` would cut
  the QoS-0 topic alloc but duplicates wire-encoding; same benchmark gate.)

---

*This is the only open item. The per-connection **memory and CPU** items that once
lived here — live (unparked) connection heap (~4.8 KiB), active-connection memory
(~7.6 KiB), and the −7 % saturating QoS 1 per core — are **not open work**: the
dispatcher-mode program (progress.md Phases 14–18) prototyped the fix and proved,
by measurement, that it is an **architectural bound of the glommio 0.9 runtime**
(serving active connections off a raw readiness ring costs ms-scale wake latency —
3.1 ms vs 0.3 ms on glommio's reactor — because the runtime's efficient low-latency
I/O wait and the per-connection memory are the same mechanism). They are recorded as
accepted constraints in [scope.md](scope.md); parking already reclaims the **idle**
case to 0.68 KiB/conn, which is where the density win actually lives.*
