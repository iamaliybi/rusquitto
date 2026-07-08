# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, the glommio-0.9 memory/CPU bounds, …) →
[scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

**No open items.** The roadmap is clear.

The one former backlog item — the `route()` per-subscriber `client_id` allocation —
**shipped in v2.2.1** (the fan-out maps now key on borrowed `&str` from the trie via
a disjoint-field-borrow, removing one heap allocation per matched subscriber per
message; full record in [progress.md](progress.md)). It carried **no measured
throughput win** (the delivery cost is mailbox-`try_send`-bound, not
allocation-bound) and was landed for the allocation reduction and cleaner borrow
structure alone, not a performance claim. The same-gate sibling (`send_publish`'s
`Publish` clone) remains un-done: identical no-measurable-win expectation, not worth
the wire-encoding duplication.

---

*The per-connection **memory and CPU** items that once
lived here — live (unparked) connection heap (~4.8 KiB), active-connection memory
(~7.6 KiB), and the −7 % saturating QoS 1 per core — are **not open work**: the
dispatcher-mode program (progress.md Phases 14–18) prototyped the fix and proved,
by measurement, that it is an **architectural bound of the glommio 0.9 runtime**
(serving active connections off a raw readiness ring costs ms-scale wake latency —
3.1 ms vs 0.3 ms on glommio's reactor — because the runtime's efficient low-latency
I/O wait and the per-connection memory are the same mechanism). They are recorded as
accepted constraints in [scope.md](scope.md); parking already reclaims the **idle**
case to 0.68 KiB/conn, which is where the density win actually lives.*
