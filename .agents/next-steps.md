# Rusquitto — TODO

Open work only. Completed work → [progress.md](progress.md). Settled product
decisions (MQTT-5-only, no plugins, …) → [scope.md](scope.md).

Each item carries three badges — **priority** (value/severity), **risk**
(implementation risk), and **status** (state). Phases use task-list checkboxes.

---

## 1. Parked-connection idle path

![priority](https://img.shields.io/badge/priority-high-red)
![risk](https://img.shields.io/badge/risk-high-red)
![status](https://img.shields.io/badge/status-on%20hold%20(awaiting%20go--ahead)-lightgrey)

Close the connection-density gap: idle costs **~3.7 KiB/conn** vs Mosquitto's
**0.76 KiB** (~5×). The floor is glommio's per-connection task + io_uring read
`Source` (~1.9 KiB) — not a buffer — removable only by dropping the per-idle task
and holding an idle fd as a minimal struct on a shared readiness ring. Target:
**~0.1 KiB/conn** idle.

- [x] **Phase 0 — feasibility spike.** `examples/park_probe.rs` proved a parked fd
      on a shared `IORING_OP_POLL_ADD` ring costs **0.08 KiB/conn**, wake path
      works (2000/2000), and glommio streams expose `IntoRawFd`/`FromRawFd`.
- [ ] **Phase 1 — per-shard readiness ring.** Raw io_uring for parked fds (glommio
      doesn't expose its `POLL_ADD`), driven by one glommio task on the ring's
      eventfd. Plain-TCP only (TLS/WS carry mid-stream state).
- [ ] **Phase 2 — park predicate + transition.** In `event_loop`, when idle and
      `inflight`/`incoming_qos2`/`pending_outbound`/`inbound` are empty and no
      partial frame is buffered (the `partial_since` guard already tracks this):
      serialize into `ParkedConn`, register the fd, return (free the task).
- [ ] **Phase 3 — unpark.** Ingress readiness **and** egress (`route`→`deliver_to`
      targeting a parked client) both resurrect a task that rebuilds the
      `Connection`, drains, and re-parks. Egress-wake is the subtle part.
- [ ] **Phase 4 — lifecycle.** Parked keep-alive expiry (task-less, via the sweep
      timer), a `$SYS/broker/parked-connections` gauge, shed/migration interaction.

Gate each phase on the `alloc_probe` + adversarial-battery numbers.

---

*(The parked-connection idle path is the only open item. The mTLS cert-CN →
username ACL mapping shipped and moved to [progress.md](progress.md); a SAN-based
identity fallback — using a Subject Alternative Name when the cert has no CN — is a
possible small follow-up, not currently tracked.)*
