# Rusquitto — What's Next

**The backlog is empty.** Every item of the Phase 3 hardening roadmap — and the
follow-ups it accumulated — has shipped. The last four (anonymous-client ACL,
Argon2id password hashing, outbound topic aliases, and globally-coordinated
shared-subscription delivery) landed in **v1.5.0** (2026-07-05).

The full history of what was built, in order, with design decisions and
gotchas, lives in [progress.md](progress.md). The current feature matrix is in
[overview.md](overview.md).

The memory deep-dive, kernel socket-buffer caps, and the aarch64 target
shipped in **v1.6.0** (2026-07-05): idle 7.5 KiB/conn (task future 13144 → 600 B
via boxed transport pipelines), `malloc_trim` on the maintenance tick,
`[server] socket_recv_buffer`/`socket_send_buffer`, and a
`rusquitto-aarch64-unknown-linux-gnu` release asset via `cargo zigbuild`.

## Candidate future work (nothing committed)

Ideas noted along the way, in rough value order — none is planned or promised:

- **Sub-4-KiB idle connections** — the remaining footprint is the connection
  state machine itself (~4.5 KiB boxed) plus session/channel bookkeeping;
  shrinking further means slimming `event_loop`/handler futures. `examples/
  allocprobe.rs` measures it.
- **Session/queued-message WAL** — persistence is snapshot-based; a
  write-ahead log would close the crash window (`snapshot_interval`).
- **mTLS** (client-certificate authentication) and certificate hot-reload.
- **Multi-machine clustering** — extend the mesh design across hosts (the
  shared-subscription membership replication and deterministic pick were built
  to survive that step).
