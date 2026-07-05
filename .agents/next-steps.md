# Rusquitto — What's Next

**The backlog is empty.** Every item of the Phase 3 hardening roadmap — and the
follow-ups it accumulated — has shipped. The last four (anonymous-client ACL,
Argon2id password hashing, outbound topic aliases, and globally-coordinated
shared-subscription delivery) landed in **v1.5.0** (2026-07-05).

The full history of what was built, in order, with design decisions and
gotchas, lives in [progress.md](progress.md). The current feature matrix is in
[overview.md](overview.md).

## Candidate future work (nothing committed)

Ideas noted along the way, in rough value order — none is planned or promised:

- **Idle-memory deep dive** — a heaptrack/DHAT pass on the remaining
  ~16 KiB/conn idle footprint (glommio task/source/channel overhead); see the
  deferred list in PR #31.
- **Kernel socket-buffer caps** — configurable `SO_RCVBUF`/`SO_SNDBUF` on the
  listeners to bound kernel-side memory at high concurrency on small hosts.
- **aarch64 release target** — `.cargo/config.toml` pins x86_64; t4g-class
  deployment needs an aarch64 build.
- **Session/queued-message WAL** — persistence is snapshot-based; a
  write-ahead log would close the crash window (`snapshot_interval`).
- **mTLS** (client-certificate authentication) and certificate hot-reload.
- **Multi-machine clustering** — extend the mesh design across hosts (the
  shared-subscription membership replication and deterministic pick were built
  to survive that step).
