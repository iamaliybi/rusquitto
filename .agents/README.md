# Agentic Context for Rusquitto

Internal working notes. Last refreshed 2026-07-02 (Phase 3a–3d — sessions, will, negotiation, auth).

| File                               | Contents                                                                       |
|------------------------------------|--------------------------------------------------------------------------------|
| [overview.md](overview.md)         | Project status summary, current feature matrix, build/run commands             |
| [architecture.md](architecture.md) | Thread-per-core design, io_uring, SO_REUSEPORT, inter-shard mesh, key files    |
| [dependencies.md](dependencies.md) | Direct deps with purpose, key types by source crate                            |
| [next-steps.md](next-steps.md)     | Phase 3 hardening roadmap: cross-shard QoS, sessions, will, auth, negotiation  |
| [progress.md](progress.md)         | Full implementation log (Steps 1–6 + logging + CLI/config + sessions), decisions, gotchas |
