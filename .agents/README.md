# Agentic Context for Rusquitto

Internal working notes. Last refreshed 2026-07-03 (Phase 3a–3i — + subscription options; shutdown/drain, $SYS, auth+ACL, etc.).

| File                               | Contents                                                                       |
|------------------------------------|--------------------------------------------------------------------------------|
| [overview.md](overview.md)         | Project status summary, current feature matrix, build/run commands             |
| [architecture.md](architecture.md) | Thread-per-core design, io_uring, SO_REUSEPORT, inter-shard mesh, key files    |
| [dependencies.md](dependencies.md) | Direct deps with purpose, key types by source crate                            |
| [scope.md](scope.md)               | Settled product decisions — what the broker is and is deliberately not         |
| [next-steps.md](next-steps.md)     | TODO list: open work only, phased, with priority/risk/status badges            |
| [progress.md](progress.md)         | Full implementation log (all phases through v1.9.1), decisions, gotchas        |
