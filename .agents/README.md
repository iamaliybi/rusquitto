# Agentic Context for Rusquitto

Notes saved by Claude Code on 2026-06-29 after full project analysis.

| File                               | Contents                                                                        |
|------------------------------------|---------------------------------------------------------------------------------|
| [overview.md](overview.md)         | Project status summary, feature matrix, build commands                          |
| [architecture.md](architecture.md) | Thread-per-core design, io_uring, SO_REUSEPORT, connection lifecycle, key files |
| [dependencies.md](dependencies.md) | Direct deps with purpose, key types by source crate                             |
| [next-steps.md](next-steps.md)     | Prioritised Phase 2 work: Topic Trie → Pub/Sub → QoS → inter-shard              |
| [progress.md](progress.md)         | Implementation progress (Steps 1–4 done & verified), decisions, mqttbytes gotchas |
