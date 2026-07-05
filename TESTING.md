# Testing & Reliability

How rusquitto is tested, layer by layer — what each layer covers, how to run it,
and where the gaps are. The goal is a pyramid: many fast, deterministic checks at
the bottom; fewer, broader end-to-end and adversarial checks above; and a set of
out-of-band harnesses for the things that need a real process, real crypto, or a
kill signal.

Everything under **Automated (CI)** runs from a single `cargo test` and gates the
pre-commit hook. Everything under **Harnesses** is scripted and reproducible but
needs external tooling (a broker process to `kill -9`, `openssl`, `mosquitto`)
so it is run deliberately, not on every commit.

---

## Automated (CI) — `cargo test`

### A. Unit tests — the connection state machine over an in-memory stream

`src/**/tests.rs`, ~94 tests. The MQTT engine is written against the
[`ByteStream`] trait, so the entire handshake and packet-handling logic is driven
over an in-memory `MockStream` with **no sockets** — the full protocol runs
deterministically and fast.

- **Connection engine** (`server/connection/tests.rs`): CONNECT ordering
  (first-packet-must-be-CONNECT, duplicate-CONNECT), CONNACK, PING, QoS 1/2 ack
  bookkeeping, reserved/`$SYS`/wildcard/NUL publish-topic rejection, subscription
  counting, outbound topic-alias substitution + table-full fallback, No-Local on
  shared subscriptions, per-connection rate limiting, and the **partial-frame
  stall guard** (a `StallStream` that yields bytes once then parks, driving the
  real `event_loop` to prove a stalled frame is reaped even with keep-alive off).
- **Broker state** (`broker/shard/tests.rs`): fan-out with QoS downgrade,
  subscription-identifier delivery, session open/resume/suspend/expire generation
  handling, shared-subscription membership + the deterministic global pick, and
  the WAL dirty/removed tracking round-tripped through replay.
- **Persistence codec** (`persistence/*/tests.rs`): retained + session snapshot
  round-trips, bad-magic/truncation rejection, and the **WAL** (last-writer-wins
  replay, snapshot-seeded replay, torn-trailing-record tolerance).
- **Config** (`config.rs`): validation rules (port uniqueness, required TLS
  cert/key, mTLS CA requirement, credential exclusivity, …).
- **Auth** (`auth.rs`): plaintext / SHA-256 / Argon2id verification, unknown-user
  timing parity, and per-operation topic ACLs.
- **Topics** (`broker/topics/*.rs`): wildcard matching and the segment interner.
- **Transport** (`transport/{tls,websocket}.rs`): the rustls version/cipher
  posture, mutual-TLS verifier construction, and the RFC 6455 WS handshake +
  control-frame validation.

### B. Integration tests — a real broker over real sockets

`tests/integration.rs`, 15 tests. Each boots a **real broker in-process** (via the
public `rusquitto::run`) on an ephemeral port and drives it with a minimal MQTT 5
client built on `mqttbytes` + `std::net::TcpStream`. Brokers are lazily started
and **shared per configuration** (a `OnceLock` each — a default anonymous broker,
an auth/ACL broker, and a 3-shard broker), so the suite spins up only a handful of
executor pools and tests stay isolated via unique client ids and topics.

| Area | Tests |
|------|-------|
| Handshake | CONNACK success |
| QoS | QoS 0 / 1 / 2 end-to-end (full PUBACK and PUBREC→PUBREL→PUBCOMP), downgrade-to-granted |
| Retained | replay to a late subscriber, and clear-on-empty-payload |
| Wildcards | `+` and `#` matching |
| Unsubscribe | delivery stops after UNSUBSCRIBE |
| Sessions | persistent session queues offline then replays on resume |
| Will | fires on abrupt disconnect (no DISCONNECT) |
| Resilience | a malformed frame closes the connection, broker keeps serving |
| Auth | bad password → `BadUserNamePassword`; anonymous → `NotAuthorized`; correct → success |
| ACL | out-of-scope publish is refused (not delivered), in-scope is |
| Cross-shard | every publish crossing shards is delivered |
| Shared subs | each message delivered exactly once across group members on different shards |

```sh
cargo test                    # unit + integration + doctests
cargo test --test integration # just the end-to-end suite
```

---

## Harnesses — scripted, reproducible, run deliberately

### C. Adversarial / chaos battery — `stress/attack.py`

A ruthless, stdlib-only battery (`stress/mqttwire.py` codec + `attack.py`). Each
scenario targets a specific mechanism and ends with a health check — the assertion
is "did the broker survive and keep serving honest clients?". Scenarios: `idle`
(silent-socket reaping), `churn`, `slowloris`, `slowreader` (bounded mailbox under
a firehose), `fragment` (byte-by-byte reassembly), `malformed` (a battery of
hostile frames), `topics` (deep trees, wildcard explosion, oversized topics), and
`throughput`.

```sh
cargo build --release
target/x86_64-unknown-linux-gnu/release/rusquitto stress.toml &   # single-shard config
python3 stress/attack.py --port 1883 all
```

### D. Crash recovery (WAL)

Proves the session write-ahead log closes the crash window: create a durable
subscriber, suspend it, publish a QoS 1 message to its offline queue, `kill -9`
the broker *between snapshots*, restart, and verify the session resumes
(`session_present = 1`) with its queued message redelivered. Requires
`[persistence] enabled = true`, a large `snapshot_interval` (so only the WAL can
save the state), and a small `wal_flush_ms`.

### E. Mutual TLS (live)

An `openssl`-generated CA + server + client certs against a broker with
`require_client_cert = true`: a cert-verified client with no MQTT username is
accepted (even under `allow_anonymous = false`), a certless client is rejected at
the handshake, and a rotated server certificate is served to new handshakes after
`reload_interval`. (X.509 v3 certs required — rustls rejects v1.)

### F. Soak — `stress/soak.py`

Long-running connect/subscribe/publish/disconnect churn that asserts RSS returns
to baseline (leak detection), complementing the periodic `malloc_trim`.

---

## Performance & memory probes (`cargo run --example …`)

These measure rather than assert — used to catch regressions and to guide
optimization.

- **`alloc_probe`** — decomposes idle per-connection heap by allocation size
  class, separating true heap from allocator/page overhead (idle is ~3.7 KiB/conn).
- **`park_probe`** — the parked-connection feasibility spike: measures the memory
  floor of an idle fd on a shared `io_uring` readiness ring (~0.08 KiB/conn).
- **`stresser`** (`stress/stresser.rs`) — the throughput hammer.
- Ad-hoc latency: a synchronous PUBLISH→PUBACK round-trip probe (single-shard p50
  ~55 µs).

---

## Coverage summary & known gaps

**Well covered:** the connection state machine, packet handling, QoS 0/1/2 flows,
retained, wildcards, sessions + offline queue, will messages, auth + ACLs, the
persistence codec + WAL replay, cross-shard routing, shared subscriptions,
malformed-frame resilience, and the security posture of the TLS stack — all in
`cargo test`.

**Covered by harness, not yet in `cargo test`** (they need process-level control
or external tools): crash-recovery restart, live mTLS handshakes, the full
adversarial battery, and soak. These are the reliability checks that can't run
purely in-process.

**Gaps / future work:**
- No property-based / fuzz harness on the frame parser (the `malformed` battery is
  hand-curated, not generative). A `cargo-fuzz` or `arbitrary`-driven target over
  `parse_packet` would harden the edges further.
- WebSocket / `wss` transports are unit-tested at the handshake layer but not
  exercised end-to-end in the integration suite (which uses raw TCP).
- The crash-recovery and mTLS harnesses are scripted but not wired into CI.
