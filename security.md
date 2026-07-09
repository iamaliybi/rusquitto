# Security Log

A complete record of the security vulnerabilities, loopholes, and hardening measures
identified and resolved in rusquitto — the class of each issue, the fix applied, the
security implication, and the reasoning. Where an initial security approach was tried
and **discarded for a better one**, that is recorded too, with why.

Companion to `.agents/progress.md` (build history) and `CHANGELOG.md`.

## Posture & threat model

rusquitto is an internet-facing MQTT 5.0 broker. The adversary is an **unauthenticated
remote client** who can open TCP/TLS/WebSocket connections and send arbitrary bytes.
The defensive priorities, in order: (1) no unauthenticated action; (2) no
authorization bypass or privilege escalation; (3) no single connection able to exhaust
a shard's CPU or memory; (4) no panic / undefined behavior reachable from the wire.

The **thread-per-core, shared-nothing invariant is itself a security property**: no
`Mutex`/`RwLock`/`std::thread`, no cross-shard shared memory. This is *mechanically
enforced* — `clippy.toml` disallows those types and the pre-commit hook runs
`clippy -D warnings` — so a whole class of data-race and lock-ordering bugs is
structurally impossible, not merely avoided by review.

---

## 1. Authentication & pre-auth attack surface

### ✅ CONNECT-first, CONNECT-once (pre-auth bypass) — v1.0.0

- **Class:** authentication bypass / protocol state confusion.
- **Issue:** without strict ordering, a client could send PUBLISH/SUBSCRIBE packets
  before (or a second CONNECT after) authenticating, acting on the session before
  credentials were checked.
- **Fix:** the connection state machine rejects any packet before CONNECT and any
  second CONNECT — the first packet *must* be a valid CONNECT, and only one is ever
  accepted. Enforced in the pre-connect dispatch path.
- **Implication:** every authenticated action is gated behind exactly one verified
  CONNECT.

### ✅ Handshake / idle timeouts — v1.0.0, hardened v1.8.0

- **Class:** resource exhaustion via slow-loris / silent sockets.
- **Issue:** a client that connects but never completes the handshake (or completes it
  then stalls mid-frame with keep-alive disabled) ties up a connection slot
  indefinitely.
- **Fix:** `connect_timeout` bounds the CONNECT handshake; keep-alive is enforced at
  1.5× the client's declared interval. **v1.8.0 closed the remaining gap** (the audit's
  15th adversarial case): a `partial_since` timestamp + `framing_deadline()` bound *any*
  incomplete frame by `connect_timeout` **even when keep-alive is 0**, defeating both
  the header-only truncated CONNECT and its unbounded post-CONNECT sibling. Tested with
  a `StallStream` that yields bytes once then parks (a live-but-silent socket, unlike a
  clean EOF).
- **Implication:** no connection can occupy a slot without making progress.

### ✅ Unguessable server-assigned client IDs — v1.0.0

- **Class:** session hijack / predictability.
- **Issue:** MQTT 5 lets a client send an empty client ID for the server to assign. A
  predictable assignment scheme would let an attacker guess and take over another
  client's assigned session.
- **Fix:** assigned IDs are unguessable; the per-shard counter is shard-local (the shard
  id is baked into the string) so IDs stay broker-unique with no cross-core atomic.

---

## 2. Authorization / ACL correctness

### ✅ Will-topic authorization bypass — v2.1.2 (audit finding, MEDIUM)

- **Class:** authorization bypass → telemetry poisoning / injection into a reserved
  namespace.
- **Issue:** the Will Message topic was checked against per-user ACLs but — unlike every
  live PUBLISH — **never run through `valid_publish_topic`**. A client could set a
  **retained** will on the broker-reserved `$SYS/...` namespace (or a wildcard- or
  NUL-bearing topic), then disconnect abnormally; the broker would publish and retain
  the forged message, **poisoning `$SYS/#` telemetry for every current and future
  subscriber**.
- **Fix:** `connect.rs` now validates the will topic exactly like a live publish —
  `valid_publish_topic(&will.topic) && authorize_publish(...)` — at CONNECT, dropping
  the will if invalid. Regression test `will_on_reserved_or_wildcard_topic_is_dropped`.
- **Implication:** the will path is no longer a bypass around publish-topic validation.
  The root cause was a *single validation applied on the live path but not the deferred
  (will) path* — the fix unifies them.

### ✅ Subscribe-ACL wildcard-subsumption escalation — v2.1.2 (audit finding, MEDIUM)

- **Class:** privilege escalation.
- **Issue:** SUBSCRIBE authorization compared the requested **filter** against an allow
  rule with `filter_matches(rule, requested)`, a function that treats its second
  argument as a **concrete topic**. So `filter_matches("home/+", "home/#") == true` — a
  client granted `home/+` could subscribe to `home/#` and receive the **entire
  subtree**, because the request's `#` was read as a literal level rather than a
  wildcard.
- **Discarded approach & why:** the original design reused `filter_matches` for both
  publish and subscribe ACLs. That is correct for *publish* (the topic is concrete) but
  **wrong for subscribe** (the request is itself a filter that can be broader than it
  looks). Reusing one function for two different semantic questions was the bug.
- **Fix:** a new `filter_subsumes(rule, request)` performs a proper **filter-subset**
  test — a subscription is allowed only if *every* topic the request could match is also
  covered by an allow rule. `filter_subsumes` is used for SUBSCRIBE; `filter_matches` is
  kept for PUBLISH (concrete topic). Verified against the full existing auth suite plus
  `subscribe_acl_blocks_wildcard_escalation`. The publish path is unchanged.
- **Implication:** an allow rule now bounds the subtree a client can reach, as intended.

### ✅ Anonymous-client ACLs — v1.5.0

- **Class:** least-privilege for unauthenticated access.
- **Fix:** `[auth] anonymous_publish` / `anonymous_subscribe` allow-lists constrain what
  an anonymous client (when `allow_anonymous`) may do, instead of all-or-nothing access.

### ✅ Certificate-CN → MQTT identity mapping — v1.10.0

- **Class:** per-device authorization.
- **Fix:** `[tls] cert_cn_as_username` maps a verified client certificate's subject CN
  onto an MQTT username so `[[auth.users]]` ACLs apply per device. Reasoning:
  rustls verifies the chain but does not expose the parsed subject, so `x509-parser`
  reads the CN from the verified cert. An explicit username always takes the normal
  `[auth]` path (cert-CN never silently overrides an intended identity).

---

## 3. Credential handling

### ✅ Argon2id password hashing — v1.5.0

- **Class:** offline credential cracking.
- **Issue:** SHA-256 hex password hashes are unsalted and fast — vulnerable to rainbow
  tables and high-rate brute force.
- **Fix:** `[[auth.users]] password_hash` accepts Argon2id PHC strings (salted,
  memory-hard, the recommended form) alongside legacy SHA-256 hex. Verification costs
  ~10–50 ms and ~19 MiB on the accepting core per CONNECT (the intended work factor).
- **Discarded/guarded approach:** `PasswordHash::new` will happily accept a malformed
  string like `$argon2id$garbage` — so the loader **validates that salt and hash are
  present**, rather than trusting the PHC parser to reject junk. A silently-invalid hash
  that never matches would be a self-inflicted lockout, and worse, could mask a
  misconfiguration.

### ✅ Constant-time comparison + dummy-hash for unknown users — v1.0.0 / v1.5.0

- **Class:** timing side-channel → user enumeration.
- **Issue:** returning early for an unknown username (skipping the hash verification)
  leaks, via response timing, *which usernames exist*.
- **Fix:** credential comparison is constant-time; for an **unknown** user the broker
  verifies against a **dummy hash** (reusing the first Argon2id user's PHC) so the CPU
  and wall-time cost of "no such user" matches "wrong password". Combined, an attacker
  cannot distinguish the two cases by timing.
- **Implication:** the auth path does not leak the user table's contents.

---

## 4. Topic injection & validation

### ✅ Publish-topic validation: `$`, wildcards, NUL — v1.0.0

- **Class:** injection into reserved namespaces / malformed-topic handling.
- **Issue:** a client publishing to `$SYS/...`, to a topic containing `+`/`#`, or an
  embedded NUL could inject into broker-reserved telemetry or produce undefined
  matching behavior.
- **Fix:** `valid_publish_topic` rejects wildcard characters, embedded NUL, and the
  reserved `$` prefix on the publish path. (v2.1.2 extended this same check to the will
  path — see §2.)
- **Implication:** clients cannot forge `$SYS` telemetry or inject wildcard/NUL topics.

### ✅ Topic-depth cap (recursive-trie stack overflow) — v1.0.0 (second pass)

- **Class:** denial of service via stack exhaustion.
- **Issue:** the wildcard-matching topic trie recurses per level; an attacker sending a
  pathologically deep topic could overflow the stack and crash the shard.
- **Fix:** a topic-depth cap bounds recursion depth before it can exhaust the stack.

---

## 5. Transport security

### ✅ Hardened TLS / mTLS — v1.1.0 (TLS), v1.8.0 (mTLS + hot-reload)

- **Class:** transport confidentiality / weak-protocol downgrade / peer authentication.
- **Fix:** TLS termination via rustls (ring provider), **pinned to TLS 1.3 + 1.2 only**
  with a curated AEAD/ECDHE cipher-suite list; the handshake is bounded by
  `connect_timeout`. rustls implements no protocol below 1.2 and ships only AEAD
  suites, so **weak protocols and ciphers are structurally impossible**, not merely
  disabled. v1.8.0 added mutual TLS (`require_client_cert`) with **shard-local**
  certificate hot-reload (`Rc<RefCell<Option<TlsAcceptor>>>` swapped by a per-shard
  maintenance task watching mtimes — no cross-thread `Arc`, fits the shared-nothing
  model). The `ByteStream` seam means the entire MQTT engine is reused unchanged over
  TCP / WS / TLS / WSS.
- **Design choices & constraints found:**
  - **Optional vs required client cert:** `WebPkiClientVerifier::builder(...)` for
    required mTLS, or `.allow_unauthenticated()` for optional — an explicit, configured
    decision rather than an implicit default.
  - **rustls rejects X.509 v1 client certs** (`UnsupportedCertVersion`) — discovered in
    testing; certs must be v3 (openssl needs `-extfile` with an extension). Documented
    so it isn't mistaken for a broker bug.
- **Implication:** no plaintext-downgrade or weak-cipher negotiation; optional
  cryptographic peer authentication with zero-downtime cert rotation.

### ✅ WebSocket handshake hardening — v1.0.0 (second pass)

- **Class:** DoS / protocol abuse on the WS transport.
- **Fix:** WS handshake bounded by a timeout; RFC 6455 control-frame validation.

---

## 6. Resource exhaustion / denial of service

### ✅ Per-connection and per-shard resource caps — v1.0.0, v1.2.0

- **Class:** memory / connection / CPU exhaustion.
- **Fixes:**
  - **DoS caps** (v1.0.0): session-expiry, subscriptions-per-client, retained-per-shard,
    and pending-outbound bounds — no single client can grow shard state without limit.
  - **Bounded outbound mailbox** (v1.0.0 / v1.4.0): a consumer that stops reading its
    socket stops draining; the `MAILBOX_LIMIT = 256` guard drops on full instead of
    growing unboundedly (the same bound a bounded channel would give, without its
    320 KiB pre-allocation — see `optimization.md` M1).
  - **Per-IP connection cap** (v1.0.0): `limits.max_connections_per_ip`.
  - **Per-connection PUBLISH rate limiting** (v1.2.0): a `TokenBucket`
    (`limits.max_message_rate`) throttles via `glommio::timer::sleep` (backpressure, not
    drop) so one client can't monopolize a shard.
  - **Overload subsystem** (v1.2.0): a `LoadMonitor` (EWMA of reactor scheduling delay)
    drives admission control (drop new connections when the shard is overloaded) and
    load shedding (drop live mailboxes so clients reconnect and `SO_REUSEPORT` rehashes
    them to a cooler core). Background `$SYS`/sweep tasks run in a low-share scheduling
    group so they starve rather than die under load.

### ✅ Unbounded topic-trie / interner / shared-cursor growth — v2.1.2 (memory-DoS)

- **Class:** slow memory-exhaustion DoS via long-lived churn.
- **Issue:** the subscription trie never pruned empty nodes and the segment interner was
  grow-only, so the broker's memory grew with **every distinct filter ever subscribed**
  (per-client-id filters are common); the `shared_cursor` map gained a key per shared
  group ever routed and never released it. A client cycling through unique filters could
  drive unbounded growth.
- **Fix:** trie `remove`/`remove_client`/`take_client` now prune dead nodes (no subs, no
  children) on the way back up; a periodic per-shard GC (`gc_indexes`, every 30 sweeps
  alongside `malloc_trim`) reclaims interned segments via `retain_live`
  (`strong_count == 1` ⇒ only the interner holds it) and retains only live shared-group
  cursors. Tests: `empty_nodes_are_pruned_on_removal`,
  `interner_reclaims_dead_segments_on_gc`, `gc_indexes_reclaims_stale_shared_cursor`.
- **Implication:** index memory is now bounded by the *current* subscription set, not
  the historical one.

---

## 7. Memory safety & robustness

### ✅ Panic-safe (RAII) connection accounting — v1.0.0 (second pass)

- **Class:** resource-leak / counter-desync on panic.
- **Fix:** connection counts and gauges are balanced via RAII guards, so an error or
  panic on any connection path cannot leak a slot or desync the admission counters.

### ✅ Continuous parser fuzzing — v2.1.1

- **Class:** defensive — reachable panics / undefined behavior from malformed wire input.
- **Fix:** a `proptest` harness (`server/connection/tests.rs::fuzz`) feeds
  adversarially-generated byte streams (random / "packetish" / concatenated) through
  `parse_packet` and the connected/pre-connect dispatch paths, asserting the state
  machine **never panics** — only returns a clean `Ok`/`Err`. It runs inside
  `cargo test`, so the malformed-input surface is **continuously fuzzed in CI**, not
  spot-checked. Deep-validated at `PROPTEST_CASES=50000` (50k parser + 3k dispatch) with
  **no findings** — `mqttbytes` plus the connection guards held.
- **Implication:** the wire-facing parser has a standing, automated proof-of-robustness,
  not a one-time manual review.

---

## Discarded / failed security approaches — consolidated

| Approach tried | Why discarded | Replaced by |
|----------------|---------------|-------------|
| **`filter_matches` for SUBSCRIBE ACLs** (original design) | Treats the requested filter as a concrete topic → `home/+` grant lets a client subscribe to `home/#` (whole-subtree escalation). One function answering two different questions. | **`filter_subsumes`** (proper filter-subset test) for subscribe; `filter_matches` kept for publish (concrete topic). (v2.1.2) |
| **Validating the will topic by ACL only** | ACL check without `valid_publish_topic` let a retained will forge `$SYS`/wildcard/NUL topics — the live-publish validation was applied on the live path but not the deferred will path. | Unified validation: will topic runs the **same** `valid_publish_topic + ACL` as a live publish. (v2.1.2) |
| **Trusting `PasswordHash::new` to reject junk** | It accepts malformed PHC like `$argon2id$garbage`, which would silently never match (self-lockout / masked misconfig). | Explicit validation that **salt and hash are present** before accepting the hash. (v1.5.0) |
| **Early-return for unknown usernames** | Leaks user existence via response timing (enumeration). | **Dummy-hash verification** for unknown users so timing matches "wrong password"; constant-time compare. (v1.0.0 / v1.5.0) |
| **SHA-256 hex as the only password hash** | Unsalted and fast → rainbow tables / cheap brute force. | **Argon2id** PHC (salted, memory-hard) added and recommended; SHA-256 retained only for legacy compatibility. (v1.5.0) |
| **Work-stealing runtime (e.g. tokio) for load balancing** | Would require `Send` state and cross-core shared memory, reintroducing the data-race / lock-ordering bug class the shared-nothing model eliminates. | Kept **shared-nothing**; load handled by admission control + shedding + `SO_REUSEPORT` rehash. (v1.2.0) |
| **Per-publisher exclusion for No Local on shared subscriptions** | No Local on a shared subscription is a protocol error (MQTT 5 §3.8.3.1); the old per-publisher exclusion could desync the deterministic cross-shard delivery pick (double/zero delivery). | **Reject** No Local on shared subscriptions at SUBSCRIBE, keeping every shard's candidate view identical. (v1.5.0) |

---

## Summary

Two genuine authorization vulnerabilities (will-topic bypass, subscribe-ACL
escalation) and one memory-exhaustion vector (unbounded index growth) were found and
fixed in the v2.1.2 audit; the earlier releases built the foundational hardening
(pre-auth gating, topic validation, DoS caps, hardened TLS/mTLS, strong password
hashing, timing-safe auth) and the standing defenses (continuous parser fuzzing,
mechanically-enforced shared-nothing, RAII accounting).

The through-line: **fixes unify a check that was applied inconsistently** (will vs
live publish), **replace a function used for the wrong semantic question**
(`filter_matches` → `filter_subsumes`), **bound what was unbounded** (indexes,
mailboxes, connections, frames), and **remove side channels** (timing). Where a first
approach was wrong, it was replaced deliberately and the reasoning recorded here so the
weaker design is not reintroduced.
