# rusquitto adversarial stress & chaos suite

A replacement for the old `scripts/mosquitto.sh` fork-bomb. Where that spawned one
`mosquitto_pub` process per message, this suite drives the broker at the socket
and protocol level with hostile intent, and asserts one thing above all:

> **the broker survives every attack and keeps serving honest clients.**

Each scenario targets a specific mechanism in the thread-per-core / glommio design
and ends with a health check + a broker-liveness/panic scan.

## Layout

| File | Language | Purpose |
|------|----------|---------|
| `mqttwire.py`  | Python (stdlib) | Hand-rolled MQTT 5 codec — builds valid **and malformed** frames byte-for-byte |
| `attack.py`    | Python (stdlib + asyncio) | The scenario battery (see below) |
| `stresser.rs`  | Rust (std only) | Compiled, many-threaded throughput hammer for max load |
| `netchaos.sh`  | Bash + tc/hping3 | Kernel/TCP-level chaos (latency, loss, SYN flood) — needs root |
| `run.sh`       | Bash | Orchestrator: build → launch → attack → teardown |

Everything is dependency-free: `attack.py` needs only Python 3.8+, `stresser.rs`
compiles with a bare `rustc`, and `netchaos.sh` degrades gracefully if `tc`/`hping3`
aren't installed.

## Quick start

```bash
cd stress
./run.sh                      # build broker, run every app-level scenario, tear down
./run.sh throughput           # one scenario
SKIP_BUILD=1 ./run.sh idle    # reuse an existing release binary
```

Against an already-running broker:

```bash
python3 attack.py --host 127.0.0.1 --port 1883 all
python3 attack.py malformed
python3 attack.py throughput --connections 3000 --duration 20 --qos 2
```

Compiled throughput hammer (heaviest load):

```bash
rustc -O stresser.rs -o stresser
./stresser 127.0.0.1:1883 --connections 4000 --duration 20 --qos 1 --payload 128
```

## Scenarios (`attack.py`)

| Scenario | Attack | Mechanism under test |
|----------|--------|----------------------|
| `idle`       | Open N sockets, send nothing | CONNECT handshake timeout, idle reaping, slot holding |
| `churn`      | Massive connect/CONNECT/disconnect loop | fd exhaustion, io_uring accept/close reactor, per-shard accounting |
| `slowloris`  | Dribble CONNECT one byte then stall | slow-handshake timeout (pre-auth deadline) |
| `slowreader` | Subscribe, never read, flood the victim | outbound **mailbox bounding** / backpressure (samples broker RSS) |
| `fragment`   | Valid CONNECT+PUBLISH sent 1 byte at a time | incremental frame reassembly, partial-packet buffering |
| `malformed`  | 15-frame hostile battery (bad varint, 256 MiB length claim, pre-CONNECT PUBLISH, `$SYS` spoof, wildcard/NUL/empty topic, oversized payload, double CONNECT…) | parser hardening, auth ordering, no crash/OOM/hang |
| `topics`     | 5000-level topic/filter, wildcard explosion, ~60 KiB topic | trie recursion **depth cap**, matcher fan-out |
| `throughput` | asyncio, thousands of conns, QoS 0/1/2 | cross-core contention, starvation, sustained msg/s |

The `slowreader` and `throughput` scenarios sample the broker's RSS from `/proc`
so you can *see* that a dead reader or a firehose doesn't balloon memory — the
bounded mailbox and queues should keep it flat.

## Kernel / TCP chaos (`netchaos.sh`, root)

```bash
sudo ./netchaos.sh latency 200 50    # 200ms ± 50ms jitter on loopback
sudo ./netchaos.sh loss 10           # 10% packet loss
sudo ./netchaos.sh corrupt 5         # 5% corruption
sudo ./netchaos.sh reorder 50 25     # reorder 25% of packets
sudo ./netchaos.sh synflood 1883 20  # 20s SYN flood (relies on tcp_syncookies)
sudo ./netchaos.sh halfopen 1883 2000
sudo ./netchaos.sh reset             # remove all shaping
```

Apply a chaos profile in one terminal, run `attack.py`/`stresser` in another, to
test the broker under latency/loss + adversarial load simultaneously.

> **Note on WSL2:** loopback `netem` shaping is limited under WSL2's virtual NIC.
> For faithful TCP-chaos results use a bare-metal/VM Linux host or a `veth` pair.
> The app-level scenarios (`attack.py`, `stresser.rs`) are fully faithful anywhere.

## Safety

These tools are for **your own broker on a host you control**. `netchaos.sh`
shapes/floods real interfaces and needs root; `synflood`/`halfopen` generate
attack traffic — never point them at infrastructure you don't own. `run.sh` binds
the broker to `127.0.0.1` only.

## Interpreting results

`attack.py` prints a **report card**; a scenario is `PASS` only if the broker was
still healthy afterwards. `run.sh` additionally fails the run if the broker process
died or if `panic`/`overflow`/`abort` appears in the broker log. Exit code `0` =
the broker withstood everything.
