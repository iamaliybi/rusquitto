#!/usr/bin/env python3
"""rusquitto adversarial stress & chaos suite.

A ruthless, self-contained (stdlib-only) battery of attacks against a running
broker. Each scenario is designed to break a *specific* mechanism, and every one
ends with a health check — the real assertion is "did the broker survive and keep
serving honest clients?".

    python3 attack.py --host 127.0.0.1 --port 1883 all
    python3 attack.py idle --connections 500 --hold 12
    python3 attack.py throughput --connections 2000 --duration 15 --qos 1

Scenarios: idle | churn | slowloris | slowreader | fragment | malformed |
           topics | throughput | all
"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import struct
import sys
import threading
import time

import mqttwire as m

# --- tiny ANSI reporting -----------------------------------------------------
G, R, Y, B, X = "\033[32m", "\033[31m", "\033[33m", "\033[34m", "\033[0m"
RESULTS: list[tuple[str, bool, str]] = []


def hdr(title: str) -> None:
    print(f"\n{B}== {title} =={X}")


def ok(msg: str) -> None:
    print(f"  {G}PASS{X} {msg}")


def bad(msg: str) -> None:
    print(f"  {R}FAIL{X} {msg}")


def info(msg: str) -> None:
    print(f"  {Y}··{X}  {msg}")


def record(name: str, survived: bool, detail: str) -> None:
    RESULTS.append((name, survived, detail))
    (ok if survived else bad)(f"{name}: {detail}")


# --- shared helpers ----------------------------------------------------------


def raw_connect(host: str, port: int, timeout: float = 3.0) -> socket.socket:
    s = socket.create_connection((host, port), timeout=timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def healthy(host: str, port: int, cid: str = "health") -> bool:
    """A full honest CONNECT->CONNACK round trip. Proves the broker still serves."""
    try:
        s = raw_connect(host, port)
        s.sendall(m.connect(client_id=cid, keepalive=30))
        good = m.expect_connack_ok(s)
        s.sendall(m.DISCONNECT)
        s.close()
        return good
    except OSError:
        return False


def broker_rss_kb() -> int | None:
    """Sum RSS (KiB) of all rusquitto processes on this host, or None if unknown."""
    total = 0
    found = False
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/comm") as f:
                if f.read().strip() != "rusquitto":
                    continue
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        total += int(line.split()[1])
                        found = True
        except (OSError, ValueError):
            continue
    return total if found else None


# --- 1. Idle / silent connections -------------------------------------------


def scn_idle(host, port, connections, hold):
    hdr(f"IDLE/SILENT — {connections} sockets, send nothing, watch the reaper")
    socks = []
    for _ in range(connections):
        try:
            s = raw_connect(host, port)
            s.setblocking(False)
            socks.append([s, time.time(), None])  # sock, opened_at, closed_after
        except OSError:
            pass
    info(f"opened {len(socks)} silent sockets; waiting up to {hold}s for the broker to drop them")

    deadline = time.time() + hold
    while time.time() < deadline and any(s[2] is None for s in socks):
        for entry in socks:
            if entry[2] is not None:
                continue
            try:
                if entry[0].recv(1) == b"":  # broker closed it
                    entry[2] = time.time() - entry[1]
            except BlockingIOError:
                pass
            except OSError:
                entry[2] = time.time() - entry[1]
        time.sleep(0.1)

    closed = [e[2] for e in socks if e[2] is not None]
    for e in socks:
        try:
            e[0].close()
        except OSError:
            pass
    if closed:
        info(f"broker closed {len(closed)}/{len(socks)} idle sockets; "
             f"first {min(closed):.1f}s / last {max(closed):.1f}s")
        record("idle", healthy(host, port), f"{len(closed)} idle sockets reaped, broker healthy")
    else:
        record("idle", healthy(host, port),
               f"no idle sockets reaped within {hold}s (check connect_timeout) — broker healthy")


# --- 2. Connection churn -----------------------------------------------------


def scn_churn(host, port, total, concurrency):
    hdr(f"CONNECTION CHURN — {total} connect/CONNECT/disconnect, {concurrency} threads")
    done = [0]
    errors = [0]
    lock = threading.Lock()
    per_thread = max(1, total // concurrency)

    def worker():
        local_ok = local_err = 0
        for _ in range(per_thread):
            try:
                s = raw_connect(host, port, timeout=2.0)
                s.sendall(m.connect(client_id="", keepalive=5))
                m.expect_connack_ok(s, 2.0)
                s.sendall(m.DISCONNECT)
                s.close()
                local_ok += 1
            except OSError:
                local_err += 1
        with lock:
            done[0] += local_ok
            errors[0] += local_err

    t0 = time.time()
    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt = time.time() - t0
    rate = done[0] / dt if dt else 0
    info(f"{done[0]} cycles in {dt:.1f}s = {rate:,.0f}/s, {errors[0]} errors")
    record("churn", healthy(host, port), f"{rate:,.0f} conn/s sustained, broker healthy")


# --- 3. Slowloris (slow handshake) ------------------------------------------


def scn_slowloris(host, port, connections, byte_delay):
    hdr(f"SLOWLORIS — {connections} sockets dribbling CONNECT one byte / {byte_delay}s")
    pkt = m.connect(client_id="slowloris", keepalive=60)
    socks = []
    for _ in range(connections):
        try:
            socks.append(raw_connect(host, port))
        except OSError:
            pass
    info(f"holding {len(socks)} sockets, dribbling the CONNECT byte-by-byte")

    # Send only the first two bytes of each CONNECT, then stall — a classic
    # never-complete-the-request slow-loris. A hardened broker times these out.
    reaped = 0
    for s in socks:
        try:
            s.sendall(pkt[:2])
        except OSError:
            pass
    time.sleep(byte_delay)
    deadline = time.time() + 15
    while time.time() < deadline and reaped < len(socks):
        for s in socks:
            try:
                s.settimeout(0.2)
                if s.recv(1) == b"":
                    reaped += 1
            except (socket.timeout, BlockingIOError):
                pass
            except OSError:
                reaped += 1
        time.sleep(0.5)
    for s in socks:
        try:
            s.close()
        except OSError:
            pass
    info(f"broker dropped {reaped}/{len(socks)} stalled handshakes")
    record("slowloris", healthy(host, port),
           f"{reaped} stalled handshakes reaped, broker healthy")


# --- 4. Slow reader (backpressure / mailbox bounding) ------------------------


def scn_slowreader(host, port, flood, payload_kb):
    hdr(f"SLOW READER — subscriber never reads while {flood} msgs flood it "
        f"({payload_kb} KiB each)")
    rss0 = broker_rss_kb()
    # Victim subscribes then stops reading its socket entirely.
    victim = raw_connect(host, port)
    victim.sendall(m.connect(client_id="victim", keepalive=0))
    m.expect_connack_ok(victim)
    victim.sendall(m.subscribe([("flood/#", 0x01)], pkid=1))
    m.read_packet(victim, 2.0)  # SUBACK
    # From here the victim NEVER reads -> broker must bound what it buffers for it.

    pub = raw_connect(host, port)
    pub.sendall(m.connect(client_id="flooder", keepalive=0))
    m.expect_connack_ok(pub)
    blob = os.urandom(payload_kb * 1024)
    sent = 0
    for i in range(flood):
        try:
            pub.sendall(m.publish("flood/x", blob, qos=1, pkid=(i % 65535) + 1))
            # Drain the flooder's own PUBACKs so it doesn't block on us.
            try:
                pub.settimeout(0.01)
                pub.recv(65536)
            except (socket.timeout, BlockingIOError):
                pass
            sent += 1
        except OSError:
            break
    rss1 = broker_rss_kb()
    for s in (victim, pub):
        try:
            s.close()
        except OSError:
            pass

    flooded_kib = sent * payload_kb
    growth = f"{(rss1 - rss0) / 1024:.1f} MiB RSS growth" if (rss0 and rss1) else "RSS unknown"
    info(f"flooded {sent} msgs (~{flooded_kib / 1024:.0f} MiB) at a dead reader; {growth}")
    if sent < 12000:
        info("NOTE: flood is below the outbound-mailbox cap; increase --flood to test bounding")
    # Broker must survive AND its memory must NOT scale with the firehose: once the
    # bounded mailbox fills, further deliveries to the dead reader are dropped, so RSS
    # growth stays far below the total bytes flooded (here: < 50% of it).
    survived = healthy(host, port)
    bounded = (rss0 is None or rss1 is None) or ((rss1 - rss0) < flooded_kib * 0.5)
    record("slowreader", survived and bounded,
           f"{growth} vs ~{flooded_kib / 1024:.0f} MiB flooded, bounded={bounded}, healthy={survived}")


# --- 5. Fragmentation (byte-by-byte framing) --------------------------------


def scn_fragment(host, port, byte_delay):
    hdr(f"FRAGMENTATION — valid CONNECT+SUBSCRIBE+PUBLISH sent 1 byte / {byte_delay:.3f}s")
    # Subscriber (normal) to observe end-to-end reassembly.
    sub = raw_connect(host, port)
    sub.sendall(m.connect(client_id="frag-sub", keepalive=30))
    m.expect_connack_ok(sub)
    sub.sendall(m.subscribe([("frag/topic", 0x00)], pkid=1))
    m.read_packet(sub, 2.0)

    # Attacker dribbles a CONNECT then a PUBLISH one byte at a time.
    atk = raw_connect(host, port)

    def dribble(data: bytes):
        for byte in data:
            try:
                atk.sendall(bytes([byte]))
                time.sleep(byte_delay)
            except OSError:
                return False
        return True

    dribble(m.connect(client_id="frag-atk", keepalive=30))
    if not m.expect_connack_ok(atk, 5.0):
        record("fragment", healthy(host, port), "broker rejected the fragmented CONNECT")
        atk.close(); sub.close()
        return
    dribble(m.publish("frag/topic", b"reassembled-ok", qos=0))

    # Did the fragmented publish reassemble and route?
    got = m.read_packet(sub, 5.0)
    delivered = got is not None and got[0] == m.PUBLISH and b"reassembled-ok" in got[2]
    atk.close(); sub.close()
    info(f"fragmented publish {'reassembled and delivered' if delivered else 'NOT delivered'}")
    record("fragment", delivered and healthy(host, port),
           "byte-by-byte frame reassembled correctly" if delivered else "reassembly failed")


# --- 6. Malformed packet battery --------------------------------------------


def scn_malformed(host, port):
    hdr("MALFORMED PACKETS — a battery of hostile frames, each on a fresh socket")
    cid = m.connect(client_id="malf", keepalive=30)

    # (name, bytes-to-send, needs_prior_connect)
    cases = [
        ("bad protocol name", m.connect(protocol_name="XXXX"), False),
        ("bad protocol level 3", m.connect(protocol_level=3), False),
        ("remaining length claims 256 MiB", bytes([0x10]) + m.varint(256 * 1024 * 1024), False),
        ("never-terminating varint length", bytes([0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]), False),
        ("truncated CONNECT (header only)", bytes([0x10, 0x0A]), False),
        ("reserved packet type 0", bytes([0x00, 0x00]), False),
        ("reserved packet type 15", bytes([0xF0, 0x00]), False),
        ("SUBSCRIBE before CONNECT", m.subscribe([("a/b", 0x00)]), False),
        ("PUBLISH before CONNECT", m.publish("a/b", b"x"), False),
        ("publish to $SYS (spoof)", m.publish("$SYS/broker/fake", b"x", qos=1, pkid=1), True),
        ("publish to wildcard topic", m.publish("a/+/c", b"x", qos=1, pkid=1), True),
        ("publish to empty topic", m.publish("", b"x", qos=1, pkid=1), True),
        ("publish with embedded NUL", m.publish("a\x00b", b"x", qos=1, pkid=1), True),
        ("oversized payload (2 MiB)", m.publish("big/topic", os.urandom(2 * 1024 * 1024), qos=0), True),
        ("double CONNECT", cid, True),  # a second CONNECT after connecting
    ]

    handled = 0
    for name, data, need_conn in cases:
        try:
            s = raw_connect(host, port)
            if need_conn:
                s.sendall(cid)
                if not m.expect_connack_ok(s):
                    info(f"[{name}] setup CONNECT refused (ok)")
                    s.close(); handled += 1; continue
            # Send the frame under test. For "double CONNECT", data is a CONNECT, so
            # this is a second CONNECT after the setup one.
            s.sendall(data)
            # The broker should close the connection (EOF) rather than hang or crash.
            s.settimeout(3.0)
            closed = _drained_or_closed(s)
            handled += 1 if closed else 0
            info(f"[{name}] {'closed by broker' if closed else 'still open (lenient)'}")
            s.close()
        except OSError:
            handled += 1
            info(f"[{name}] connection error (acceptable)")

    record("malformed", healthy(host, port),
           f"{handled}/{len(cases)} hostile frames handled without crash, broker healthy")


def _drained_or_closed(s: socket.socket) -> bool:
    """True if the peer closed the socket (possibly after a DISCONNECT) within timeout."""
    try:
        while True:
            data = s.recv(4096)
            if data == b"":
                return True
    except (socket.timeout, BlockingIOError):
        return False
    except OSError:
        return True


# --- 7. Topic-structure abuse -----------------------------------------------


def scn_topics(host, port):
    hdr("TOPIC ABUSE — deep trees, wildcard explosion, huge topics")

    # 7a. Pathologically deep filter/topic (would blow a recursive trie / stack).
    deep_filter = "/".join(["a"] * 5000)
    deep_topic = "/".join(["b"] * 5000)
    s = raw_connect(host, port)
    s.sendall(m.connect(client_id="deep", keepalive=30))
    m.expect_connack_ok(s)
    s.sendall(m.subscribe([(deep_filter, 0x01)], pkid=1))
    sub = m.read_packet(s, 3.0)
    deep_sub_rejected = sub is not None and sub[0] == m.SUBACK and sub[2][-1] >= 0x80
    info(f"5000-level SUBSCRIBE {'rejected (SubAck failure code)' if deep_sub_rejected else 'accepted?!'}")
    s.close()

    p = raw_connect(host, port)
    p.sendall(m.connect(client_id="deep-pub", keepalive=30))
    m.expect_connack_ok(p)
    p.sendall(m.publish(deep_topic, b"x", qos=1, pkid=1))
    deep_pub_closed = _drained_or_closed(p)
    info(f"5000-level PUBLISH {'disconnected' if deep_pub_closed else 'accepted?!'}")
    p.close()

    # 7b. Wildcard explosion: one client, many overlapping wildcard subs, then a
    #     publish that matches all of them — stresses the matcher's fan-out.
    w = raw_connect(host, port)
    w.sendall(m.connect(client_id="wild", keepalive=30))
    m.expect_connack_ok(w)
    overlap = [(f"a/{'+/' * i}#", 0x00) for i in range(1, 60)] + [("#", 0x00), ("a/#", 0x00)]
    # Chunk into SUBSCRIBEs of 20 filters to stay under packet limits.
    for i in range(0, len(overlap), 20):
        w.sendall(m.subscribe(overlap[i:i + 20], pkid=i + 1))
        m.read_packet(w, 2.0)
    info(f"registered {len(overlap)} overlapping wildcard subscriptions")
    w.close()

    # 7c. Huge (but legal-length) topic name near the 64 KiB limit.
    huge = "z/" * 30000  # ~60 KiB, but that's >128 levels -> must be rejected on depth
    h = raw_connect(host, port)
    h.sendall(m.connect(client_id="huge", keepalive=30))
    m.expect_connack_ok(h)
    h.sendall(m.publish(huge.rstrip("/"), b"x", qos=1, pkid=1))
    huge_closed = _drained_or_closed(h)
    info(f"~60 KiB topic {'disconnected' if huge_closed else 'accepted?!'}")
    h.close()

    survived = healthy(host, port)
    record("topics", survived and deep_sub_rejected and deep_pub_closed,
           "deep/huge topics rejected, wildcard fan-out survived, broker healthy")


# --- 8. Throughput (asyncio, thousands of connections) ----------------------


async def _async_read_packet(reader: asyncio.StreamReader):
    head = await reader.readexactly(1)
    mult, length = 1, 0
    for _ in range(4):
        b = (await reader.readexactly(1))[0]
        length += (b & 0x7F) * mult
        if not b & 0x80:
            break
        mult *= 128
    body = await reader.readexactly(length) if length else b""
    return head[0] >> 4, body


async def _pub_worker(host, port, qos, stop_at, payload, counter, idx):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError:
        return
    writer.write(m.connect(client_id=f"tp-{idx}", keepalive=0))
    await writer.drain()
    try:
        await asyncio.wait_for(_async_read_packet(reader), timeout=5)  # CONNACK
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, OSError):
        writer.close()
        return
    pkid = 1
    loop = asyncio.get_event_loop()
    try:
        while loop.time() < stop_at:
            writer.write(m.publish("bench/topic", payload, qos=qos, pkid=pkid))
            if qos == 0:
                counter[0] += 1
            else:
                await writer.drain()
                ptype, _ = await _async_read_packet(reader)  # PUBACK / PUBREC
                if qos == 2 and ptype == m.PUBREC:
                    writer.write(bytes([0x62, 0x02]) + struct.pack("!H", pkid))  # PUBREL
                    await _async_read_packet(reader)  # PUBCOMP
                counter[0] += 1
            pkid = (pkid % 65535) + 1
            if qos == 0 and pkid % 256 == 0:
                await writer.drain()
    except (OSError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def _throughput(host, port, connections, duration, qos, payload_bytes):
    counter = [0]
    payload = os.urandom(payload_bytes)
    stop_at = asyncio.get_event_loop().time() + duration
    tasks = [
        asyncio.create_task(_pub_worker(host, port, qos, stop_at, payload, counter, i))
        for i in range(connections)
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    return counter[0]


def scn_throughput(host, port, connections, duration, qos, payload_bytes):
    hdr(f"THROUGHPUT — {connections} conns, QoS {qos}, {payload_bytes}B payload, {duration}s")
    rss0 = broker_rss_kb()
    t0 = time.time()
    total = asyncio.run(_throughput(host, port, connections, duration, qos, payload_bytes))
    dt = time.time() - t0
    rss1 = broker_rss_kb()
    rate = total / dt if dt else 0
    mib = rate * payload_bytes / (1024 * 1024)
    info(f"{total:,} publishes in {dt:.1f}s = {rate:,.0f} msg/s (~{mib:,.1f} MiB/s)")
    if rss0 and rss1:
        info(f"broker RSS {rss0 / 1024:.0f} -> {rss1 / 1024:.0f} MiB")
    record("throughput", healthy(host, port),
           f"{rate:,.0f} msg/s at QoS {qos}, broker healthy")


# --- driver ------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description="rusquitto adversarial stress suite")
    ap.add_argument("scenario",
                    choices=["idle", "churn", "slowloris", "slowreader", "fragment",
                             "malformed", "topics", "throughput", "all"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--connections", type=int, default=500)
    ap.add_argument("--duration", type=int, default=10)
    ap.add_argument("--hold", type=int, default=12)
    ap.add_argument("--qos", type=int, default=0, choices=[0, 1, 2])
    ap.add_argument("--payload", type=int, default=64, help="throughput payload bytes")
    ap.add_argument("--churn-total", type=int, default=20000)
    ap.add_argument("--concurrency", type=int, default=200)
    ap.add_argument("--flood", type=int, default=40000,
                    help="slowreader: messages to fire at the dead reader (must exceed the mailbox cap)")
    args = ap.parse_args()

    if not healthy(args.host, args.port):
        print(f"{R}Broker not reachable / not healthy at {args.host}:{args.port}{X}")
        sys.exit(2)

    s = args.scenario
    run = lambda name: s in (name, "all")

    if run("idle"):
        scn_idle(args.host, args.port, min(args.connections, 1000), args.hold)
    if run("churn"):
        scn_churn(args.host, args.port, args.churn_total, args.concurrency)
    if run("slowloris"):
        scn_slowloris(args.host, args.port, min(args.connections, 500), 1.0)
    if run("slowreader"):
        scn_slowreader(args.host, args.port, args.flood, 8)
    if run("fragment"):
        scn_fragment(args.host, args.port, 0.02)
    if run("malformed"):
        scn_malformed(args.host, args.port)
    if run("topics"):
        scn_topics(args.host, args.port)
    if run("throughput"):
        scn_throughput(args.host, args.port, args.connections, args.duration,
                       args.qos, args.payload)

    hdr("REPORT CARD")
    passed = sum(1 for _, good, _ in RESULTS if good)
    for name, good, detail in RESULTS:
        print(f"  {(G + 'PASS' + X) if good else (R + 'FAIL' + X)}  {name:<12} {detail}")
    print(f"\n{passed}/{len(RESULTS)} scenarios left the broker healthy.")
    sys.exit(0 if passed == len(RESULTS) else 1)


if __name__ == "__main__":
    main()
