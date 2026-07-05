#!/usr/bin/env python3
"""Per-connection memory measurement for rusquitto.

Phase 1 (idle): open N idle MQTT connections, report RSS and VmSize delta per conn.
Phase 2 (--burst): subscribe half the connections but stop reading their sockets,
then flood publishes so their mailboxes/kernel buffers fill; report RSS again.
This is the adversarial case: pages touched under load stay resident.
"""

import socket
import struct
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
import mqttwire as mw

HOST = "127.0.0.1"
PORT = 1885  # override with --port N


def broker_pid() -> int:
    import subprocess

    out = subprocess.check_output(["pgrep", "-x", "rusquitto"]).split()
    return int(out[0])


def mem_kb(pid: int) -> tuple[int, int]:
    """(VmRSS, VmSize) in KiB."""
    rss = size = 0
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
            elif line.startswith("VmSize:"):
                size = int(line.split()[1])
    return rss, size


def open_conn(cid: str) -> socket.socket:
    s = socket.create_connection((HOST, PORT), timeout=5)
    s.sendall(mw.connect(client_id=cid, keepalive=300))
    if not mw.expect_connack_ok(s):
        raise RuntimeError(f"CONNACK failed for {cid}")
    return s


def main():
    global PORT
    argv = sys.argv[1:]
    burst = "--burst" in argv
    if "--port" in argv:
        i = argv.index("--port")
        PORT = int(argv[i + 1])
        del argv[i : i + 2]
    args = [a for a in argv if not a.startswith("--")]
    n = int(args[0]) if args else 2000

    pid = broker_pid()
    rss0, size0 = mem_kb(pid)
    print(f"baseline: RSS {rss0} KiB, VmSize {size0} KiB")

    conns = []
    t0 = time.time()
    for i in range(n):
        conns.append(open_conn(f"m{i}"))
    print(f"opened {n} idle connections in {time.time() - t0:.1f}s")

    time.sleep(2)  # let allocations settle
    rss1, size1 = mem_kb(pid)
    print(f"after connect: RSS {rss1} KiB (+{rss1 - rss0}), VmSize {size1} KiB (+{size1 - size0})")
    print(f"==> idle RSS/conn:    {(rss1 - rss0) / n:8.2f} KiB")
    print(f"==> idle VmSize/conn: {(size1 - size0) / n:8.2f} KiB")

    if burst:
        # Half the connections subscribe, then never read their sockets again.
        half = n // 2
        for i, s in enumerate(conns[:half]):
            s.sendall(mw.subscribe([("bench/#", 0)], pkid=1))
        # Drain SUBACKs so the subscribe itself isn't what stalls.
        for s in conns[:half]:
            mw.read_packet(s, timeout=3)
        print(f"{half} connections subscribed to bench/# and stopped reading")

        # Flood from one publisher: stalled subscribers' outbound paths fill up.
        pub = open_conn("publisher")
        payload = b"x" * 1024
        t0 = time.time()
        for i in range(3000):
            pub.sendall(mw.publish(f"bench/t{i % 16}", payload, qos=0))
        print(f"published 3000x1KiB in {time.time() - t0:.1f}s")

        time.sleep(3)
        rss2, size2 = mem_kb(pid)
        print(f"after burst: RSS {rss2} KiB (+{rss2 - rss1} over idle)")
        print(f"==> burst RSS/conn:   {(rss2 - rss0) / n:8.2f} KiB")

        # Do stalled pages come back? Close everything and re-measure.
        for s in conns:
            try:
                s.close()
            except OSError:
                pass
        pub.close()
        time.sleep(4)
        rss3, _ = mem_kb(pid)
        print(f"after close-all: RSS {rss3} KiB (delta vs baseline: +{rss3 - rss0})")
    else:
        for s in conns:
            s.close()


if __name__ == "__main__":
    main()
