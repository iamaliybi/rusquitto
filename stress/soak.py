#!/usr/bin/env python3
"""Memory-stability soak test for rusquitto.

Runs repeating adversarial cycles against a broker and samples its RSS, then
judges stability: after a warm-up, resident memory must plateau — a steady
upward trend means a leak (or unbounded fragmentation) and fails the run.

Each cycle:
  1. churn    — open a wave of connections, half subscribe, then all disconnect
                (exercises session create/suspend/expire and allocator reuse)
  2. flood    — persistent subscribers receive a QoS 0/1 publish flood
  3. stall    — a group of subscribers stops reading while the flood continues
                (exercises the mailbox drop-on-full bound and flush ceiling)
  4. recover  — stalled sockets close; memory should return to the plateau

Usage:
  python3 soak.py [--minutes 30] [--conns 500] [--host 127.0.0.1] [--port 1885]

Exit code 0 = stable, 1 = leak suspected or broker died. RSS samples are
written to soak_rss.csv for plotting.
"""

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import mqttwire as mw


def broker_pid() -> int:
    return int(subprocess.check_output(["pgrep", "-x", "rusquitto"]).split()[0])


def rss_kb(pid: int) -> int:
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def open_conn(host: str, port: int, cid: str, keepalive: int = 600) -> socket.socket:
    s = socket.create_connection((host, port), timeout=5)
    s.sendall(mw.connect(client_id=cid, keepalive=keepalive))
    if not mw.expect_connack_ok(s):
        raise RuntimeError(f"CONNACK failed for {cid}")
    return s


def drain(sock: socket.socket, seconds: float):
    """Read and discard whatever the broker sends for `seconds`."""
    sock.settimeout(0.05)
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            if not sock.recv(65536):
                return
        except socket.timeout:
            pass
        except OSError:
            return


def cycle(host: str, port: int, n: int, cycle_no: int):
    # 1) churn: a wave of short-lived sessions.
    wave = []
    for i in range(n):
        try:
            wave.append(open_conn(host, port, f"churn-{cycle_no}-{i}"))
        except OSError:
            break
    for i, s in enumerate(wave[: n // 2]):
        s.sendall(mw.subscribe([("soak/#", 1)], pkid=1))
    time.sleep(0.5)
    for s in wave:
        try:
            s.sendall(mw.DISCONNECT)
            s.close()
        except OSError:
            pass

    # 2) flood into persistent subscribers.
    subs = [open_conn(host, port, f"sub-{cycle_no}-{i}") for i in range(20)]
    for i, s in enumerate(subs):
        s.sendall(mw.subscribe([("soak/#", 0)], pkid=1))
        mw.read_packet(s, timeout=2)
    pub = open_conn(host, port, f"pub-{cycle_no}")
    payload = b"s" * 512
    for i in range(2000):
        pub.sendall(mw.publish(f"soak/t{i % 8}", payload, qos=0))
    # Healthy subscribers drain; the last 5 stall (stop reading entirely).
    for s in subs[:15]:
        drain(s, 1.0)
    # 3) stall: keep publishing at the stalled group.
    for i in range(2000):
        pub.sendall(mw.publish(f"soak/t{i % 8}", payload, qos=0))
    time.sleep(1)

    # 4) recover: everything closes.
    for s in subs:
        try:
            s.close()
        except OSError:
            pass
    pub.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=30)
    ap.add_argument("--conns", type=int, default=500)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1885)
    args = ap.parse_args()

    pid = broker_pid()
    t_end = time.time() + args.minutes * 60
    samples: list[tuple[float, int]] = []  # (minutes elapsed, RSS KiB)
    t0 = time.time()
    cycle_no = 0

    print(f"soaking pid {pid} for {args.minutes} min ({args.conns} conns/cycle)")
    while time.time() < t_end:
        cycle(args.host, args.port, args.conns, cycle_no)
        cycle_no += 1
        elapsed = (time.time() - t0) / 60
        rss = rss_kb(pid)
        if rss == 0:
            print("FAIL: broker process died")
            return 1
        samples.append((elapsed, rss))
        print(f"  cycle {cycle_no:3d}  t={elapsed:6.1f}m  RSS={rss} KiB")

    Path("soak_rss.csv").write_text("minutes,rss_kb\n" + "\n".join(f"{t:.2f},{r}" for t, r in samples))

    # Verdict: compare the mean RSS of the 2nd and 4th quarters (post-warm-up).
    # A stable broker plateaus; sustained growth beyond 10% flags a leak.
    if len(samples) < 8:
        print("WARN: too few cycles for a verdict; run longer")
        return 0
    q = len(samples) // 4
    early = sum(r for _, r in samples[q : 2 * q]) / q
    late = sum(r for _, r in samples[3 * q :]) / len(samples[3 * q :])
    growth = (late - early) / early * 100
    print(f"RSS mean: 2nd quarter {early:.0f} KiB -> 4th quarter {late:.0f} KiB ({growth:+.1f}%)")
    if growth > 10:
        print("FAIL: sustained RSS growth suggests a leak or unbounded fragmentation")
        return 1
    print("PASS: memory is stable under adversarial churn")
    return 0


if __name__ == "__main__":
    sys.exit(main())
