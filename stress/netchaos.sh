#!/usr/bin/env bash
#
# Kernel/TCP-level network chaos for rusquitto. These operate below the MQTT
# layer (traffic shaping, SYN floods, RST storms) and therefore need root and,
# for some actions, `tc` (iproute2) and `hping3`. Run the app-level attacks in
# attack.py separately.
#
#   sudo ./netchaos.sh latency 200 50     # 200ms +/- 50ms jitter on loopback
#   sudo ./netchaos.sh loss 10            # drop 10% of packets
#   sudo ./netchaos.sh corrupt 5          # corrupt 5% of packets
#   sudo ./netchaos.sh synflood 1883 20   # 20s SYN flood (hping3)
#   sudo ./netchaos.sh halfopen 1883 2000 # 2000 half-open connections
#   sudo ./netchaos.sh reset              # remove all shaping
#
# On WSL2, loopback shaping via netem is limited; prefer a real Linux host or a
# veth pair for faithful results.
set -euo pipefail

IFACE="${IFACE:-lo}"
CMD="${1:-help}"

need_root() { [[ $EUID -eq 0 ]] || { echo "must run as root"; exit 1; }; }
have()      { command -v "$1" >/dev/null 2>&1; }

reset_tc() { tc qdisc del dev "$IFACE" root 2>/dev/null || true; }

case "$CMD" in
  latency)   # $2 = ms delay, $3 = ms jitter
    need_root; have tc || { echo "install iproute2 (tc)"; exit 1; }
    reset_tc
    tc qdisc add dev "$IFACE" root netem delay "${2:-100}ms" "${3:-0}ms" distribution normal
    echo "netem: ${2:-100}ms +/- ${3:-0}ms jitter on $IFACE" ;;

  loss)      # $2 = percent
    need_root; have tc || { echo "install iproute2 (tc)"; exit 1; }
    reset_tc
    tc qdisc add dev "$IFACE" root netem loss "${2:-10}%"
    echo "netem: ${2:-10}% packet loss on $IFACE" ;;

  corrupt)   # $2 = percent
    need_root; have tc || { echo "install iproute2 (tc)"; exit 1; }
    reset_tc
    tc qdisc add dev "$IFACE" root netem corrupt "${2:-5}%"
    echo "netem: ${2:-5}% packet corruption on $IFACE" ;;

  reorder)   # $2 = ms delay, $3 = percent reordered
    need_root; have tc || { echo "install iproute2 (tc)"; exit 1; }
    reset_tc
    tc qdisc add dev "$IFACE" root netem delay "${2:-50}ms" reorder "${3:-25}%" 50%
    echo "netem: reordering ${3:-25}% of packets on $IFACE" ;;

  synflood)  # $2 = port, $3 = seconds
    need_root; have hping3 || { echo "install hping3"; exit 1; }
    echo "SYN flood -> 127.0.0.1:${2:-1883} for ${3:-15}s (rely on tcp_syncookies)"
    timeout "${3:-15}" hping3 -S -p "${2:-1883}" --flood 127.0.0.1 || true ;;

  halfopen)  # $2 = port, $3 = count — open then abandon, exercising accept/backlog
    need_root; have hping3 || { echo "install hping3"; exit 1; }
    echo "sending ${3:-2000} bare SYNs to 127.0.0.1:${2:-1883} (half-open)"
    hping3 -S -p "${2:-1883}" -c "${3:-2000}" -i u200 127.0.0.1 || true ;;

  rstflood)  # $2 = port — RST storm
    need_root; have hping3 || { echo "install hping3"; exit 1; }
    timeout "${3:-10}" hping3 -R -p "${2:-1883}" --flood 127.0.0.1 || true ;;

  reset)
    need_root; reset_tc; echo "cleared shaping on $IFACE" ;;

  *)
    grep -E '^#( |$)' "$0" | sed 's/^# \{0,1\}//' ;;
esac
