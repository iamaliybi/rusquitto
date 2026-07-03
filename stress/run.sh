#!/usr/bin/env bash
#
# One-shot orchestrator: build the broker, launch it with a stress-tuned config,
# run the adversarial suite, then tear down. Survival is the pass condition.
#
#   ./run.sh                 # build + all app-level scenarios
#   ./run.sh throughput      # a single scenario (passed straight to attack.py)
#   SKIP_BUILD=1 ./run.sh    # reuse an existing release binary
#   TARGET=x86_64-unknown-linux-gnu ./run.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
BIN="$ROOT/target/$TARGET/release/rusquitto"
HOST=127.0.0.1
PORT=1883
SCENARIO="${1:-all}"

CONF="$(mktemp /tmp/rusquitto-stress.XXXX.toml)"
LOG="$(mktemp /tmp/rusquitto-stress.XXXX.log)"
cat >"$CONF" <<'EOF'
[server]
bind = "127.0.0.1"
port = 1883
websocket = true
websocket_port = 1884

[runtime]
# use all cores to exercise the thread-per-core paths and cross-shard mesh
placement = "max-spread"

[limits]
connect_timeout = 5          # small so idle/slow-loris reaping is visible fast
max_connections_per_shard = 65536
max_payload_size = 1048576   # 1 MiB, so the oversized-payload case is > this

[logging]
enable_terminal = false
EOF

cleanup() {
  [[ -n "${BROKER_PID:-}" ]] && kill "$BROKER_PID" 2>/dev/null || true
  pkill -x rusquitto 2>/dev/null || true
  rm -f "$CONF"
  echo "broker log left at: $LOG"
}
trap cleanup EXIT

# Raise the fd ceiling so the churn/throughput scenarios aren't self-limited.
ulimit -n 1048576 2>/dev/null || ulimit -n 65536 2>/dev/null || true

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo ">> building release broker ($TARGET)"
  ( cd "$ROOT" && cargo build --release --target "$TARGET" )
fi
[[ -x "$BIN" ]] || { echo "broker binary not found: $BIN"; exit 1; }

echo ">> launching broker"
pkill -x rusquitto 2>/dev/null || true
sleep 0.3
setsid "$BIN" "$CONF" >"$LOG" 2>&1 < /dev/null &
BROKER_PID=$!
sleep 1.5
if ! kill -0 "$BROKER_PID" 2>/dev/null; then
  echo "broker failed to start:"; cat "$LOG"; exit 1
fi
echo ">> broker up (pid $BROKER_PID)"

echo ">> running scenario: $SCENARIO"
python3 "$HERE/attack.py" --host "$HOST" --port "$PORT" "$SCENARIO" \
  --connections "${CONNECTIONS:-1000}" \
  --duration "${DURATION:-10}" \
  --qos "${QOS:-1}" \
  --churn-total "${CHURN_TOTAL:-20000}" \
  --concurrency "${CONCURRENCY:-200}"
RC=$?

echo ">> post-run broker health:"
if kill -0 "$BROKER_PID" 2>/dev/null; then echo "   broker still alive ✓"; else echo "   broker DIED ✗"; RC=1; fi
grep -iE 'panic|overflow|abort' "$LOG" && { echo "   found panic/abort in log ✗"; RC=1; } || echo "   no panic/abort in log ✓"

exit $RC
