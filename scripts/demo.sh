#!/usr/bin/env bash
# Walks through every property the assessment asks for, against a real server.
#
#   ./scripts/demo.sh
#
# Uses a throwaway data directory and cleans up on exit.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PORT:-8080}"
DATA="$(mktemp -d)"
export QUEUE_ADDR="http://localhost:${PORT}"
SERVER_PID=""

cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

section() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
note()    { printf '   %s\n' "$1"; }

start_server() {
  ./bin/queued -addr ":${PORT}" -data "$DATA" >"$DATA/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    if ./bin/qctl list >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  echo "server did not come up; log follows:" >&2
  cat "$DATA/server.log" >&2
  exit 1
}

section "Build"
go build -o bin/queued ./cmd/queued
go build -o bin/qctl ./cmd/qctl
start_server
note "server up on ${QUEUE_ADDR}, data in ${DATA}"

# ---------------------------------------------------------------------------
section "1. Priority FIFO -- priority first, oldest breaks the tie"
./bin/qctl create orders -order fifo -priority
./bin/qctl send orders "checkout-1"  -priority 1  >/dev/null
./bin/qctl send orders "refund"      -priority 9  >/dev/null
./bin/qctl send orders "checkout-2"  -priority 1  >/dev/null
./bin/qctl send orders "fraud-alert" -priority 9  >/dev/null
note "sent: checkout-1(p1) refund(p9) checkout-2(p1) fraud-alert(p9)"
note "expect: refund, fraud-alert, checkout-1, checkout-2"
./bin/qctl drain orders

# ---------------------------------------------------------------------------
section "2. Priority LIFO -- same priorities, newest breaks the tie"
./bin/qctl create stack -order lifo -priority
./bin/qctl send stack "checkout-1"  -priority 1 >/dev/null
./bin/qctl send stack "refund"      -priority 9 >/dev/null
./bin/qctl send stack "checkout-2"  -priority 1 >/dev/null
./bin/qctl send stack "fraud-alert" -priority 9 >/dev/null
note "expect: fraud-alert, refund, checkout-2, checkout-1"
./bin/qctl drain stack

# ---------------------------------------------------------------------------
section "3. Delay -- hidden from the moment it is sent, not when it is read"
./bin/qctl create scheduled -order fifo -delay 2
./bin/qctl send scheduled "in-two-seconds" >/dev/null
./bin/qctl send scheduled "right-now" -delay 0 >/dev/null
note "queue default is 2s; the second message overrides it to 0s"
./bin/qctl stats scheduled
note "popping immediately:"
./bin/qctl drain scheduled
note "waiting 2s..."
sleep 2.2
note "popping again:"
./bin/qctl drain scheduled

# ---------------------------------------------------------------------------
section "4. Durability -- kill -9 the server and start it again"
./bin/qctl create durable -order fifo -priority
for i in 1 2 3 4 5; do ./bin/qctl send durable "job-$i" -priority "$i" >/dev/null; done
./bin/qctl pop durable
note "one message consumed, four left:"
./bin/qctl stats durable
note "sending SIGKILL to pid ${SERVER_PID} -- no shutdown hook, no flush"
kill -9 "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
start_server
note "restarted; the consumed message stays consumed and the rest survive:"
./bin/qctl stats durable
./bin/qctl drain durable

# ---------------------------------------------------------------------------
section "5. Concurrency -- four consumers, no message delivered twice"
./bin/qctl create work -order fifo
for i in $(seq 1 200); do ./bin/qctl send work "task-$i" >/dev/null; done
note "200 messages, 4 concurrent workers:"
./bin/qctl worker work -n 4 -for 6s -quiet

section "Done"
