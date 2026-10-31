#!/usr/bin/env bash
# Process control for the demo. Logs and pids live in afterlife/.run.
#   scripts/demo.sh up            reset, start bridge, reviewer, standbys s1-s3, workers w1-w3, control
#   scripts/demo.sh kill-w2       kill -9 worker w2 and the bridge (a standby takes t2 over anyway)
#   scripts/demo.sh bridge        start the bridge again
#   scripts/demo.sh stop-w3 / cont-w3   freeze and resume worker w3 (zombie write)
#   scripts/demo.sh hang-w1       w1 keeps heartbeating but stops progressing (the actor calls it stuck)
#   scripts/demo.sh down          stop the run (the control server keeps running for the board)
#   scripts/demo.sh stop-all      stop the run and the control server
set -euo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$SELF")/.."
RUN=../.run
mkdir -p "$RUN/pids" "$RUN/logs"
start() { # name, command...
  local name=$1; shift
  local old; old=$(cat "$RUN/pids/$name" 2>/dev/null || true)
  if [ -n "$old" ] && kill -0 "$old" 2>/dev/null; then echo "$name already running ($old)"; return 0; fi
  echo "--- start $(date '+%H:%M:%S') ---" >>"$RUN/logs/$name.log"
  nohup "$@" >>"$RUN/logs/$name.log" 2>&1 &
  echo $! >"$RUN/pids/$name"
  echo "started $name (pid $!)"
}
pid() { cat "$RUN/pids/$1" 2>/dev/null || true; }
# The control server lives outside pids/ so `down` (and a new run started from the board) keeps it.
control() {
  local old; old=$(cat "$RUN/control.pid" 2>/dev/null || true)
  if [ -n "$old" ] && kill -0 "$old" 2>/dev/null; then return 0; fi
  nohup bun run scripts/control.ts >>"$RUN/logs/control.log" 2>&1 &
  echo $! >"$RUN/control.pid"
}
case "${1:-}" in
  up)
    "$SELF" down >/dev/null 2>&1 || echo "warning: down failed" >&2
    bun run scripts/reset.ts
    start bridge bun run bridge/index.ts
    if [ "${REVIEWER:-1}" != "0" ]; then start reviewer bun run reviewer/index.ts; fi
    start s1 bun run worker/index.ts standby s1
    start s2 bun run worker/index.ts standby s2
    start s3 bun run worker/index.ts standby s3
    sleep 2
    start w1 bun run worker/index.ts worker w1 t1
    start w2 bun run worker/index.ts worker w2 t2
    start w3 bun run worker/index.ts worker w3 t3
    control
    echo "board: http://127.0.0.1:${BRIDGE_PORT:-3000}"
    ;;
  kill-w2) kill -9 "$(pid w2)" "$(pid bridge)" && echo "killed w2 and bridge" ;;
  bridge) start bridge bun run bridge/index.ts ;;
  stop-w3) kill -STOP "$(pid w3)" && echo "w3 frozen" ;;
  cont-w3) kill -CONT "$(pid w3)" && echo "w3 resumed" ;;
  hang-w1) kill -USR1 "$(pid w1)" && echo "w1 hanging" ;;
  down)
    for f in "$RUN"/pids/*; do [ -e "$f" ] || continue; kill -CONT "$(cat "$f")" 2>/dev/null || true; kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
    echo "stopped"
    ;;
  stop-all)
    "$SELF" down
    kill "$(cat "$RUN/control.pid" 2>/dev/null)" 2>/dev/null || true
    rm -f "$RUN/control.pid"
    ;;
  *) sed -n 2,9p "$SELF"; exit 1 ;;
esac
