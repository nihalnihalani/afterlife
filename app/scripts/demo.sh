#!/usr/bin/env bash
# Process control for the demo. Logs and pids live in afterlife/.run.
#   scripts/demo.sh up            reset, start bridge + standbys + workers
#   scripts/demo.sh kill-w2       kill -9 worker w2 and the bridge (the lease keeps ticking in the actor)
#   scripts/demo.sh bridge        start the bridge again
#   scripts/demo.sh stop-w3 / cont-w3   freeze and resume worker w3 (zombie write)
#   scripts/demo.sh down          stop everything
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
case "${1:-}" in
  up)
    "$SELF" down >/dev/null 2>&1 || echo "warning: down failed" >&2
    bun run scripts/reset.ts
    start bridge bun run bridge/index.ts
    start s1 bun run worker/index.ts standby s1
    start s2 bun run worker/index.ts standby s2
    sleep 2
    start w1 bun run worker/index.ts worker w1 t1
    start w2 bun run worker/index.ts worker w2 t2
    start w3 bun run worker/index.ts worker w3 t3
    echo "board: http://127.0.0.1:${BRIDGE_PORT:-3000}"
    ;;
  kill-w2) kill -9 "$(pid w2)" "$(pid bridge)" && echo "killed w2 and bridge" ;;
  bridge) start bridge bun run bridge/index.ts ;;
  stop-w3) kill -STOP "$(pid w3)" && echo "w3 frozen" ;;
  cont-w3) kill -CONT "$(pid w3)" && echo "w3 resumed" ;;
  down)
    for f in "$RUN"/pids/*; do [ -e "$f" ] || continue; kill -CONT "$(cat "$f")" 2>/dev/null || true; kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
    echo "stopped"
    ;;
  *) sed -n 2,8p "$SELF"; exit 1 ;;
esac
