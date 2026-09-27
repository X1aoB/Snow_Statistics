#!/bin/sh
# The deadline is frozen from original acceptance, never container creation time.
set -eu
case "${SNOW_EPOCH_EXPIRES_UNIX-}" in ''|*[!0-9]*) exit 78 ;; esac
test "$(date +%s)" -lt "$SNOW_EPOCH_EXPIRES_UNIX" || exit 78
"$@" &
child=$!
stop_child() {
  kill -TERM "$child" 2>/dev/null || true
  sleep 2
  kill -KILL "$child" 2>/dev/null || true
}
trap 'stop_child; exit 0' TERM INT
while kill -0 "$child" 2>/dev/null; do
  if test "$(date +%s)" -ge "$SNOW_EPOCH_EXPIRES_UNIX"; then
    stop_child
    exit 78
  fi
  sleep 1
done
wait "$child"
