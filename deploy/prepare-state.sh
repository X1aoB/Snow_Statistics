#!/usr/bin/env bash
set -euo pipefail
# Dedicated 2 GiB filesystem. No deletion, shared Docker pruning or business volumes.
target=/var/lib/snow-statistics
if [[ -e "$target/state.img" || -e "$target/state/statistics.db" ]]; then
  echo 'Existing state found; refusing to reformat.' >&2
  exit 1
fi
install -d -m 0700 "$target" "$target/state"
fallocate -l 2G "$target/state.img"
mkfs.ext4 -F -m 0 "$target/state.img"
mount -o loop,nodev,nosuid,noexec "$target/state.img" "$target/state"
chown 10001:10001 "$target/state"
echo 'Mounted dedicated 2 GiB state filesystem; record a persistent mount before enabling restart.'
