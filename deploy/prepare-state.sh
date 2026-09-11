#!/usr/bin/env bash
set -euo pipefail
# New dedicated filesystem only; existing data/volumes are never resized.
target=/var/lib/snow-statistics
size_mib=${SNOW_STATE_SIZE_MIB:-512}
case "$size_mib" in 512|1024|2048) ;; *) echo 'Use 512, 1024 or 2048 MiB for a new state volume.' >&2; exit 1 ;; esac
if [[ -e "$target/state.img" || -L "$target/state.img" || -L "$target" || -L "$target/state" ]] || mountpoint -q "$target/state" ||
   [[ -d "$target/state" && -n "$(find "$target/state" -mindepth 1 -print -quit)" ]]; then
  echo 'Existing state found; refusing to reformat.' >&2
  exit 1
fi
install -d -m 0700 "$target" "$target/state"
fallocate -l "${size_mib}M" "$target/state.img"
mkfs.ext4 -F -m 0 "$target/state.img"
mount -o loop,nodev,nosuid,noexec "$target/state.img" "$target/state"
chown 10001:10001 "$target/state"
echo "Mounted dedicated ${size_mib} MiB state filesystem; match SNOW_BUDGET_BYTES and record a persistent mount before enabling restart."
