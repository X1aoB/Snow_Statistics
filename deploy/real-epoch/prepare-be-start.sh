#!/bin/sh
# The digest-locked Doris image ships an executable 2.67 GiB binary. Its startup
# chmod copies the whole file into every overlay. Patch only that verified line.
set -eu
script=/opt/apache-doris/be/bin/start_be.sh
binary=/opt/apache-doris/be/lib/doris_be
original=d901a0cb6f13b3016d65fe3ccfdb3832a0dd88352f96cea99516bfc1076d906b
patched=dcb0d8265e282cc1deec46ac039ea913df0833b877fe2a703f7e314a65954107
input=$(sha256sum "$script" | cut -d ' ' -f 1)
test -f "$binary" && test ! -L "$binary" && test -x "$binary"
test "$(stat -c '%s' "$binary")" = 2867606656
mode=$(stat -c '%a' "$binary")
case "$mode" in 755|550) ;; *) exit 78 ;; esac
case "$input" in
    "$original")
        temporary=$(mktemp /tmp/snow-statistics-be-start.XXXXXX)
        trap 'rm -f "$temporary"' EXIT HUP INT TERM
        awk '
          $0 == "chmod 550 \"${DORIS_HOME}/lib/doris_be\"" {
            print "if [[ ! -x \"${DORIS_HOME}/lib/doris_be\" ]]; then chmod 550 \"${DORIS_HOME}/lib/doris_be\"; fi"
            count++; next
          }
          { print }
          END { if (count != 1) exit 78 }
        ' "$script" > "$temporary"
        test "$(sha256sum "$temporary" | cut -d ' ' -f 1)" = "$patched"
        cat "$temporary" > "$script"
        ;;
    "$patched") ;;
    *) exit 78 ;;
esac
test "$(sha256sum "$script" | cut -d ' ' -f 1)" = "$patched"
printf '{"component":"snow_be_startup","input_sha256":"%s","output_sha256":"%s","binary_mode":"%s","binary_bytes":2867606656}\n' "$input" "$patched" "$mode"
