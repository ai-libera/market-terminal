#!/bin/sh
# Builds every Go program and every Qt app of this repo into build/bin/.
set -eu
cd "$(dirname "$0")"

QMAKE=${QMAKE:-$(command -v qmake6 || command -v qmake-qt5 || command -v qmake || true)}
JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)

mkdir -p build/bin

for dir in */; do
  dir=${dir%/}
  case "$dir" in build|common) continue ;; esac
  if ls "$dir"/*.go >/dev/null 2>&1 && grep -qs '^package main' "$dir"/*.go; then
    echo "go build $dir"
    go build -o "build/bin/$dir" "./$dir"
  fi
done

for dir in */; do
  dir=${dir%/}
  [ -f "$dir/$dir.pro" ] || continue
  if [ -z "$QMAKE" ]; then
    echo "qmake not found, skipping $dir (set QMAKE=/path/to/qmake)" >&2
    exit 1
  fi
  echo "qmake $dir"
  mkdir -p "build/qmake/$dir"
  (cd "build/qmake/$dir" && "$QMAKE" "../../../$dir/$dir.pro" && make -j"$JOBS")
done
