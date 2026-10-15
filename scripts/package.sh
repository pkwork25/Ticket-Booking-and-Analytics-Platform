#!/usr/bin/env bash
# Build deployable zips into ./build (pure Python, no external deps needed).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"
rm -rf "$BUILD" && mkdir -p "$BUILD"
find "$ROOT" -name "__pycache__" -type d -prune -exec rm -rf {} +

(cd "$ROOT/glue" && python3 -m zipfile -c "$BUILD/ticket_etl.zip" ticket_etl/)
(cd "$ROOT/lambdas/sqs_to_n8n" && python3 -m zipfile -c "$BUILD/sqs_to_n8n.zip" handler.py)
(cd "$ROOT/lambdas/kpi_snapshot" && python3 -m zipfile -c "$BUILD/kpi_snapshot.zip" handler.py)
ls -l "$BUILD"
