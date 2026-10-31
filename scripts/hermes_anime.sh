#!/bin/sh
set -eu
case "$0" in
    */*) SCRIPT_PARENT=${0%/*} ;;
    *) SCRIPT_PARENT=. ;;
esac
SCRIPT_DIR=$(CDPATH= cd -- "$SCRIPT_PARENT" && pwd -P)
if [ -f "$SCRIPT_DIR/hermes_runtime.py" ]; then
    RUNTIME="$SCRIPT_DIR/hermes_runtime.py"
else
    RUNTIME="$SCRIPT_DIR/../scripts/hermes_runtime.py"
fi
PYTHON_BIN=${ANIME_PIPELINE_PYTHON:-python3}
"$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 64)'
# -B keeps the immutable release tree free of __pycache__ bytecode.
exec "$PYTHON_BIN" -B "$RUNTIME" "$@"
