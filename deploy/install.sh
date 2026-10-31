#!/bin/sh
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
PYTHON_BIN=${ANIME_PIPELINE_PYTHON:-python3}
exec "$PYTHON_BIN" "$HERE/hermes_release.py" install "$@"
