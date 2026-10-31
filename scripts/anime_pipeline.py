#!/usr/bin/env python3
"""Compatibility entrypoint delegating every command to the V5 CLI."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from run_pipeline import (  # noqa: E402
    _parse_backfill as _v5_parse_backfill,
    build_executor,
    main,
)


def _parse_backfill(value):
    """Keep the legacy helper's SystemExit contract for old callers."""

    try:
        return _v5_parse_backfill(value)
    except ValueError as exc:
        raise SystemExit(str(exc)) from None


if __name__ == "__main__":
    raise SystemExit(main())
