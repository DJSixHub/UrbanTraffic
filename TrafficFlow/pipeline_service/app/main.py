"""Entrypoint for the traffic flow Kafka pipeline."""
from __future__ import annotations

import sys

from .main2 import Config, LOG, load_config, run_pipeline


__all__ = [
    "Config",
    "LOG",
    "load_config",
    "run_pipeline",
    "main",
]


def main() -> int:
    try:
        config = load_config()
    except SystemExit as exc:
        LOG.error(str(exc))
        return exc.code if isinstance(exc.code, int) else 1
    return run_pipeline(config)


if __name__ == "__main__":
    sys.exit(main())

