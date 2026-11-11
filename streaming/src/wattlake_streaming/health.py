"""Liveness check of the producer: exits 1 if its heartbeat file is too old.

The producer touches the file at the end of every cycle, even when the API is
down. A stale file means the loop is stuck (a hung call, a deadlock): Kubernetes
then restarts the container. Run with: python -m wattlake_streaming.health
"""

import os
import sys
import time
from pathlib import Path

# Three poll intervals of 5 minutes: one slow cycle does not trigger a restart.
DEFAULT_MAX_AGE_SECONDS = 900


def is_alive(heartbeat_file: Path, max_age_seconds: float, now: float) -> bool:
    try:
        return now - heartbeat_file.stat().st_mtime <= max_age_seconds
    except FileNotFoundError:
        return False


def main() -> None:
    heartbeat_file = Path(os.environ.get("HEARTBEAT_FILE", "/tmp/heartbeat"))
    max_age = float(os.environ.get("HEARTBEAT_MAX_AGE_SECONDS", DEFAULT_MAX_AGE_SECONDS))
    sys.exit(0 if is_alive(heartbeat_file, max_age, time.time()) else 1)


if __name__ == "__main__":
    main()
