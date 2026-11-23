"""When the dbt_transformations DAG compacts and fully rebuilds silver and gold.

Silver and gold are built incrementally (docs/adr/017-reconstruction-incrementale.md):
every hourly run merges a few rows, which leaves small files and delete files.

- Once a day, the 11:00 UTC run compacts them after publishing. Midday, when
  the laptop is usually on, as for the bronze compaction (ADR 013). With
  catchup=False, a laptop that wakes up after 12:00 skips that day's
  compaction: the next day's catches up.
- Once a week, the Sunday 11:00 UTC run rebuilds them in full instead of
  merging, as a safety net for what the incremental logic cannot see (a
  quarter hour removed from bronze). A manual run triggered with the
  configuration {"full_refresh": true} does too.

Plain functions of the run's logical time (run_after), so that the DAG stays
declarative and the rules are unit-tested without Airflow.
"""

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

MAINTENANCE_HOUR_UTC = 11
FULL_REFRESH_WEEKDAY = 6  # Sunday, datetime.weekday()


def _utc(run_after: datetime) -> datetime:
    if run_after.tzinfo is None:
        raise ValueError(f"run_after must be timezone-aware: {run_after}")
    return run_after.astimezone(UTC)


def is_compaction_run(run_after: datetime) -> bool:
    """The run that compacts silver and gold: the one of 11:00 UTC."""
    return _utc(run_after).hour == MAINTENANCE_HOUR_UTC


def is_full_refresh_run(run_after: datetime, conf: Mapping[str, Any] | None = None) -> bool:
    """The run that rebuilds silver and gold in full: Sunday 11:00 UTC, or asked for."""
    if conf and conf.get("full_refresh") is True:
        return True
    utc = _utc(run_after)
    return utc.weekday() == FULL_REFRESH_WEEKDAY and utc.hour == MAINTENANCE_HOUR_UTC


def dbt_full_refresh_flag(run_after: datetime, conf: Mapping[str, Any] | None = None) -> str:
    """The dbt build option for this run: "--full-refresh" or nothing."""
    return "--full-refresh" if is_full_refresh_run(run_after, conf) else ""
