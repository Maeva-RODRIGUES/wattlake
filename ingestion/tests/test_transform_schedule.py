from datetime import UTC, datetime, timedelta, timezone

import pytest

from wattlake_ingestion.transform_schedule import dbt_full_refresh_flag, is_compaction_run, is_full_refresh_run

PARIS_WINTER = timezone(timedelta(hours=1))
SUNDAY_11 = datetime(2026, 11, 22, 11, 0, tzinfo=UTC)
MONDAY_11 = datetime(2026, 11, 23, 11, 0, tzinfo=UTC)


class TestIsCompactionRun:
    def test_the_11_utc_run_compacts(self) -> None:
        assert is_compaction_run(MONDAY_11)

    @pytest.mark.parametrize("hour", [0, 10, 12, 23])
    def test_other_hours_do_not(self, hour: int) -> None:
        assert not is_compaction_run(MONDAY_11.replace(hour=hour))

    def test_the_hour_is_read_in_utc(self) -> None:
        # 12:00 in Paris (winter) is 11:00 UTC.
        assert is_compaction_run(datetime(2026, 11, 23, 12, 0, tzinfo=PARIS_WINTER))

    def test_a_naive_time_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            is_compaction_run(datetime(2026, 11, 23, 11, 0))


class TestIsFullRefreshRun:
    def test_sunday_11_utc_rebuilds(self) -> None:
        assert is_full_refresh_run(SUNDAY_11)

    def test_other_sunday_hours_do_not(self) -> None:
        assert not is_full_refresh_run(SUNDAY_11.replace(hour=12))

    def test_weekdays_at_11_do_not(self) -> None:
        assert not is_full_refresh_run(MONDAY_11)

    def test_a_run_triggered_with_full_refresh_rebuilds(self) -> None:
        assert is_full_refresh_run(MONDAY_11.replace(hour=15), {"full_refresh": True})

    @pytest.mark.parametrize("conf", [None, {}, {"full_refresh": False}, {"full_refresh": "true"}])
    def test_anything_else_in_the_configuration_does_not(self, conf: dict | None) -> None:
        # Only the boolean true: a string "true" from a hand-written JSON is ignored.
        assert not is_full_refresh_run(MONDAY_11.replace(hour=15), conf)


class TestDbtFullRefreshFlag:
    def test_flag_on_full_refresh_runs(self) -> None:
        assert dbt_full_refresh_flag(SUNDAY_11) == "--full-refresh"

    def test_empty_otherwise(self) -> None:
        assert dbt_full_refresh_flag(MONDAY_11) == ""
