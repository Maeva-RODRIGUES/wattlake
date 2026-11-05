from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest
from respx import MockRouter, Route

from wattlake_ingestion.odre import BASE_URL, HISTORY_DATASET, OdreClient, OdreError, period_filter

EXPORT_URL = f"{BASE_URL}/catalog/datasets/{HISTORY_DATASET}/exports/parquet"
START = datetime(2026, 8, 1, tzinfo=UTC)
END = datetime(2026, 8, 2, tzinfo=UTC)
PAYLOAD = b"PAR1 fake parquet content PAR1"


class BrokenStream(httpx.SyncByteStream):
    """A response body cut off by the network after its first bytes."""

    def __iter__(self) -> Iterator[bytes]:
        yield b"PAR1 partial"
        raise httpx.ReadError("connection reset")


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(sleeps: list[float]) -> Iterator[OdreClient]:
    with OdreClient(max_attempts=3, backoff_seconds=2.0, sleep=sleeps.append) as odre:
        yield odre


@pytest.fixture
def export(respx_mock: MockRouter) -> Route:
    """The mocked export endpoint: no test reaches the real API."""
    return respx_mock.get(EXPORT_URL)


@pytest.fixture
def dest(tmp_path: Path) -> Path:
    return tmp_path / "day.parquet"


def files_in(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.iterdir())


class TestPeriodFilter:
    def test_bounds_are_converted_to_utc(self) -> None:
        paris = ZoneInfo("Europe/Paris")
        where = period_filter(datetime(2026, 8, 1, tzinfo=paris), datetime(2026, 8, 2, tzinfo=paris))
        assert where == (
            "date_heure >= date'2026-07-31T22:00:00+00:00' "
            "and date_heure < date'2026-08-01T22:00:00+00:00'"
        )

    def test_naive_datetime_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            period_filter(datetime(2026, 8, 1), END)

    @pytest.mark.parametrize("end", [START, datetime(2026, 7, 31, tzinfo=UTC)])
    def test_empty_or_reversed_period_is_rejected(self, end: datetime) -> None:
        with pytest.raises(ValueError, match="must be before"):
            period_filter(START, end)


class TestExportToFile:
    def test_downloads_the_period_to_dest(self, client: OdreClient, export: Route, dest: Path) -> None:
        export.respond(200, content=PAYLOAD)

        assert client.export_to_file(HISTORY_DATASET, START, END, dest) == dest

        assert dest.read_bytes() == PAYLOAD
        assert files_in(dest.parent) == ["day.parquet"]
        assert export.calls.last.request.url.params["where"] == period_filter(START, END)

    def test_retries_a_server_error_with_exponential_backoff(
        self, client: OdreClient, export: Route, dest: Path, sleeps: list[float]
    ) -> None:
        export.side_effect = [httpx.Response(503), httpx.Response(502), httpx.Response(200, content=PAYLOAD)]

        client.export_to_file(HISTORY_DATASET, START, END, dest)

        assert sleeps == [2.0, 4.0]
        assert dest.read_bytes() == PAYLOAD

    def test_honours_retry_after_on_rate_limiting(
        self, client: OdreClient, export: Route, dest: Path, sleeps: list[float]
    ) -> None:
        export.side_effect = [
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, content=PAYLOAD),
        ]

        client.export_to_file(HISTORY_DATASET, START, END, dest)

        assert sleeps == [7.0]

    def test_retries_connection_errors(
        self, client: OdreClient, export: Route, dest: Path, sleeps: list[float]
    ) -> None:
        export.side_effect = [httpx.ConnectError("unreachable"), httpx.Response(200, content=PAYLOAD)]

        client.export_to_file(HISTORY_DATASET, START, END, dest)

        assert sleeps == [2.0]

    def test_a_cut_download_is_restarted_from_scratch(
        self, client: OdreClient, export: Route, dest: Path
    ) -> None:
        export.side_effect = [httpx.Response(200, stream=BrokenStream()), httpx.Response(200, content=PAYLOAD)]

        client.export_to_file(HISTORY_DATASET, START, END, dest)

        assert dest.read_bytes() == PAYLOAD
        assert files_in(dest.parent) == ["day.parquet"]

    def test_gives_up_after_max_attempts_without_leaving_files(
        self, client: OdreClient, export: Route, dest: Path, sleeps: list[float]
    ) -> None:
        export.side_effect = [
            httpx.Response(200, stream=BrokenStream()),
            httpx.Response(503),
            httpx.Response(503),
        ]

        with pytest.raises(OdreError, match="gave up after 3 attempts, last failure: HTTP 503"):
            client.export_to_file(HISTORY_DATASET, START, END, dest)

        assert export.call_count == 3
        assert sleeps == [2.0, 4.0]
        assert files_in(dest.parent) == []

    def test_client_errors_are_not_retried(
        self, client: OdreClient, export: Route, dest: Path, sleeps: list[float]
    ) -> None:
        export.respond(400, text="ODSQL query is malformed")

        with pytest.raises(OdreError, match="HTTP 400: ODSQL query is malformed"):
            client.export_to_file(HISTORY_DATASET, START, END, dest)

        assert export.call_count == 1
        assert sleeps == []
        assert files_in(dest.parent) == []
