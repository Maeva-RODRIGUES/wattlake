import json
from datetime import UTC, datetime, timedelta, timezone

import httpx
import pytest
import respx

from wattlake_streaming.odre import BASE_URL, DATASET, OdreClient, OdreError, measured_rows_filter

METADATA_URL = f"{BASE_URL}/catalog/datasets/{DATASET}"
EXPORT_URL = f"{BASE_URL}/catalog/datasets/{DATASET}/exports/jsonl"


def client(sleeps: list[float] | None = None) -> OdreClient:
    return OdreClient(sleep=(sleeps.append if sleeps is not None else lambda _: None))


def jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(row) + "\n" for row in rows)


def test_filter_without_since_selects_all_measured_rows():
    assert measured_rows_filter(None) == "consommation is not null"


def test_filter_converts_since_to_utc():
    paris = timezone(timedelta(hours=1))
    since = datetime(2026, 11, 11, 17, 0, tzinfo=paris)
    assert measured_rows_filter(since) == "date_heure >= date'2026-11-11T16:00:00+00:00' and consommation is not null"


def test_filter_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        measured_rows_filter(datetime(2026, 11, 11))


@respx.mock
def test_updated_at_reads_data_processed():
    respx.get(METADATA_URL).respond(json={"metas": {"default": {"data_processed": "2026-11-11T16:15:07+00:00"}}})
    with client() as odre:
        assert odre.updated_at() == "2026-11-11T16:15:07+00:00"


@respx.mock
def test_updated_at_fails_without_data_processed():
    respx.get(METADATA_URL).respond(json={"metas": {}})
    with client() as odre, pytest.raises(OdreError, match="data_processed"):
        odre.updated_at()


@respx.mock
def test_measured_rows_sends_filter_and_order_and_parses_lines():
    rows = [{"date_heure": "2026-11-11T15:45:00+00:00"}, {"date_heure": "2026-11-11T16:00:00+00:00"}]
    route = respx.get(EXPORT_URL).respond(text=jsonl(rows))
    since = datetime(2026, 11, 11, 13, 0, tzinfo=UTC)

    with client() as odre:
        assert odre.measured_rows(since) == rows

    params = route.calls.last.request.url.params
    assert params["where"] == measured_rows_filter(since)
    assert params["order_by"] == "date_heure"


@respx.mock
def test_measured_rows_handles_empty_export():
    respx.get(EXPORT_URL).respond(text="")
    with client() as odre:
        assert odre.measured_rows() == []


@respx.mock
def test_retries_transient_failures_with_backoff():
    respx.get(METADATA_URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.ConnectTimeout("timeout"),
            httpx.Response(200, json={"metas": {"default": {"data_processed": "x"}}}),
        ]
    )
    sleeps: list[float] = []
    with client(sleeps) as odre:
        assert odre.updated_at() == "x"
    assert sleeps == [2.0, 4.0]


@respx.mock
def test_honours_retry_after():
    respx.get(METADATA_URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, json={"metas": {"default": {"data_processed": "x"}}}),
        ]
    )
    sleeps: list[float] = []
    with client(sleeps) as odre:
        odre.updated_at()
    assert sleeps == [7.0]


@respx.mock
def test_gives_up_after_max_attempts():
    respx.get(METADATA_URL).respond(502)
    sleeps: list[float] = []
    with client(sleeps) as odre, pytest.raises(OdreError, match="gave up after 5 attempts"):
        odre.updated_at()
    assert len(sleeps) == 4


@respx.mock
def test_does_not_retry_client_errors():
    route = respx.get(EXPORT_URL).respond(400, text="bad where")
    with client() as odre, pytest.raises(OdreError, match="HTTP 400"):
        odre.measured_rows()
    assert route.call_count == 1
