"""Client for the real-time eCO2mix dataset of the ODRE Explore API v2.1.

The dataset holds one row per quarter-hour, from September 1st to the next day:
rows after the last measurement only carry forecasts. Only measured rows
(consommation not null) are returned. See docs/sources.md.
"""

import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self

import httpx

BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1"
DATASET = "eco2mix-national-tr"

# Transient server-side failures. Other 4xx mean the request itself is wrong.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

Row = dict[str, Any]


class OdreError(Exception):
    """The API could not deliver the requested data."""


def measured_rows_filter(since: datetime | None) -> str:
    """Return the ODSQL filter selecting measured rows, from since (inclusive) if given.

    since must be timezone-aware: date_heure is stored in UTC, and a naive
    datetime would silently shift the window by one or two hours.
    """
    measured = "consommation is not null"
    if since is None:
        return measured
    if since.tzinfo is None:
        raise ValueError("since must be a timezone-aware datetime")
    return f"date_heure >= date'{since.astimezone(UTC).isoformat()}' and {measured}"


class OdreClient:
    """Reads the real-time dataset, retrying transient failures."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        *,
        dataset: str = DATASET,
        timeout: float = 60.0,
        max_attempts: int = 5,
        backoff_seconds: float = 2.0,
        max_backoff_seconds: float = 60.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "wattlake-streaming"},
        )
        self._dataset = dataset
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds
        # Injected so that tests check the waits without actually waiting.
        self._sleep = sleep

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def updated_at(self) -> str:
        """Return when the dataset was last refreshed (data_processed, ISO 8601).

        A cheap call: the rows only need to be read again when this changes.
        """
        response = self._get(f"/catalog/datasets/{self._dataset}", {})
        try:
            return response.json()["metas"]["default"]["data_processed"]
        except (ValueError, KeyError, TypeError) as exc:
            raise OdreError(f"{self._dataset}: no data_processed in the dataset metadata") from exc

    def measured_rows(self, since: datetime | None = None) -> list[Row]:
        """Return the measured rows from since (all of them if None), oldest first.

        Uses /exports, which has no row limit, unlike /records (10,000 rows):
        the first run of the producer reads the whole dataset.
        """
        params = {"where": measured_rows_filter(since), "order_by": "date_heure"}
        response = self._get(f"/catalog/datasets/{self._dataset}/exports/jsonl", params)
        try:
            return [json.loads(line) for line in response.text.splitlines() if line.strip()]
        except ValueError as exc:
            raise OdreError(f"{self._dataset}: invalid JSON lines in the export") from exc

    def _get(self, url: str, params: dict[str, str]) -> httpx.Response:
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._client.get(url, params=params)
                if response.status_code == httpx.codes.OK:
                    return response
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    raise OdreError(f"{self._dataset}: HTTP {response.status_code}: {response.text[:200]}")
                failure = f"HTTP {response.status_code}"
                delay = self._retry_delay(attempt, response.headers.get("Retry-After"))
            except httpx.TransportError as exc:
                # Connection errors and timeouts.
                failure = f"{type(exc).__name__}: {exc}"
                delay = self._retry_delay(attempt, None)

            if attempt < self._max_attempts:
                self._sleep(delay)

        raise OdreError(f"{self._dataset}: gave up after {self._max_attempts} attempts, last failure: {failure}")

    def _retry_delay(self, attempt: int, retry_after: str | None) -> float:
        """Wait the server's Retry-After if given in seconds, else back off exponentially."""
        if retry_after is not None and retry_after.isdigit():
            return min(float(retry_after), self._max_backoff_seconds)
        return min(self._backoff_seconds * 2 ** (attempt - 1), self._max_backoff_seconds)
