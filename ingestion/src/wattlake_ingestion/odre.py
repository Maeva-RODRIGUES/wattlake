"""Client for the ODRE Explore API v2.1, which publishes the RTE eCO2mix datasets.

The history dataset has more rows than the /records endpoint can page through
(offset + limit <= 10,000), so periods are downloaded with /exports instead.
See docs/sources.md.
"""

import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Self

import httpx

BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1"
HISTORY_DATASET = "eco2mix-national-cons-def"
REALTIME_DATASET = "eco2mix-national-tr"

# Transient server-side failures. Other 4xx mean the request itself is wrong.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class OdreError(Exception):
    """The API could not deliver the requested data."""


def period_filter(start: datetime, end: datetime) -> str:
    """Return the ODSQL filter selecting rows with start <= date_heure < end.

    Bounds must be timezone-aware: date_heure is stored in UTC, and a naive
    datetime would silently shift the period by one or two hours.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware datetimes")
    if start >= end:
        raise ValueError(f"start ({start}) must be before end ({end})")
    start_utc = start.astimezone(UTC).isoformat()
    end_utc = end.astimezone(UTC).isoformat()
    return f"date_heure >= date'{start_utc}' and date_heure < date'{end_utc}'"


class OdreClient:
    """Downloads eCO2mix data, retrying transient failures."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        *,
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
            headers={"User-Agent": "wattlake-ingestion"},
        )
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

    def export_to_file(
        self,
        dataset_id: str,
        start: datetime,
        end: datetime,
        dest: Path,
        *,
        file_format: str = "parquet",
    ) -> Path:
        """Download the rows of [start, end) to dest and return its path.

        The data is streamed to a temporary file renamed at the end, so dest
        never holds a partial download.
        """
        params = {"where": period_filter(start, end)}
        url = f"/catalog/datasets/{dataset_id}/exports/{file_format}"
        dest = Path(dest)
        partial = dest.with_name(dest.name + ".part")

        try:
            for attempt in range(1, self._max_attempts + 1):
                try:
                    with self._client.stream("GET", url, params=params) as response:
                        if response.status_code == httpx.codes.OK:
                            with partial.open("wb") as file:
                                for chunk in response.iter_bytes():
                                    file.write(chunk)
                            partial.replace(dest)
                            return dest
                        if response.status_code not in RETRYABLE_STATUS_CODES:
                            response.read()
                            raise OdreError(
                                f"{dataset_id}: HTTP {response.status_code}: {response.text[:200]}"
                            )
                        failure = f"HTTP {response.status_code}"
                        delay = self._retry_delay(attempt, response.headers.get("Retry-After"))
                except httpx.TransportError as exc:
                    # Connection errors, timeouts, and streams cut mid-download.
                    failure = f"{type(exc).__name__}: {exc}"
                    delay = self._retry_delay(attempt, None)

                if attempt < self._max_attempts:
                    self._sleep(delay)
        finally:
            partial.unlink(missing_ok=True)

        raise OdreError(f"{dataset_id}: gave up after {self._max_attempts} attempts, last failure: {failure}")

    def _retry_delay(self, attempt: int, retry_after: str | None) -> float:
        """Wait the server's Retry-After if given in seconds, else back off exponentially."""
        if retry_after is not None and retry_after.isdigit():
            return min(float(retry_after), self._max_backoff_seconds)
        return min(self._backoff_seconds * 2 ** (attempt - 1), self._max_backoff_seconds)
