"""Nessie branches for Write-Audit-Publish (see docs/adr/008-write-audit-publish.md).

dbt writes silver and gold on an audit branch, its tests run there, and the
branch is merged into main only when they all pass. Trino cannot create or
merge Nessie branches in SQL: this module calls the Nessie REST API v2.

A reference is written "name@hash" when an operation must check the hash: the
operation is refused if the branch moved, instead of acting on contents the
caller has not seen.
"""

from types import TracebackType
from typing import Self

import httpx

API_URI = "http://nessie:19120/api/v2"


class NessieError(RuntimeError):
    """Nessie refused an operation (conflict, missing reference, invalid input)."""


class NessieClient:
    """Minimal client of the Nessie REST API v2: branch heads, recreation and merge."""

    def __init__(self, base_url: str = API_URI, timeout: float = 30.0):
        self._http = httpx.Client(base_url=base_url, timeout=timeout)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def head(self, branch: str) -> str | None:
        """Return the hash of the branch's last commit, or None if the branch does not exist."""
        response = self._http.get(f"/trees/{branch}")
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        return self._json(response, f"read branch {branch}")["reference"]["hash"]

    def recreate_branch(self, branch: str, source: str) -> str:
        """Point branch at the current head of source, deleting the old branch first.

        Returns the hash both branches start from. The old branch is deleted
        whatever it contains: it only holds the previous audit, merged or not.
        """
        if branch == source:
            raise ValueError(f"cannot recreate branch {branch} from itself")
        source_hash = self.head(source)
        if source_hash is None:
            raise NessieError(f"source branch {source} does not exist")

        old_hash = self.head(branch)
        if old_hash is not None:
            self._json(
                self._http.delete(f"/trees/{branch}@{old_hash}", params={"type": "BRANCH"}),
                f"delete branch {branch}",
            )
        created = self._json(
            self._http.post(
                "/trees",
                params={"name": branch, "type": "BRANCH"},
                json={"type": "BRANCH", "name": source, "hash": source_hash},
            ),
            f"create branch {branch} from {source}",
        )
        return created["reference"]["hash"]

    def merge(self, source: str, source_hash: str, target: str, target_hash: str, message: str) -> str:
        """Merge source, as of source_hash, into target. Returns the new hash of target.

        target_hash is the target commit the source branched from: Nessie
        detects conflicts with every change made to target since then. A
        commit on other tables of target in the meantime (a bronze load) does
        not conflict; a change to a merged table does, and nothing is merged.
        """
        merged = self._json(
            self._http.post(
                f"/trees/{target}@{target_hash}/history/merge",
                json={"fromRefName": source, "fromHash": source_hash, "message": message},
            ),
            f"merge {source} into {target}",
        )
        return merged["resultantTargetHash"]

    @staticmethod
    def _json(response: httpx.Response, action: str) -> dict:
        if response.is_error:
            # Nessie errors carry a JSON body with a readable "message".
            try:
                detail = response.json().get("message", response.text)
            except ValueError:
                detail = response.text
            raise NessieError(f"cannot {action}: HTTP {response.status_code}: {detail}")
        return response.json()
