import json
from collections.abc import Iterator

import httpx
import pytest
from respx import MockRouter

from wattlake_ingestion.nessie import API_URI, NessieClient, NessieError

MAIN_HASH = "a" * 64
OLD_AUDIT_HASH = "b" * 64
AUDITED_HASH = "c" * 64
MERGED_HASH = "d" * 64


def branch(name: str, hash_: str) -> dict:
    return {"reference": {"type": "BRANCH", "name": name, "hash": hash_}}


@pytest.fixture
def client() -> Iterator[NessieClient]:
    with NessieClient() as nessie:
        yield nessie


class TestHead:
    def test_returns_the_hash_of_the_branch(self, client: NessieClient, respx_mock: MockRouter) -> None:
        respx_mock.get(f"{API_URI}/trees/main").respond(json=branch("main", MAIN_HASH))
        assert client.head("main") == MAIN_HASH

    def test_missing_branch_is_none(self, client: NessieClient, respx_mock: MockRouter) -> None:
        respx_mock.get(f"{API_URI}/trees/audit").respond(404, json={"message": "Named reference 'audit' not found"})
        assert client.head("audit") is None

    def test_other_errors_are_raised(self, client: NessieClient, respx_mock: MockRouter) -> None:
        respx_mock.get(f"{API_URI}/trees/main").respond(500, json={"message": "boom"})
        with pytest.raises(NessieError, match="HTTP 500: boom"):
            client.head("main")


class TestRecreateBranch:
    def test_deletes_the_old_branch_then_creates_it_from_the_source_head(
        self, client: NessieClient, respx_mock: MockRouter
    ) -> None:
        respx_mock.get(f"{API_URI}/trees/main").respond(json=branch("main", MAIN_HASH))
        respx_mock.get(f"{API_URI}/trees/audit").respond(json=branch("audit", OLD_AUDIT_HASH))
        delete = respx_mock.delete(f"{API_URI}/trees/audit@{OLD_AUDIT_HASH}").respond(
            json=branch("audit", OLD_AUDIT_HASH)
        )
        create = respx_mock.post(f"{API_URI}/trees").respond(json=branch("audit", MAIN_HASH))

        assert client.recreate_branch("audit", "main") == MAIN_HASH

        assert delete.calls.last.request.url.params["type"] == "BRANCH"
        request = create.calls.last.request
        assert dict(request.url.params) == {"name": "audit", "type": "BRANCH"}
        # Created from the exact hash read on the source, not from whatever its head is by then.
        assert json.loads(request.content) == {"type": "BRANCH", "name": "main", "hash": MAIN_HASH}

    def test_missing_branch_is_only_created(self, client: NessieClient, respx_mock: MockRouter) -> None:
        respx_mock.get(f"{API_URI}/trees/main").respond(json=branch("main", MAIN_HASH))
        respx_mock.get(f"{API_URI}/trees/audit").respond(404)
        create = respx_mock.post(f"{API_URI}/trees").respond(json=branch("audit", MAIN_HASH))

        assert client.recreate_branch("audit", "main") == MAIN_HASH
        assert create.called
        # respx fails the test on any unmocked call, a DELETE included.

    def test_refuses_to_recreate_the_source_itself(self, client: NessieClient) -> None:
        with pytest.raises(ValueError, match="from itself"):
            client.recreate_branch("main", "main")

    def test_missing_source_is_an_error(self, client: NessieClient, respx_mock: MockRouter) -> None:
        respx_mock.get(f"{API_URI}/trees/main").respond(404)
        with pytest.raises(NessieError, match="source branch main does not exist"):
            client.recreate_branch("audit", "main")


class TestMerge:
    def test_merges_the_audited_hash_into_the_target_as_of_the_branch_point(
        self, client: NessieClient, respx_mock: MockRouter
    ) -> None:
        merge = respx_mock.post(f"{API_URI}/trees/main@{MAIN_HASH}/history/merge").respond(
            json={"resultantTargetHash": MERGED_HASH, "targetBranch": "main"}
        )

        assert client.merge("audit", AUDITED_HASH, "main", MAIN_HASH, "publish") == MERGED_HASH
        assert json.loads(merge.calls.last.request.content) == {
            "fromRefName": "audit",
            "fromHash": AUDITED_HASH,
            "message": "publish",
        }

    def test_conflict_is_raised_with_the_nessie_message(self, client: NessieClient, respx_mock: MockRouter) -> None:
        respx_mock.post(f"{API_URI}/trees/main@{MAIN_HASH}/history/merge").respond(
            409, json={"message": "The following keys have been changed in conflict: 'silver.eco2mix_national'"}
        )
        with pytest.raises(NessieError, match=r"HTTP 409: .*silver\.eco2mix_national"):
            client.merge("audit", AUDITED_HASH, "main", MAIN_HASH, "publish")

    def test_error_without_json_body(self, client: NessieClient, respx_mock: MockRouter) -> None:
        respx_mock.post(f"{API_URI}/trees/main@{MAIN_HASH}/history/merge").mock(
            return_value=httpx.Response(502, text="Bad Gateway")
        )
        with pytest.raises(NessieError, match="HTTP 502: Bad Gateway"):
            client.merge("audit", AUDITED_HASH, "main", MAIN_HASH, "publish")
