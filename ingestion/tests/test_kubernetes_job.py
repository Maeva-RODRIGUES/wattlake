from unittest.mock import MagicMock

import pytest
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from wattlake_ingestion.kubernetes_job import JobFailedError, job_from_cronjob, run_job_from_cronjob

NAMESPACE = "wattlake"


def cronjob() -> client.V1CronJob:
    return client.V1CronJob(
        metadata=client.V1ObjectMeta(name="nessie-gc", namespace=NAMESPACE, uid="uid-1"),
        spec=client.V1CronJobSpec(
            schedule="0 12 * * *",
            job_template=client.V1JobTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app.kubernetes.io/name": "nessie-gc"}),
                spec=client.V1JobSpec(backoff_limit=3, template=client.V1PodTemplateSpec()),
            ),
        ),
    )


def job_with(condition: str | None, failed: int | None = None) -> client.V1Job:
    conditions = [client.V1JobCondition(type=condition, status="True")] if condition else None
    return client.V1Job(status=client.V1JobStatus(conditions=conditions, failed=failed))


@pytest.fixture
def batch_api() -> MagicMock:
    api = MagicMock()
    api.read_namespaced_cron_job.return_value = cronjob()
    return api


class TestJobFromCronjob:
    def test_copies_the_job_template_under_the_given_name(self) -> None:
        job = job_from_cronjob(cronjob(), "nessie-gc-20261118-1130")
        assert job.metadata.name == "nessie-gc-20261118-1130"
        assert job.metadata.namespace == NAMESPACE
        assert job.metadata.labels == {"app.kubernetes.io/name": "nessie-gc"}
        assert job.spec.backoff_limit == 3

    def test_is_owned_by_the_cronjob_and_marked_manual(self) -> None:
        job = job_from_cronjob(cronjob(), "nessie-gc-20261118-1130")
        (owner,) = job.metadata.owner_references
        assert (owner.kind, owner.name, owner.uid, owner.controller) == ("CronJob", "nessie-gc", "uid-1", True)
        assert job.metadata.annotations["cronjob.kubernetes.io/instantiate"] == "manual"


class TestRunJobFromCronjob:
    def test_creates_the_job_and_waits_for_its_success(self, batch_api: MagicMock) -> None:
        batch_api.read_namespaced_job_status.side_effect = [job_with(None), job_with("Complete")]
        run_job_from_cronjob(NAMESPACE, "nessie-gc", "nessie-gc-1", poll_seconds=0, batch_api=batch_api)
        created = batch_api.create_namespaced_job.call_args.args[1]
        assert created.metadata.name == "nessie-gc-1"
        assert batch_api.read_namespaced_job_status.call_count == 2

    def test_waits_for_an_existing_job_instead_of_starting_another(self, batch_api: MagicMock) -> None:
        batch_api.create_namespaced_job.side_effect = ApiException(status=409)
        batch_api.read_namespaced_job_status.return_value = job_with("Complete")
        run_job_from_cronjob(NAMESPACE, "nessie-gc", "nessie-gc-1", poll_seconds=0, batch_api=batch_api)
        batch_api.read_namespaced_job_status.assert_called_once_with("nessie-gc-1", NAMESPACE)

    def test_a_failed_job_raises(self, batch_api: MagicMock) -> None:
        batch_api.read_namespaced_job_status.return_value = job_with("Failed", failed=4)
        with pytest.raises(JobFailedError, match="nessie-gc-1 failed after 4 pod failure"):
            run_job_from_cronjob(NAMESPACE, "nessie-gc", "nessie-gc-1", poll_seconds=0, batch_api=batch_api)

    def test_other_api_errors_are_raised(self, batch_api: MagicMock) -> None:
        batch_api.create_namespaced_job.side_effect = ApiException(status=403)
        with pytest.raises(ApiException):
            run_job_from_cronjob(NAMESPACE, "nessie-gc", "nessie-gc-1", poll_seconds=0, batch_api=batch_api)
