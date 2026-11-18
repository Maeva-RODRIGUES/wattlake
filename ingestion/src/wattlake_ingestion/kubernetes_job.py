"""Run a Kubernetes Job from the template of a CronJob, and wait for its end.

Same result as "kubectl create job --from=cronjob/<name>": the Job gets the
CronJob's job template, an owner reference to the CronJob (so that the CronJob's
history limits apply to it) and the annotation of a manual run. The CronJob
stays the single definition of the Job, deployed by Argo CD.

The Job name is chosen by the caller and identifies one run: if a Job with that
name already exists (a previous attempt of the same Airflow task), it is not
created again but awaited, so that two Jobs of the same run never overlap.
"""

import logging
import time

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

log = logging.getLogger(__name__)

HTTP_CONFLICT = 409


class JobFailedError(RuntimeError):
    """The Job ended without succeeding (retries exhausted or deadline reached)."""


def job_from_cronjob(cronjob: client.V1CronJob, name: str) -> client.V1Job:
    """Build the Job that the CronJob would start, under the given name."""
    template = cronjob.spec.job_template
    template_metadata = template.metadata or client.V1ObjectMeta()
    return client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=cronjob.metadata.namespace,
            labels=dict(template_metadata.labels or {}),
            annotations={
                **(template_metadata.annotations or {}),
                "cronjob.kubernetes.io/instantiate": "manual",
            },
            owner_references=[
                client.V1OwnerReference(
                    api_version="batch/v1",
                    kind="CronJob",
                    name=cronjob.metadata.name,
                    uid=cronjob.metadata.uid,
                    controller=True,
                )
            ],
        ),
        spec=template.spec,
    )


def job_outcome(job: client.V1Job) -> str | None:
    """'Complete' or 'Failed' once the Job has ended, None while it runs."""
    for condition in (job.status and job.status.conditions) or []:
        if condition.type in ("Complete", "Failed") and condition.status == "True":
            return condition.type
    return None


def run_job_from_cronjob(
    namespace: str,
    cronjob_name: str,
    job_name: str,
    poll_seconds: float = 15,
    batch_api: client.BatchV1Api | None = None,
) -> None:
    """Start the Job (or join it if it exists) and return once it has succeeded.

    Raises JobFailedError if it fails. Outside of tests, batch_api is built from
    the service account of the pod (in-cluster configuration).
    """
    if batch_api is None:
        config.load_incluster_config()
        batch_api = client.BatchV1Api()

    cronjob = batch_api.read_namespaced_cron_job(cronjob_name, namespace)
    try:
        batch_api.create_namespaced_job(namespace, job_from_cronjob(cronjob, job_name))
        log.info("Created Job %s from CronJob %s", job_name, cronjob_name)
    except ApiException as error:
        if error.status != HTTP_CONFLICT:
            raise
        log.info("Job %s already exists, waiting for it", job_name)

    while True:
        job = batch_api.read_namespaced_job_status(job_name, namespace)
        outcome = job_outcome(job)
        if outcome == "Complete":
            log.info("Job %s succeeded", job_name)
            return
        if outcome == "Failed":
            raise JobFailedError(
                f"Job {job_name} failed after {job.status.failed or 0} pod failure(s);"
                f" see: kubectl -n {namespace} logs job/{job_name}"
            )
        time.sleep(poll_seconds)
