import time
from typing import Optional

import requests
from dagster import AssetExecutionContext


def trigger_airbyte_sync(
    connection_id: str,
    airbyte_url: str,
    wait_for_completion: bool = True,
    poll_interval_seconds: int = 10,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Trigger an Airbyte connection sync and optionally wait for it to complete.

    Args:
        connection_id: The Airbyte connection ID to sync.
        airbyte_url: Base URL of the Airbyte instance (e.g. 'http://localhost:8000').
        wait_for_completion: If True, polls until the sync job finishes.
        poll_interval_seconds: Seconds between status polls.
        context: Optional Dagster context for logging.
    """
    trigger_url = f"{airbyte_url.rstrip('/')}/api/v1/connections/sync"

    if context:
        context.log.info(f"Triggering Airbyte sync for connection: {connection_id}")

    response = requests.post(trigger_url, json={"connectionId": connection_id})
    response.raise_for_status()

    job = response.json().get("job", {})
    job_id = job.get("id")

    if context:
        context.log.info(f"Airbyte sync job started: job_id={job_id}")

    if not wait_for_completion or not job_id:
        return

    status_url = f"{airbyte_url.rstrip('/')}/api/v1/jobs/get"
    while True:
        time.sleep(poll_interval_seconds)
        status_response = requests.post(status_url, json={"id": job_id})
        status_response.raise_for_status()

        status = status_response.json().get("job", {}).get("status")

        if context:
            context.log.info(f"  job {job_id} status: {status}")

        if status == "succeeded":
            if context:
                context.log.info(f"Airbyte sync completed successfully: job_id={job_id}")
            return
        elif status in ("failed", "cancelled"):
            raise RuntimeError(f"Airbyte sync job {job_id} ended with status: {status}")
