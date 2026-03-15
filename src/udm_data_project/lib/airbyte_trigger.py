import time
from typing import Optional

import requests
from dagster import AssetExecutionContext


def _get_airbyte_token(airbyte_url: str, client_id: str, client_secret: str) -> str:
    """Obtain a Bearer token from Airbyte using client credentials."""
    response = requests.post(
        f"{airbyte_url.rstrip('/')}/api/v1/applications/token",
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


def trigger_airbyte_sync(
    connection_id: str,
    airbyte_url: str,
    client_id: str,
    client_secret: str,
    wait_for_completion: bool = True,
    poll_interval_seconds: int = 10,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Trigger an Airbyte connection sync and optionally wait for it to complete.

    Args:
        connection_id: The Airbyte connection ID to sync.
        airbyte_url: Base URL of the Airbyte instance (e.g. 'http://localhost:8000').
        client_id: Airbyte OAuth2 client ID.
        client_secret: Airbyte OAuth2 client secret.
        wait_for_completion: If True, polls until the sync job finishes.
        poll_interval_seconds: Seconds between status polls.
        context: Optional Dagster context for logging.
    """
    token = _get_airbyte_token(airbyte_url, client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}"}

    if context:
        context.log.info(f"Triggering Airbyte sync for connection: {connection_id}")

    response = requests.post(
        f"{airbyte_url.rstrip('/')}/api/public/v1/jobs",
        json={"connectionId": connection_id, "jobType": "sync"},
        headers=headers,
    )

    if response.status_code == 409:
        # A sync is already running — find its job ID and wait for it
        if context:
            context.log.info("Sync already running, waiting for existing job ...")
        running = requests.get(
            f"{airbyte_url.rstrip('/')}/api/public/v1/jobs",
            params={"connectionId": connection_id, "status": "running", "limit": 1},
            headers=headers,
        )
        running.raise_for_status()
        job_id = running.json()["data"][0]["jobId"]
    else:
        response.raise_for_status()
        job_id = response.json().get("jobId")

    if context:
        context.log.info(f"Airbyte sync job started: job_id={job_id}")

    if not wait_for_completion or not job_id:
        return

    while True:
        time.sleep(poll_interval_seconds)
        status_response = requests.get(
            f"{airbyte_url.rstrip('/')}/api/public/v1/jobs/{job_id}",
            headers=headers,
        )
        status_response.raise_for_status()

        status = status_response.json().get("status")

        if context:
            context.log.info(f"  job {job_id} status: {status}")

        if status == "succeeded":
            if context:
                context.log.info(f"Airbyte sync completed successfully: job_id={job_id}")
            return
        elif status in ("failed", "cancelled"):
            raise RuntimeError(f"Airbyte sync job {job_id} ended with status: {status}")
