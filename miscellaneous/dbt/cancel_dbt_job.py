"""Cancel dbt job."""
import json
import logging
import os
import urllib.parse
import urllib.request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# get environment variables
# ------------------------------------------------------------------------------
api_base = os.getenv(
    "DBT_URL", "https://cloud.getdbt.com/"
)  # default to multitenant url
job_cause = os.getenv(
    "DBT_JOB_CAUSE", "API-triggered job"
)  # default to generic message
git_branch = os.getenv("DBT_JOB_BRANCH", None)  # default to None
schema_override = os.getenv("DBT_JOB_SCHEMA_OVERRIDE", None)  # default to None
api_key = os.environ[
    "DBT_API_KEY"
]  # no default here, just throw an error here if key not provided
account_id = os.environ[
    "DBT_ACCOUNT_ID"
]  # no default here, just throw an error here if id not provided
job_id = os.environ[
    "DBT_PR_JOB_ID"
]  # no default here, just throw an error here if id not provided
run_id = os.environ["DBT_RUN_ID"]

gh_sha = os.getenv("PR_GITHUB_SHA", None)  # default to None

job_definition_id = 377226  # hardcode for now, ci job.

logger.info(
    f"""
Configuration:
api_base: {api_base}
job_cause: {job_cause}
git_branch: {git_branch}
schema_override: {schema_override}
account_id: {account_id}
job_id: {job_id}
run_id: {run_id}
gh_sha: {gh_sha}
"""
)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# use environment variables to set configuration
# ------------------------------------------------------------------------------
req_auth_header = {"Authorization": f"Token {api_key}"}
req_job_url = f"{api_base}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
run_status_map = {  # dbt run statuses are encoded as integers. This map provides a human-readable status
    1: "Queued",
    2: "Starting",
    3: "Running",
    10: "Success",
    20: "Error",
    30: "Cancelled",
}


def get_runs_for_git_sha(git_sha):
    """Get all runs for a given git sha."""
    # build status check url and run status link
    req_status_url = f"{api_base}/api/v2/accounts/{account_id}/runs/?status=3"

    rep = urllib.request.Request(  # noqa: S310
        req_status_url, headers=req_auth_header, method="GET"
    )
    resp = urllib.request.urlopen(rep, timeout=300)  # noqa: S310

    req_status_resp = json.loads(resp.read())

    for run in req_status_resp["data"]:
        if run["git_sha"] == git_sha:
            return run["id"]
    return None


def cancel_run(url, headers, n_retry=3):
    """Cancel running job."""
    counter = 0
    while counter < n_retry:
        rep = urllib.request.Request(url, headers=headers, method="POST")  # noqa: S310
        resp = urllib.request.urlopen(rep, timeout=300)  # noqa: S310
        resp_payload = json.loads(resp.read())
        if resp_payload["status"]["is_success"]:
            logger.info(f"cancelled run job: {resp_payload}")
            break
        counter += 1


def get_run_status(url, headers) -> str:
    """Get the status of a running dbt job."""
    rep = urllib.request.Request(url, headers=headers, method="GET")  # noqa: S310
    resp = urllib.request.urlopen(rep, timeout=300)  # noqa: S310

    req_status_resp = json.loads(resp.read())

    run_status_code = req_status_resp["data"]["status"]
    run_status = run_status_map[run_status_code]
    return run_status


def main():
    """Endpoint."""
    logger.info("Beginning request for job run...")

    if not run_id and not gh_sha:
        logger.info("No run_id or gh_sha provided, skipping cancelling job.")
        return

    found_run_id = None
    if not run_id:
        logger.info("No run_id provided, searching for run_id by git_sha.")
        found_run_id = get_runs_for_git_sha(gh_sha)
    else:
        found_run_id = run_id

    if found_run_id:
        logger.info(f"Found run_id: {found_run_id}, with sha: {gh_sha}")
        req_status_url = f"{api_base}/api/v2/accounts/{account_id}/runs/{found_run_id}/"
        cancel_job_run_url = (
            f"{api_base}/api/v2/accounts/{account_id}/runs/{found_run_id}/cancel/"
        )
    else:
        logger.info(f"No run_id found for sha: {gh_sha}, skipping cancelling job.")
        return

    status = get_run_status(req_status_url, req_auth_header)
    logger.info(f"Run status -> {status}")

    if status in ["Error", "Cancelled", "Success"]:
        logger.info("Skip cancelling job, because it's not running.")
        return

    cancel_run(cancel_job_run_url, req_auth_header)


if __name__ == "__main__":
    main()
