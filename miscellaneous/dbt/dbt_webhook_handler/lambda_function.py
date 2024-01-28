"""Handle dbt webhook using lambda function."""
import hashlib
import hmac
import json
import logging
import os
import re

import requests

SUCCESS_HTTP_CODE = 200

logging.basicConfig()

# Create a custom logger
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()

# Create formatters and add it to handlers
c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

# Add handlers to the ogger
logger.addHandler(c_handler)
logger.setLevel(logging.DEBUG)


class DbtWebHookError(Exception):
    """Throw this exception when failed to call Braze API."""

    def __init__(self):
        # Call the base class constructor with the parameters it needs
        super().__init__(
            "Calculated signature doesn't match contents of the Authorization header. This webhook may not have been sent from dbt Cloud."
        )


class DbtWebHookHandler:
    """DbtWebHookHandler."""

    def __init__(self, dbt_hook_secret, dbt_api_token, slack_hook_url):
        self.dbt_hook_secret = dbt_hook_secret
        self.dbt_api_token = dbt_api_token
        self.slack_hook_url = slack_hook_url

    def handle(self, input_data: dict):
        """Handle response from dbt webhook."""
        logger.info(f"[handle], input_data={input_data}")
        headers = input_data.get("headers", None)
        auth_header = headers.get("authorization", None)
        raw_body = input_data.get("body", input_data)

        # Validate the webhook came from dbt Cloud
        if auth_header:
            signature = hmac.new(
                self.dbt_hook_secret.encode("utf-8"),
                raw_body.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()

            if signature != auth_header:
                raise DbtWebHookError

        full_body = json.loads(raw_body) if not isinstance(raw_body, dict) else raw_body
        hook_data = full_body["data"]

        # Skip if the job is from pull requests
        if "PR Check" in hook_data["jobName"]:
            logger.info("[handle], skip PR Check.")
            return

        # When testing, you will want to hardcode run_id and account_id to IDs that exist; the sample webhook won't work.
        run_id = hook_data["runId"]
        account_id = full_body["accountId"]

        err_msg = self.extract_run_info(run_id, account_id, hook_data)
        if err_msg["send_error_thread"]:
            self._send_slack(err_msg)
        else:
            logger.info("[handle], no error msg, skip now.")

    def extract_run_info(self, run_id, account_id, hook_data):
        """Fetch run info from the dbt Cloud Admin API."""
        # Steps derived from these commands won't have their error details shown inline, as they're messy
        commands_to_skip_logs = ["dbt source", "dbt docs"]

        url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/?include_related=["run_steps"]'
        headers = {"Authorization": f"Bearer {self.dbt_api_token}"}
        run_data_response = requests.get(url, headers=headers, timeout=300)
        run_data_response.raise_for_status()
        run_data_results = run_data_response.json()["data"]

        # Overall run summary
        step_summary_post = f"""
        *🚨🚨🚨 [{hook_data['runStatus']} for Run #{run_id} on Job \"{hook_data['jobName']}\"]({run_data_results['href']})*

        *Environment:* {hook_data['environmentName']} | *Trigger:* {hook_data['runReason']} | *Duration:* {run_data_results['duration_humanized']}

        """

        threaded_errors_post = ""

        # Step-specific summaries
        for step in run_data_results["run_steps"]:
            if step["status_humanized"] == "Success":
                step_summary_post += f"""
        ✅ {step['name']} ({step['status_humanized']} in {step['duration_humanized']})
        """
            else:
                step_summary_post += f"""
        ❌ {step['name']} ({step['status_humanized']} in {step['duration_humanized']})
        """

                # Don't try to extract info from steps that don't have well-formed logs
                show_logs = not any(
                    cmd in step["name"] for cmd in commands_to_skip_logs
                )
                if show_logs:
                    full_log = step["logs"]
                    # Remove timestamp and any colour tags
                    full_log = re.sub("\x1b?\\[[0-9]+m[0-9:]*", "", full_log)

                    summary_start = re.search(
                        "(?:Completed with \\d+ errors? and \\d+ warnings?:|Database Error|Compilation Error|Runtime Error)",
                        full_log,
                    )

                    line_items = re.findall(
                        "(^.*(?:Failure|Error) in .*\n.*\n.*)", full_log, re.MULTILINE
                    )

                    if not summary_start:
                        continue

                    threaded_errors_post += f"""
        *{step['name']}*
        """
                    # If there are no line items, the failure wasn't related to dbt nodes, and we want the whole rest of the message.
                    # If there are, then we just want the summary line and then to log out each individual node's error.
                    if len(line_items) == 0:
                        relevant_log = f"```{full_log[summary_start.start():]}```"
                    else:
                        relevant_log = summary_start[0]
                        for item in line_items:
                            relevant_log += f"\n```\n{item.strip()}\n```\n"
                    threaded_errors_post += f"""
        {relevant_log}
        """

        send_error_thread = len(threaded_errors_post) > 0

        return {
            "step_summary_post": step_summary_post,
            "send_error_thread": send_error_thread,
            "threaded_errors_post": threaded_errors_post,
        }

    def _send_slack(self, msg):
        """Send msg to slack."""
        payload = {
            "text": ":rotating_light: Dbt Job Errors :rotating_light:",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": msg["step_summary_post"]},
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": msg["threaded_errors_post"]},
                },
            ],
        }

        response = requests.post(
            self.slack_hook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=300,
        )
        response.raise_for_status()


def lambda_handler(event, _context):
    """Endpoint function."""
    handler = DbtWebHookHandler(
        os.environ["DBT_HOOK_SECRET"],
        os.environ["DBT_API_TOKEN"],
        os.environ["SLACK_HOOK_URL"],
    )

    try:
        handler.handle(event)
    except DbtWebHookError as e:
        return {"statusCode": 400, "body": json.dumps(f"Invalid payload, err: {e}")}


if __name__ == "__main__":
    handler = DbtWebHookHandler(
        os.environ["DBT_HOOK_SECRET"],
        os.environ["DBT_API_TOKEN"],
        os.environ["SLACK_HOOK_URL"],
    )
    handler.handle({})
