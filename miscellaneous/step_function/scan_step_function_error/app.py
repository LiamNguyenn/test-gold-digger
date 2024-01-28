"""Scan step function error."""
import logging

import boto3.session
from chalice import Chalice
from eh_lambda_utils.tools import AwsSFNClient
from eh_lambda_utils.utils.slack_utils import send_message_to_slack

app = Chalice(app_name="scan_step_function_error")
app.log.setLevel(logging.INFO)


@app.lambda_function()
def scan_step_function_error(event, context):  # noqa: ARG001
    """Scan step function error."""
    session = boto3.session.Session()
    sfn_client = AwsSFNClient(session=session)
    error_detail_list = sfn_client.get_error_executions()
    num_error = 0
    for error_detail in error_detail_list:
        num_error += 1
        send_message_to_slack(**error_detail)
    logging.info(event)
    return {"statusCode": 200, "body": {"message": "scan step function error success"}}
