# type: ignore
# ruff: noqa
import boto3
import pytest

from ..chalicelib.helper_models.keypay_config_models import KeypayConfig


class TestKeypayScanFileToProcess:
    ASSUMED_ACCOUNT_NUMBER = "979797940137"
    ASSUMED_ROLE_NAME = "lambda_trigger_role"

    @pytest.fixture()
    def event(self):
        return {
            "job_name": "keypay_user_reseller",
            "s3_source_bucket": "eh-keypay",
            "is_production_s3_source_bucket": True,
            "s3_prefix": "",
            "s3_file_pattern": "(?i)UserReseller_BiMonthly_\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d.zip",
            "redshift_schema": "stg_keypay",
            "redshift_temp_schema": "temp_keypay",
            "redshift_table": "user_reseller",
        }

    def test_scan_file_to_process(self, event):
        pipeline_config = KeypayConfig(**event)
        print(pipeline_config)

    def test_get_aws_assumed_session(self):
        session = boto3.Session(profile_name="418054751921_DataEngineer")
        sts_connection = session.client("sts")
        account_number = TestKeypayScanFileToProcess.ASSUMED_ACCOUNT_NUMBER
        role_name = TestKeypayScanFileToProcess.ASSUMED_ROLE_NAME
        assumed_role = sts_connection.assume_role(
            RoleArn=f"arn:aws:iam::{account_number}:role/{role_name}",
            RoleSessionName="cross_account_lambda",
        )
        assumed_role_credentials = assumed_role["Credentials"]
        assumed_session = boto3.Session(
            aws_access_key_id=assumed_role_credentials["AccessKeyId"],
            aws_secret_access_key=assumed_role_credentials["SecretAccessKey"],
            aws_session_token=assumed_role_credentials["SessionToken"],
        )
