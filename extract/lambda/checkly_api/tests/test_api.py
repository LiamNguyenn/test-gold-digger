# type: ignore
# ruff: noqa
"""Test API CHECKLY"""
import os
import sys
import time
from datetime import datetime, timedelta

import pytest
import pytz
import requests

sys.path.append("..")  # add up 1 more level in test

from ..chalicelib.helper.checkly_models import (
    PayLoadListAllChecks,
    PayLoadListAllCheckResult,
)
from ..chalicelib.helper.checkly_api_call import (
    all_checks_api_call,
    all_check_results_api_call,
)


class TestChecklyAPI:
    @pytest.fixture(scope="session", autouse=True)
    def set_env(self):
        os.environ["CHECKLY_API_BASE_URL"] = ""
        os.environ["CHECKLY_API_KEY"] = ""
        os.environ["CHECKLY_ACCOUNT_ID"] = ""
        os.environ["CHECKLY_TIMEOUT"] = ""

    def test_call_checkly_all_checks_api(self):
        CHECKLY_API_BASE_URL = os.environ["CHECKLY_API_BASE_URL"]
        CHECKLY_API_KEY = os.environ["CHECKLY_API_KEY"]
        CHECKLY_ACCOUNT_ID = os.environ["CHECKLY_ACCOUNT_ID"]
        CHECKLY_TIMEOUT: int = int(os.environ["CHECKLY_TIMEOUT"])

        payload = PayLoadListAllChecks(limit=10, page=1)
        df, row_count = all_checks_api_call(
            checkly_api_base_url=CHECKLY_API_BASE_URL,
            checkly_account_id=CHECKLY_ACCOUNT_ID,
            checkly_api_key=CHECKLY_API_KEY,
            checkly_timeout=CHECKLY_TIMEOUT,
            payload=payload,
        )
        assert row_count != 0

    def test_call_all_check_results_api(self):
        CHECKLY_API_BASE_URL = os.environ["CHECKLY_API_BASE_URL"]
        CHECKLY_API_KEY = os.environ["CHECKLY_API_KEY"]
        CHECKLY_ACCOUNT_ID = os.environ["CHECKLY_ACCOUNT_ID"]
        CHECKLY_TIMEOUT: int = int(os.environ["CHECKLY_TIMEOUT"])

        check_id = "fdc1e180-2776-4db2-8758-646e635ec2c3"

        payload = PayLoadListAllCheckResult(
            limit=100,
            page=1,
            from_date=time.mktime(
                (datetime.now(tz=pytz.utc).now() - timedelta(hours=6)).timetuple()
            ),
            to_date=time.mktime(datetime.now(tz=pytz.utc).now().timetuple()),
            location="ap-southeast-1",
            checkType="BROWSER",
            hasFailures=False,
            resultType=None,
        )

        df, row_count = all_check_results_api_call(
            checkly_api_base_url=CHECKLY_API_BASE_URL,
            checkly_account_id=CHECKLY_ACCOUNT_ID,
            checkly_api_key=CHECKLY_API_KEY,
            checkly_timeout=CHECKLY_TIMEOUT,
            check_id=check_id,
            payload=payload,
        )
        assert row_count != 0
