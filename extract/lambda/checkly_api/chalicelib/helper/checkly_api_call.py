# ruff: noqa: F841, ERA001, TRY002, TRY003
"""module use to call checkly api."""

from datetime import datetime

import polars as pl
import pytz  # type: ignore[import]
import requests
from eh_lambda_utils.utils import common_rename_to_standard_columns_name
from requests.exceptions import HTTPError

from .checkly_models import PayLoadListAllCheckResult, PayLoadListAllChecks


def all_checks_api_call(
    checkly_api_base_url: str,
    checkly_account_id: str,
    checkly_api_key: str,
    checkly_timeout: int,
    payload: PayLoadListAllChecks,
) -> tuple[pl.DataFrame | None, int]:
    """
    Call API to get all checks available.

    @param checkly_api_base_url: Checkly API base url
    @param checkly_account_id: Checkly account id
    @param checkly_api_key: Checkly API key
    @param checkly_timeout: Checkly timeout
    @param payload: PayLoadListAllChecks module

    @return: result Dataframe and row_count.
    """
    try:
        response = requests.get(
            f"{checkly_api_base_url}/v1/checks",
            params=payload.model_dump(),
            headers={
                "x-checkly-account": checkly_account_id,
                "Accept": "application/json",
                "Authorization": checkly_api_key,
            },
            timeout=checkly_timeout,
        )
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        response.encoding = "utf-8"
        json_response: list[dict] = response.json()
        row_count = len(json_response)
        if not row_count:
            return None, 0
    except HTTPError as http_err:
        raise Exception(f"HTTP error occurred, {response.json()}") from http_err
    else:
        # CONVERT RESPONSE TO POLARS DATAFRAME
        all_checks_df: pl.DataFrame = pl.from_dicts(
            json_response, infer_schema_length=1000
        )
        all_checks_df = all_checks_df.explode("locations")  # spread by locations
        # REMOVE COLUMNS WITH LIST OR STRUCT TYPE
        all_checks_df = all_checks_df.select(
            pl.exclude(
                [
                    col_name
                    for col_name, dtype in all_checks_df.schema.items()
                    if dtype in (pl.List, pl.Struct)
                ]
            )
        )
        all_checks_df = all_checks_df.with_columns(
            _transaction_date=datetime.now(tz=pytz.utc).now(),
            _etl_date=datetime.now(tz=pytz.utc).now(),
        )
        # RENAME COLUMNS BY STANDARDS
        all_checks_df.columns = common_rename_to_standard_columns_name(
            all_checks_df.columns
        )
        return all_checks_df, row_count


def all_check_results_api_call(
    checkly_api_base_url: str,
    checkly_account_id: str,
    checkly_api_key: str,
    checkly_timeout: int,
    check_id: str,
    payload: PayLoadListAllCheckResult,
) -> tuple[pl.DataFrame | None, int]:
    """Use to call checkly api to get check results base on check_id."""
    # construct right name & remove unnecessary
    try:
        final_payload = {}
        for k, v in payload.model_dump().items():
            if v is not None:
                new_key = k
                if k in ("from_date", "to_date"):
                    new_key = k.replace("_date", "")
                final_payload[new_key] = v
        # CALL API TO GET DATA
        response = requests.get(
            f"{checkly_api_base_url}/v1/check-results/{check_id}",
            params=final_payload,
            headers={
                "x-checkly-account": checkly_account_id,
                "Accept": "application/json",
                "Authorization": checkly_api_key,
            },
            timeout=checkly_timeout,
        )
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        response.encoding = "utf-8"
        json_response: list[dict] = [
            {
                k: v
                for k, v in record.items()
                if k not in ("browserCheckResult", "apiCheckResult")
            }
            for record in response.json()
        ]
        row_count = len(json_response)
        if not row_count:
            return None, 0
    except HTTPError as http_err:
        raise Exception(f"HTTP error occurred, {response.json()}") from http_err
    else:
        # CONVERT RESPONSE TO POLARS DATAFRAME
        all_checks_results_df: pl.DataFrame = pl.from_dicts(
            json_response, infer_schema_length=1000
        )
        all_checks_results_df = all_checks_results_df.select(
            pl.exclude(
                [
                    col_name
                    for col_name, dtype in all_checks_results_df.schema.items()
                    if dtype in (pl.List, pl.Struct)
                ]
            )
        )
        all_checks_results_df = all_checks_results_df.with_columns(
            _transaction_date=datetime.fromtimestamp(payload.from_date, tz=pytz.utc),  # type: ignore[arg-type]
            _etl_date=datetime.now(tz=pytz.utc).now(),
        )
        # RENAME COLUMNS BY STANDARDS
        all_checks_results_df.columns = common_rename_to_standard_columns_name(
            all_checks_results_df.columns
        )
        return all_checks_results_df, row_count
