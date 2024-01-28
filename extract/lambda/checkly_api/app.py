# ruff: noqa: F841, ERA001, TRY002, TRY003
"""Checkly Lambda Pipeline."""
import logging
import os
import time
from datetime import datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING
from uuid import uuid4

import boto3
import pytz  # type: ignore[import]
from chalice import Chalice
from chalicelib.helper.checkly_api_call import (
    all_check_results_api_call,
    all_checks_api_call,
)
from chalicelib.helper.checkly_models import (
    LambdaEventPayLoadListAllCheckResult,
    LambdaEventScanAllChecks,
    PayLoadListAllCheckResult,
    PayLoadListAllChecks,
)
from eh_lambda_utils.drivers import RedshiftSqlDriver
from eh_lambda_utils.tools import AwsS3Client
from eh_lambda_utils.utils import (
    aws_init_assumed_session,
    convert_polars_schema_info_to_redshift_datatype,
    get_max_value_of_multiple_schema,
    get_polars_df_schema_info,
)

if TYPE_CHECKING:
    from eh_lambda_utils.models import (
        PolarsDataFrameColumnsInfo,
        S3UploadResponse,
    )

app = Chalice(app_name="checkly")
app.log.setLevel(logging.INFO)

CHECKLY_API_BASE_URL = os.environ["CHECKLY_API_BASE_URL"]
CHECKLY_API_KEY = os.environ["CHECKLY_API_KEY"]
CHECKLY_ACCOUNT_ID = os.environ["CHECKLY_ACCOUNT_ID"]
CHECKLY_TIMEOUT: int = int(os.environ["CHECKLY_TIMEOUT"])
ASSUMED_ACCOUNT_NUMBER = os.environ["ASSUMED_ACCOUNT_NUMBER"]
ASSUMED_ROLE_NAME = os.environ["ASSUMED_ROLE_NAME"]
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_CLUSTER_NAME"]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_READ_S3_DESTINATION_ROLE = os.environ["REDSHIFT_READ_S3_DESTINATION_ROLE"]
REGION_NAME = os.environ["REGION_NAME"]
S3_SINK_BUCKET = os.environ["S3_SINK_BUCKET"]
S3_UPLOAD_EXTRA_ARGS = {
    "ServerSideEncryption": "aws:kms",
    "SSEKMSKeyId": os.environ["S3_DESTINATION_BUCKET_KMS"],
}

SYNC_LOGS_SCHEMA = {
    "job_id": "varchar(256)",
    "org_job_id": "varchar(256)",
    "job_name": "varchar(256)",
    "source": "varchar(2000)",
    "schema_name": "varchar(256)",
    "table_name": "varchar(256)",
    "start_time": "timestamp",
    "end_time": "timestamp",
    "rows_updated_or_inserted": "bigint",
    "status": "varchar(256)",
    "message": "varchar(4000)",
}


def upload_to_redshift(
    redshift_driver: RedshiftSqlDriver,
    redshift_schema: str,
    redshift_temp_schema: str,
    redshift_table: str,
    redshift_table_schema: dict[str, str],
    redshift_primary_key: list[str],
    s3_path: str,
    unique_identifier: str = str(uuid4()),
    is_from_manifest: bool = True,  # noqa: FBT001, FBT002
) -> int:
    """Upload data to Redshift."""
    try:
        # CREATE TARGET TABLE IF NOT EXIST
        redshift_driver.create_table(
            table=redshift_table,
            schema=redshift_schema,
            columns_map=redshift_table_schema,
            create_if_not_exist=True,
        )

        temp_table_name = f"temp_{redshift_table}_{unique_identifier}"

        # DROP TEMP TABLE IF EXIST
        redshift_driver.drop_table(
            table=temp_table_name,
            schema=redshift_temp_schema,
        )

        # COPY DATA FROM S3 TO TEMP TABLE
        redshift_driver.copy_from_s3_to_temp_table(
            temp_table_name=temp_table_name,
            temp_schema_name=redshift_temp_schema,
            schema=redshift_table_schema,
            s3_path=s3_path,
            iam_role=REDSHIFT_READ_S3_DESTINATION_ROLE,
            region=REGION_NAME,
            is_from_manifest=is_from_manifest,
        )

        # Get table actual row count
        count_result = redshift_driver.execute_query(
            f'select count(1) as count_num from "{redshift_temp_schema}"."{temp_table_name}"'  # noqa: S608
        )
        num_records_inserted = count_result[0].get("count_num") or -1

        # UPDATE SINK TABLE STRUCTURE FROM STAGING TABLE
        redshift_driver.update_sink_table_structure_from_staging_table(
            sink_schema=redshift_schema,
            sink_table=redshift_table,
            staging_table=temp_table_name,
            staging_schema=redshift_temp_schema,
        )
        redshift_driver.delete_insert(
            primary_table=redshift_table,
            primary_schema=redshift_schema,
            staging_table=temp_table_name,
            staging_schema=redshift_temp_schema,
            keys=redshift_primary_key,
            columns=list(redshift_table_schema.keys()),
        )

        # FINALIZE PROCESS
        redshift_driver.drop_table(
            table=temp_table_name,
            schema=redshift_temp_schema,
        )
    except Exception as err:
        log_message = f"Upload to Redshift error occurred: {err}"
        app.log.exception(log_message)
        raise Exception(log_message) from err
    else:
        return num_records_inserted


@app.lambda_function()
def load_all_checks_api(event, context):  # noqa: ARG001, PLR0915
    """Load all checks from checkly API."""
    # FIRST STEP: INITIALIZE VARIABLES + AWS SESSION
    event_payload = LambdaEventScanAllChecks(**event)

    # GENERAL INFO
    job_id = str(uuid4())
    job_name: str = event_payload.job_name
    app.log.info(f"Job id: {job_id}, name: {job_name}, event received {event}")

    # TIME VARIABLES
    etl_date = datetime.now(tz=pytz.utc).date().strftime("%Y-%m-%d")
    start_time = datetime.now(tz=pytz.utc)

    # VARIABLES FOR LOGIC PROCESS
    payload = PayLoadListAllChecks(limit=event_payload.chunk_size, page=1)
    schema_info_list = []
    detail_uploaded_files_info = []

    # VARIABLES FOR UPLOAD AND INSERT PROCESS
    redshift_schema: str = event_payload.redshift_schema
    redshift_temp_schema: str = event_payload.redshift_temp_schema
    redshift_table: str = event_payload.redshift_table
    s3_upload_prefix = f"{job_name}/ETL_DATE={etl_date}"

    # VARIABLES FOR LOGGING PROCESS
    log_status = "START"
    log_message = ""
    log_total_processed_row = 0

    # Initializing Redshift's client
    assumed_prod_session = aws_init_assumed_session(
        account_number=ASSUMED_ACCOUNT_NUMBER, role_name=ASSUMED_ROLE_NAME
    )
    redshift_driver = RedshiftSqlDriver(
        session=assumed_prod_session,
        cluster_identifier=REDSHIFT_CLUSTER_NAME,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_USER,
    )
    # Initializing S3's client
    s3_client = AwsS3Client(session=boto3.Session())

    # SECOND STEP: PROCESS DATA
    try:
        flag = True
        while flag:
            # CALL API TO GET DATA
            all_checks_df, row_count = all_checks_api_call(
                checkly_api_base_url=CHECKLY_API_BASE_URL,
                checkly_account_id=CHECKLY_ACCOUNT_ID,
                checkly_api_key=CHECKLY_API_KEY,
                checkly_timeout=CHECKLY_TIMEOUT,
                payload=payload,
            )
            if row_count:
                # CONSTRUCT STATISTIC AND VARIABLE FOR UPLOADING PROCESS
                s3_upload_file_name = f"{redshift_table}_{payload.page}_{datetime.now(tz=pytz.utc).strftime('%Y-%m-%d_%H:%M:%S')}_refined.csv.gz"
                csv_buffer = BytesIO()
                all_checks_df.write_csv(csv_buffer)

                # UPLOAD TO S3
                upload_info: S3UploadResponse = (
                    s3_client.upload_and_compress_gzip_file_to_s3(
                        byte_value=csv_buffer,
                        bucket_name=S3_SINK_BUCKET,
                        prefix=s3_upload_prefix,
                        file_name=s3_upload_file_name,
                        extra_args=S3_UPLOAD_EXTRA_ARGS,
                    )
                )
                app.log.info(
                    f"SUCCESSFUL WRITE FILE TO s3://{upload_info.s3_destination_bucket}/{upload_info.s3_destination_key}"
                )
                # Construct for future schema comparing
                df_schema_info: dict[
                    str, PolarsDataFrameColumnsInfo
                ] = get_polars_df_schema_info(df=all_checks_df)
                schema_info_list.append(df_schema_info)

                # CONSTRUCT COMPLETE DATA RESPONSE
                upload_info.row_count = row_count
                detail_uploaded_files_info.append(upload_info)

                # LOOP IF DATA STILL HAVE
                if row_count < payload.limit:
                    flag = False
                payload.page += 1
                log_total_processed_row += row_count
            else:
                app.log.info(f"NO DATA FOUND WITH CURRENT PAYLOAD: {payload}")
                flag = False
        # CHECK IF THERE ARE ANY UPLOADING FILE
        if detail_uploaded_files_info:
            # VALIDATE IF ALL SCHEMA ARE THE SAME IN ALL EXPORT
            general_schema = get_max_value_of_multiple_schema(schema_info_list)
            redshift_table_schema = convert_polars_schema_info_to_redshift_datatype(
                general_schema
            )

            # CONSTRUCT MANIFEST FOR REDSHIFT CONSUMPTION
            manifest_file_name = f"{job_name}_{job_id}.manifest"
            manifest_upload_response = s3_client.construct_and_upload_manifest(
                detail_files_info=detail_uploaded_files_info,
                bucket_name=S3_SINK_BUCKET,
                prefix=s3_upload_prefix,
                file_name=manifest_file_name,
                extra_args=S3_UPLOAD_EXTRA_ARGS,
            )

            # LOADING DATA FROM S3 TO REDSHIFT
            s3_manifest_path = f"s3://{manifest_upload_response.s3_destination_bucket}/{manifest_upload_response.s3_destination_key}"
            app.log.info(f"SUCCESSFUL WRITE MANIFEST FILE TO {s3_manifest_path}")
            num_records_inserted = upload_to_redshift(
                redshift_driver=redshift_driver,
                redshift_schema=redshift_schema,
                redshift_temp_schema=redshift_temp_schema,
                redshift_table=redshift_table,
                redshift_table_schema=redshift_table_schema,
                redshift_primary_key=["id"],
                s3_path=s3_manifest_path,
                unique_identifier=job_id,
                is_from_manifest=True,
            )
            log_message = f"PROCESS COMPLETE FOR TABLE {redshift_table} with {num_records_inserted} row(s)"
            log_status = "SUCCESS"
            if num_records_inserted != log_total_processed_row:
                log_status = "WARNING"
                log_message = (
                    f"PROCESS COMPLETE FOR TABLE {redshift_table} with {num_records_inserted} row(s), "
                    f"but total processed row is {log_total_processed_row} row(s)"
                )
            app.log.info(log_message)
    except Exception as err:  # noqa: BLE001
        log_status = "FAILED"
        log_message = f"Process Error: {err}"
        raise Exception(log_message) from err
    finally:
        end_time = datetime.now(tz=pytz.utc)
        sync_logs_body = {
            "sync_log_table_name": "sync_logs",
            "sync_log_schema_name": redshift_schema,
            "sync_log_schema": SYNC_LOGS_SCHEMA,
            "job_id": job_id,
            "org_job_id": "",
            "job_name": job_name,
            "source": f"{CHECKLY_API_BASE_URL}/v1/checks",
            "schema_name": redshift_schema,
            "table_name": redshift_table,
            "start_time": start_time,
            "end_time": end_time,
            "rows_updated_or_inserted": log_total_processed_row,
            "status": log_status,
            "message": log_message.replace("'", "''"),
        }
        redshift_driver.insert_log(**sync_logs_body)
        app.log.info(
            f"Job 'checkly-load_all_checks_api' name '{job_name}' with id: '{job_id}' now finished, time elapsed: {end_time - start_time}"
        )


@app.lambda_function()
def scan_all_checks(event, context):  # noqa: ARG001
    """Scan all checks of checkly."""
    # FIRST STEP: INITIALIZE VARIABLES
    pipeline_config = LambdaEventScanAllChecks(**event)
    org_job_id = str(uuid4())
    app.log.info(f"Job 'checkly-scan_all_checks' with id: '{org_job_id}' now running")

    # Initializing session
    assumed_prod_session = aws_init_assumed_session(
        account_number=ASSUMED_ACCOUNT_NUMBER, role_name=ASSUMED_ROLE_NAME
    )

    # Initializing Redshift's client
    redshift_prod_driver = RedshiftSqlDriver(
        session=assumed_prod_session,
        cluster_identifier=REDSHIFT_CLUSTER_NAME,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_USER,
    )

    # Fetch all active checks, list of Obj: {id, check_type, locations}
    checks_results = redshift_prod_driver.execute_query(
        """select nvl("id",'') as id, nvl("check_type",'') as check_type, nvl("locations",'') as location from "dev"."stg_checkly"."all_checks" where activated=true"""
    )

    if not checks_results:
        app.log.info("No active checks found")
        return {
            "status": "SUCCESS",
            "message": "No check found",
            "data": [],
        }
    app.log.info(f"Found {len(checks_results)} checks to process")
    return_data = []
    for check in checks_results:
        check_id = check.get("id")
        check_type = check.get("check_type")
        location = check.get("location")
        push_event = LambdaEventPayLoadListAllCheckResult(
            org_job_id=org_job_id,
            job_id=str(uuid4()),
            job_name=pipeline_config.job_name,
            redshift_schema=pipeline_config.redshift_schema,
            redshift_temp_schema=pipeline_config.redshift_temp_schema,
            redshift_table=pipeline_config.redshift_table,
            check_id=check_id,
            payload=PayLoadListAllCheckResult(
                limit=pipeline_config.chunk_size,
                page=1,
                from_date=time.mktime(
                    (datetime.now(tz=pytz.utc).now() - timedelta(hours=6)).timetuple()
                ),
                to_date=time.mktime(datetime.now(tz=pytz.utc).now().timetuple()),
                location=location or None,
                checkType=check_type or None,
                hasFailures=None,
                resultType=None,
            ),
        )
        return_data.append(push_event.model_dump())
    return {
        "status": "SUCCESS",
        "message": f"Found {len(checks_results)} checks to process",
        "data": return_data,
    }


@app.lambda_function()
def load_all_check_results_api(event, context):  # noqa: ARG001, PLR0915
    """Load all check results from checkly API."""
    # FIRST STEP: INITIALIZE VARIABLES + AWS SESSION
    event_payload = LambdaEventPayLoadListAllCheckResult(**event)

    # GENERAL INFO
    org_job_id = event_payload.org_job_id
    job_id = event_payload.job_id
    job_name = event_payload.job_name
    app.log.info(
        f"Job id: {job_id}, org job id: {org_job_id}, name: {job_name}, event received {event}"
    )

    # TIME VARIABLES
    etl_date = datetime.now(tz=pytz.utc).date().strftime("%Y-%m-%d")
    start_time = datetime.now(tz=pytz.utc)

    # VARIABLES FOR LOGIC PROCESS
    checkly_check_id = event_payload.check_id
    payload = event_payload.payload
    schema_info_list = []
    detail_uploaded_files_info = []

    # VARIABLES FOR UPLOAD AND INSERT PROCESS
    redshift_schema: str = event_payload.redshift_schema
    redshift_temp_schema: str = event_payload.redshift_temp_schema
    redshift_table: str = event_payload.redshift_table
    s3_upload_prefix = f"{job_name}/ETL_DATE={etl_date}"

    # VARIABLES FOR LOGGING PROCESS
    log_status = "START"
    log_message = ""
    log_total_processed_row = 0

    # Initializing Redshift's client
    assumed_prod_session = aws_init_assumed_session(
        account_number=ASSUMED_ACCOUNT_NUMBER, role_name=ASSUMED_ROLE_NAME
    )
    redshift_driver = RedshiftSqlDriver(
        session=assumed_prod_session,
        cluster_identifier=REDSHIFT_CLUSTER_NAME,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_USER,
    )
    # Initializing S3's client
    s3_client = AwsS3Client(session=boto3.Session())

    # SECOND STEP: PROCESS DATA
    try:
        flag = True
        while flag:
            # CALL API TO GET DATA
            all_checks_results_df, row_count = all_check_results_api_call(
                checkly_api_base_url=CHECKLY_API_BASE_URL,
                checkly_account_id=CHECKLY_ACCOUNT_ID,
                checkly_api_key=CHECKLY_API_KEY,
                checkly_timeout=CHECKLY_TIMEOUT,
                check_id=checkly_check_id,
                payload=payload,
            )
            if row_count:
                # CONSTRUCT STATISTIC AND VARIABLE FOR UPLOADING PROCESS
                s3_upload_file_name = f"{redshift_table}_{payload.page}_{datetime.now(tz=pytz.utc).strftime('%Y-%m-%d_%H:%M:%S')}_refined.csv.gz"
                csv_buffer = BytesIO()
                all_checks_results_df.write_csv(csv_buffer)

                # UPLOAD TO S3
                upload_info: S3UploadResponse = (
                    s3_client.upload_and_compress_gzip_file_to_s3(
                        byte_value=csv_buffer,
                        bucket_name=S3_SINK_BUCKET,
                        prefix=s3_upload_prefix,
                        file_name=s3_upload_file_name,
                        extra_args=S3_UPLOAD_EXTRA_ARGS,
                    )
                )
                app.log.info(
                    f"SUCCESSFUL WRITE FILE TO s3://{upload_info.s3_destination_bucket}/{upload_info.s3_destination_key}"
                )
                # Construct for future schema comparing
                df_schema_info: dict[
                    str, PolarsDataFrameColumnsInfo
                ] = get_polars_df_schema_info(df=all_checks_results_df)
                schema_info_list.append(df_schema_info)

                # CONSTRUCT COMPLETE DATA RESPONSE
                upload_info.row_count = row_count
                detail_uploaded_files_info.append(upload_info)

                # LOOP IF DATA STILL HAVE
                if row_count < payload.limit:
                    flag = False
                payload.page += 1
                log_total_processed_row += row_count
            else:
                app.log.info(f"NO DATA FOUND WITH CURRENT PAYLOAD: {payload}")
                flag = False
        # CHECK IF THERE ARE ANY UPLOADING FILE
        if detail_uploaded_files_info:
            # VALIDATE IF ALL SCHEMA ARE THE SAME IN ALL EXPORT
            general_schema = get_max_value_of_multiple_schema(schema_info_list)
            redshift_table_schema = convert_polars_schema_info_to_redshift_datatype(
                general_schema
            )

            # CONSTRUCT MANIFEST FOR REDSHIFT CONSUMPTION
            manifest_file_name = f"{job_name}_{job_id}.manifest"
            manifest_upload_response = s3_client.construct_and_upload_manifest(
                detail_files_info=detail_uploaded_files_info,
                bucket_name=S3_SINK_BUCKET,
                prefix=s3_upload_prefix,
                file_name=manifest_file_name,
                extra_args=S3_UPLOAD_EXTRA_ARGS,
            )

            # LOADING DATA FROM S3 TO REDSHIFT
            s3_manifest_path = f"s3://{manifest_upload_response.s3_destination_bucket}/{manifest_upload_response.s3_destination_key}"
            app.log.info(f"SUCCESSFUL WRITE MANIFEST FILE TO {s3_manifest_path}")
            num_records_inserted = upload_to_redshift(
                redshift_driver=redshift_driver,
                redshift_schema=redshift_schema,
                redshift_temp_schema=redshift_temp_schema,
                redshift_table=redshift_table,
                redshift_table_schema=redshift_table_schema,
                redshift_primary_key=["id"],
                s3_path=s3_manifest_path,
                unique_identifier=job_id,
                is_from_manifest=True,
            )
            log_message = f"PROCESS COMPLETE FOR TABLE {redshift_table} with {num_records_inserted} row(s)"
            log_status = "SUCCESS"
            if num_records_inserted != log_total_processed_row:
                log_status = "WARNING"
                log_message = (
                    f"PROCESS COMPLETE FOR TABLE {redshift_table} with {num_records_inserted} row(s), "
                    f"but total processed row is {log_total_processed_row} row(s)"
                )
            app.log.info(log_message)
    except Exception as err:  # noqa: BLE001
        log_status = "FAILED"
        log_message = f"Process Error: {err}"
        raise Exception(log_message) from err
    finally:
        end_time = datetime.now(tz=pytz.utc)
        sync_logs_body = {
            "sync_log_table_name": "sync_logs",
            "sync_log_schema_name": redshift_schema,
            "sync_log_schema": SYNC_LOGS_SCHEMA,
            "job_id": job_id,
            "org_job_id": org_job_id,
            "job_name": job_name,
            "source": f"{CHECKLY_API_BASE_URL}/v1/check-results/{checkly_check_id}",
            "schema_name": redshift_schema,
            "table_name": redshift_table,
            "start_time": start_time,
            "end_time": end_time,
            "rows_updated_or_inserted": log_total_processed_row,
            "status": log_status,
            "message": log_message.replace("'", "''"),
        }
        redshift_driver.insert_log(**sync_logs_body)
        app.log.info(
            f"Job 'checkly-load_all_check_results_api' name '{job_name}' with id: '{job_id}' now finished, time elapsed: {end_time - start_time}"
        )
