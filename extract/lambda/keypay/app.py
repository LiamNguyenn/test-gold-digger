# ruff: noqa: F841, ERA001
"""Keypay Lambda Pipeline."""
import logging
import os
import re
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import boto3
import polars as pl
import pytz  # type: ignore[import]
from chalice import Chalice
from chalicelib.helper_models.keypay_config_models import KeypayConfig
from chalicelib.helper_models.keypay_pipeline_models import (
    LambdaKeypayEventPayLoad,
    RedshiftS3ProcessPayload,
    S3ProcessPayload,
)
from eh_lambda_utils.drivers import RedshiftSqlDriver
from eh_lambda_utils.models import (
    S3GetObjectResponse,
    S3ObjectInfo,
)
from eh_lambda_utils.tools import AwsS3Client
from eh_lambda_utils.utils import (
    aws_init_assumed_session,
    common_rename_to_standard_columns_name,
    convert_polars_schema_info_to_redshift_datatype,
    get_max_value_of_multiple_schema,
    get_polars_df_schema_info,
)

if TYPE_CHECKING:
    from eh_lambda_utils.models import (
        PolarsDataFrameColumnsInfo,
        S3UploadResponse,
    )

app = Chalice(app_name="keypay")
app.log.setLevel(logging.INFO)

ASSUMED_ACCOUNT_NUMBER = os.environ["ASSUMED_ACCOUNT_NUMBER"]
ASSUMED_ROLE_NAME = os.environ["ASSUMED_ROLE_NAME"]
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_CLUSTER_NAME"]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_READ_S3_DESTINATION_ROLE = os.environ["REDSHIFT_READ_S3_DESTINATION_ROLE"]
REGION_NAME = os.environ["REGION_NAME"]
S3_SINK_BUCKET = os.environ["S3_SINK_BUCKET"]
SQS_S3_PROCESS_QUEUE_URL = os.environ["SQS_S3_PROCESS_QUEUE_URL"]
SQS_S3_PROCESS_QUEUE = os.environ["SQS_S3_PROCESS_QUEUE"]
SQS_REDSHIFT_PROCESS_QUEUE_URL = os.environ["SQS_REDSHIFT_PROCESS_QUEUE_URL"]
SQS_REDSHIFT_PROCESS_QUEUE = os.environ["SQS_REDSHIFT_PROCESS_QUEUE"]
S3_UPLOAD_EXTRA_ARGS = {
    "ServerSideEncryption": "aws:kms",
    "SSEKMSKeyId": os.environ["S3_DESTINATION_BUCKET_KMS"],
}
S3_RUN_LOGS_KEYPAY = {
    "job_id": "varchar(256)",
    "org_job_id": "varchar(256)",
    "job_name": "varchar(256)",
    "s3_file_name": "varchar(2000)",
    "s3_bucket_name": "varchar(2000)",
    "s3_modified_time": "timestamp",
    "schema_name": "varchar(256)",
    "table_name": "varchar(256)",
    "start_time": "timestamp",
    "end_time": "timestamp",
    "rows_updated_or_inserted": "bigint",
    "status": "varchar(256)",
    "message": "varchar(4000)",
}

S3_SYNC_LOGS_KEYPAY = {
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


@app.lambda_function()
def scan_file_to_process(event, context):  # noqa: ARG001
    """
    Scan for files to run.

    @param event: event param pass when trigger lambda function
    @param context: lambda run context
    @return: None
    """
    org_job_id = str(uuid4())
    app.log.info(
        f"Job 'keypay-scan_file_to_process' with id: '{org_job_id}' now running"
    )
    pipeline_config = KeypayConfig(**event)

    # Initializing session
    assumed_prod_session = aws_init_assumed_session(
        account_number=ASSUMED_ACCOUNT_NUMBER, role_name=ASSUMED_ROLE_NAME
    )

    # Initializing S3's resources
    s3_prod_client = AwsS3Client(session=assumed_prod_session)

    # Initializing Redshift's client
    redshift_prod_driver = RedshiftSqlDriver(
        session=assumed_prod_session,
        cluster_identifier=REDSHIFT_CLUSTER_NAME,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_USER,
    )

    # Fetch processed file list
    logs_results = redshift_prod_driver.execute_query(
        "select distinct source from stg_keypay.sync_logs where upper(status) = 'SUCCESS'"
    )
    processed_key_set: set = {obj["source"] for obj in logs_results}

    # Fetch S3 file list
    s3_object_info_list: list[S3ObjectInfo] = s3_prod_client.list_objects(
        bucket_name=pipeline_config.s3_source_bucket,
        prefix=pipeline_config.s3_prefix,
    )
    s3_object_info_dict: dict[str, S3ObjectInfo] = {
        s3_object_info.Key: s3_object_info for s3_object_info in s3_object_info_list
    }

    # Final processable list
    processable_keys = set(s3_object_info_dict.keys()) - processed_key_set
    match_keys = [
        key
        for key in processable_keys
        if re.match(pipeline_config.s3_file_pattern, Path(key).name)
    ]
    if not match_keys:
        app.log.info(f"No file found for {pipeline_config.job_name}")
        return {
            "status": "SUCCESS",
            "message": f"No file found for {pipeline_config.job_name}",
            "data": [],
        }
    app.log.info(f"Found {len(match_keys)} file(s) for {pipeline_config.job_name}")
    return_data = []
    for key in match_keys:
        job_id = str(uuid4())
        push_event = LambdaKeypayEventPayLoad(
            org_job_id=org_job_id,
            job_id=job_id,
            job_name=pipeline_config.job_name,
            s3_process_payload=S3ProcessPayload(
                s3_source_bucket=pipeline_config.s3_source_bucket,
                s3_key=key,
                s3_object_info=s3_object_info_dict[key],
                s3_last_modified=str(s3_object_info_dict[key].LastModified),
                s3_size=s3_object_info_dict[key].Size,
                is_production_s3_source=pipeline_config.is_production_s3_source_bucket,
            ),
            redshift_s3_process_payload=RedshiftS3ProcessPayload(
                redshift_schema=pipeline_config.redshift_schema,
                redshift_temp_schema=pipeline_config.redshift_temp_schema,
                redshift_table=pipeline_config.redshift_table,
            ),
        )
        return_data.append(push_event.model_dump())
    return {
        "status": "SUCCESS",
        "message": f"Found {len(match_keys)} file(s) for {pipeline_config.job_name}: {match_keys}",
        "data": return_data,
    }


def handle_zip_file_and_upload_one_by_one(
    s3_client: AwsS3Client,
    s3_object_info: S3GetObjectResponse,
    s3_upload_prefix: str,
) -> tuple[list, list]:
    """
    Handle ZIP file and upload one by one.

    @param s3_client:
    @param s3_object_info:
    @param s3_upload_prefix:
    @return:
    """
    # Read ZIP file
    buffer = BytesIO(s3_object_info.body.read())
    zip_file = zipfile.ZipFile(buffer)
    # construct schema info list and detail uploaded files info
    schema_info_list = []
    detail_uploaded_files_info = []
    general_schema: dict = {}
    for filename in zip_file.namelist():
        try:
            scan_df = pl.read_csv(
                zip_file.open(filename).read(), n_rows=10, infer_schema_length=None
            )
            current_df_cols = set(scan_df.columns)
            if set(general_schema.keys()) != current_df_cols:
                general_schema.update(scan_df.schema)
        except pl.NoDataError:
            app.log.warning(f"{filename} is an empty file!")
        except Exception as e:  # noqa: BLE001
            raise Exception(  # noqa: TRY003, TRY002
                f"Unexpected error when process {filename}: {e}"
            ) from e
    if not general_schema:
        return [], []
    general_schema = {
        col_name: (pl.Utf8 if dtype in pl.INTEGER_DTYPES else dtype)
        for col_name, dtype in general_schema.items()
    }
    general_schema_df = pl.DataFrame(schema=general_schema)
    for filename in zip_file.namelist():
        try:
            zip_open: pl.DataFrame = pl.read_csv(
                zip_file.open(filename).read(), infer_schema_length=None
            )
            row_count = zip_open.select(pl.count())[0, 0]
            if row_count:
                zip_open = pl.concat(
                    [general_schema_df, zip_open], how="diagonal_relaxed"
                )
                zip_open = zip_open.with_columns(
                    _file=pl.lit(f"{s3_object_info.key}/{filename}"),
                    _transaction_date=s3_object_info.last_modified.date(),
                    _etl_date=datetime.now(tz=pytz.utc).now(),
                    _modified=s3_object_info.last_modified,
                )
                # apply overall columns
                zip_open.columns = common_rename_to_standard_columns_name(
                    zip_open.columns
                )

                # CONSTRUCT STATISTIC AND VARIABLE FOR UPLOADING PROCESS
                s3_upload_file_name = f"{filename}_refined.csv.gz"
                csv_buffer = BytesIO()
                zip_open.write_csv(csv_buffer)
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
                ] = get_polars_df_schema_info(df=zip_open)
                schema_info_list.append(df_schema_info)

                # CONSTRUCT COMPLETE DATA RESPONSE
                upload_info.row_count = row_count
                detail_uploaded_files_info.append(upload_info)
            else:
                continue
        except pl.NoDataError:
            app.log.warning(f"{filename} is an empty file!")
        except Exception as e:  # noqa: BLE001
            app.log.warning(f"Unexpected error when process {filename}: {e}")
            raise Exception(  # noqa: TRY003, TRY002
                f"Unexpected error when process {filename}: {e}"
            ) from e
    return detail_uploaded_files_info, schema_info_list


@app.lambda_function()
def process_s3_file_default_ram(event, context):  # noqa: ARG001, PLR0915
    """
    Process S3 file that receive on event.

    @param event:
    @param context:
    @return:
    """
    app.log.info(f"Event received {event}")
    log_status = "START"
    log_total_processed_row = 0
    log_message = ""
    # Timestamps process
    etl_date = datetime.now(tz=pytz.utc).date().strftime("%Y-%m-%d")
    start_time = datetime.now(tz=pytz.utc)
    s3_object_modified_time = datetime.now(tz=pytz.utc)  # create for later use

    # Parse event
    event_payload = LambdaKeypayEventPayLoad(**event)

    org_job_id, job_id, job_name, s3_process_payload, redshift_s3_process_payload = (
        event_payload.org_job_id,
        event_payload.job_id,
        event_payload.job_name,
        event_payload.s3_process_payload,
        event_payload.redshift_s3_process_payload,
    )

    # Handle ZIP file
    s3_upload_prefix = (
        f"{job_name}/ETL_DATE={etl_date}/FILE={s3_process_payload.s3_key}"
    )
    manifest_file_name = f"{job_name}_{job_id}.manifest"
    push_event = {}

    # Logging process
    app.log.info(
        f"Job 'process_s3_file' name '{job_name}' with id: '{job_id}' and original id trigger: '{org_job_id}' now running on {etl_date}"
    )

    # Initializing S3's resources
    assumed_prod_session = aws_init_assumed_session(
        account_number=ASSUMED_ACCOUNT_NUMBER, role_name=ASSUMED_ROLE_NAME
    )

    s3_sink_client = AwsS3Client(session=boto3.Session())
    if s3_process_payload.is_production_s3_source:
        s3_source_client = AwsS3Client(session=assumed_prod_session)
    else:
        s3_source_client = s3_sink_client

    # Initializing Redshift's client
    redshift_driver = RedshiftSqlDriver(
        session=assumed_prod_session,
        cluster_identifier=REDSHIFT_CLUSTER_NAME,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_USER,
    )

    try:
        # Read S3 file
        if str(s3_process_payload.s3_key).upper().endswith(".ZIP"):
            s3_object = s3_source_client.get_s3_object(
                bucket=s3_process_payload.s3_source_bucket,
                key=s3_process_payload.s3_key,
            )
            # Remove timezone info -> Pandas parse not error
            s3_object.last_modified = s3_object.last_modified.replace(tzinfo=None)
            s3_object_modified_time = s3_object.last_modified

            (
                detail_uploaded_files_info,
                schema_info_list,
            ) = handle_zip_file_and_upload_one_by_one(
                s3_client=s3_sink_client,
                s3_object_info=s3_object,
                s3_upload_prefix=s3_upload_prefix,
            )

            # CHECK IF THERE ARE ANY UPLOADING FILE
            if detail_uploaded_files_info:
                # VALIDATE IF ALL SCHEMA ARE THE SAME IN ALL EXPORT
                general_schema = get_max_value_of_multiple_schema(schema_info_list)
                redshift_schema = convert_polars_schema_info_to_redshift_datatype(
                    general_schema
                )

                # CONSTRUCT VARIABLE FOR DATA LOGS
                log_total_processed_row = sum(
                    file_detail.row_count for file_detail in detail_uploaded_files_info
                )

                # CONSTRUCT MANIFEST FOR REDSHIFT CONSUMPTION
                manifest_upload_response = s3_sink_client.construct_and_upload_manifest(
                    detail_files_info=detail_uploaded_files_info,
                    bucket_name=S3_SINK_BUCKET,
                    prefix=s3_upload_prefix,
                    file_name=manifest_file_name,
                    extra_args=S3_UPLOAD_EXTRA_ARGS,
                )
                app.log.info(
                    f"SUCCESSFUL WRITE MANIFEST FILE TO "
                    f"s3://{manifest_upload_response.s3_destination_bucket}/{manifest_upload_response.s3_destination_key}"
                )

                # construct redshift s3 info
                event_payload.redshift_s3_process_payload.s3_key = (
                    manifest_upload_response.s3_destination_key
                )
                event_payload.redshift_s3_process_payload.s3_bucket = (
                    manifest_upload_response.s3_destination_bucket
                )
                event_payload.redshift_s3_process_payload.redshift_table_schema = (
                    redshift_schema
                )

                push_event = event_payload.model_dump()
                app.log.info(f"Pushing event {push_event}")
                log_message = f"PROCESS S3 FILE {s3_process_payload.s3_key} SUCCESSFUL, TOTAL: {log_total_processed_row} row(s)"
            else:
                log_message = f"No file found in file {s3_process_payload.s3_key}"
        else:
            log_message = (
                f"File {s3_process_payload.s3_key} is not ZIP file - Not processable"
            )
        log_status = "SUCCESS"
        app.log.info(log_message)
        return {  # noqa: TRY300
            "status": log_status,
            "message": log_message,
            "data": push_event,
        }
    except Exception as err:  # noqa: BLE001
        log_status = "FAILED"
        log_message = f"Other error occurred {err}"
        raise Exception(log_message) from err  # noqa: TRY002
    finally:
        sync_logs_body = {
            "sync_log_table_name": "s3_run_logs",
            "sync_log_schema_name": redshift_s3_process_payload.redshift_schema,
            "sync_log_schema": S3_RUN_LOGS_KEYPAY,
            "job_id": job_id,
            "org_job_id": org_job_id,
            "job_name": f"{job_name}_process_s3_file",
            "s3_file_name": s3_process_payload.s3_key,
            "s3_bucket_name": s3_process_payload.s3_source_bucket,
            "s3_modified_time": s3_object_modified_time,
            "schema_name": redshift_s3_process_payload.redshift_schema,
            "table_name": redshift_s3_process_payload.redshift_table,
            "start_time": start_time,
            "end_time": datetime.now(tz=pytz.utc),
            "rows_updated_or_inserted": log_total_processed_row,
            "status": log_status,
            "message": log_message.replace("'", "''"),
        }
        redshift_driver.insert_log(**sync_logs_body)
        app.log.info(
            f"Job 'process_s3_file' name '{job_name}' with id: '{job_id}' and original id trigger: '{org_job_id}' now finished, time elapsed: {datetime.now(tz=pytz.utc) - start_time}"
        )


@app.lambda_function()
def process_s3_file_small_ram(event, context):
    """Process S3 file that receive on event on smaller ram."""
    res = process_s3_file_default_ram(event, context)
    return res


@app.lambda_function()
def process_s3_file_big_ram(event, context):
    """Process S3 file that receive on event on bigger ram."""
    res = process_s3_file_default_ram(event, context)
    return res


@app.lambda_function()
def process_s3_to_redshift(event, context):  # noqa: ARG001
    """
    Process S3 file to Redshift that receive on event.

    @param event:
    @param context:
    @return:
    """
    log_status = "START"
    log_message = ""
    num_records_inserted = -1
    event_payload = LambdaKeypayEventPayLoad(**event)
    (
        org_job_id,
        job_id,
        job_name,
        s3_process_payload,
        redshift_s3_process_payload,
    ) = (
        event_payload.org_job_id,
        event_payload.job_id,
        event_payload.job_name,
        event_payload.s3_process_payload,
        event_payload.redshift_s3_process_payload,
    )

    # Timestamps process
    etl_date = datetime.now(tz=pytz.utc).date().strftime("%Y-%m-%d")
    start_time = datetime.now(tz=pytz.utc)

    # Logging process
    app.log.info(
        f"Job 'process_s3_to_redshift' name '{job_name}' with id: '{job_id}' and original id trigger: '{org_job_id}' now running on {etl_date}"
    )
    app.log.info(f"Event received {event}")

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

    try:
        # CREATE TARGET TABLE IF NOT EXIST
        redshift_driver.create_table(
            table=redshift_s3_process_payload.redshift_table,
            schema=redshift_s3_process_payload.redshift_schema,
            columns_map=redshift_s3_process_payload.redshift_table_schema,
            create_if_not_exist=True,
        )

        temp_table_name = f"temp_{redshift_s3_process_payload.redshift_table}_{job_id}"

        # DROP TEMP TABLE IF EXIST
        redshift_driver.drop_table(
            table=temp_table_name,
            schema=redshift_s3_process_payload.redshift_temp_schema,
        )
        redshift_driver.copy_from_s3_to_temp_table(
            temp_table_name=temp_table_name,
            temp_schema_name=redshift_s3_process_payload.redshift_temp_schema,
            schema=redshift_s3_process_payload.redshift_table_schema,
            s3_path=f"s3://{redshift_s3_process_payload.s3_bucket}/{redshift_s3_process_payload.s3_key}",
            iam_role=REDSHIFT_READ_S3_DESTINATION_ROLE,
            region=REGION_NAME,
            is_from_manifest=True,
        )
        count_result = redshift_driver.execute_query(
            f'select count(1) as count_num from "{redshift_s3_process_payload.redshift_temp_schema}"."{temp_table_name}"'  # noqa: S608
        )
        num_records_inserted = count_result[0].get("count_num") or -1
        redshift_driver.update_sink_table_structure_from_staging_table(
            sink_schema=redshift_s3_process_payload.redshift_schema,
            sink_table=redshift_s3_process_payload.redshift_table,
            staging_table=temp_table_name,
            staging_schema=redshift_s3_process_payload.redshift_temp_schema,
        )
        redshift_driver.insert_into_sink_table_from_staging_table(
            primary_table=redshift_s3_process_payload.redshift_table,
            primary_schema=redshift_s3_process_payload.redshift_schema,
            staging_table=temp_table_name,
            staging_schema=redshift_s3_process_payload.redshift_temp_schema,
            columns=list(redshift_s3_process_payload.redshift_table_schema.keys()),
        )
        # redshift_driver.delete_insert(
        #     primary_table=redshift_s3_process_payload.redshift_table,
        #     primary_schema=redshift_s3_process_payload.redshift_schema,
        #     staging_table=temp_table_name,
        #     staging_schema=redshift_s3_process_payload.redshift_temp_schema,
        #     keys=["_file"],
        #     columns=list(redshift_s3_process_payload.redshift_table_schema.keys()),
        # )
        redshift_driver.drop_table(
            table=temp_table_name,
            schema=redshift_s3_process_payload.redshift_temp_schema,
        )
        log_status = "SUCCESS"
        log_message = f"PROCESS COMPLETE FOR TABLE {redshift_s3_process_payload.redshift_table} with {num_records_inserted} row(s)"
        app.log.info(log_message)
        redshift_driver.insert_log(
            sync_log_table_name="sync_logs",
            sync_log_schema_name=redshift_s3_process_payload.redshift_schema,
            sync_log_schema=S3_SYNC_LOGS_KEYPAY,
            job_id=job_id,
            org_job_id=org_job_id,
            job_name=job_name,
            source=s3_process_payload.s3_key,
            schema_name=redshift_s3_process_payload.redshift_schema,
            table_name=redshift_s3_process_payload.redshift_table,
            start_time=start_time,
            end_time=datetime.now(tz=pytz.utc),
            rows_updated_or_inserted=num_records_inserted,
            status=log_status,
            message=log_message.replace("'", "''"),
        )
        return {"status": log_status, "message": log_message}  # noqa: TRY300
    except Exception as err:  # noqa: BLE001
        log_status = "FAILED"
        log_message = f"Other error occurred {err}"
        raise Exception(log_message) from err  # noqa: TRY002
    finally:
        end_time = datetime.now(tz=pytz.utc)
        sync_logs_body = {
            "sync_log_table_name": "s3_run_logs",
            "sync_log_schema_name": redshift_s3_process_payload.redshift_schema,
            "sync_log_schema": S3_RUN_LOGS_KEYPAY,
            "job_id": job_id,
            "org_job_id": org_job_id,
            "job_name": f"{job_name}_process_s3_to_redshift",
            "s3_file_name": s3_process_payload.s3_key,
            "s3_bucket_name": s3_process_payload.s3_source_bucket,
            "s3_modified_time": end_time,
            "schema_name": redshift_s3_process_payload.redshift_schema,
            "table_name": redshift_s3_process_payload.redshift_table,
            "start_time": start_time,
            "end_time": end_time,
            "rows_updated_or_inserted": num_records_inserted,
            "status": log_status,
            "message": log_message.replace("'", "''"),
        }
        redshift_driver.insert_log(**sync_logs_body)
        app.log.info(
            f"Job 'process_s3_to_redshift' name '{job_name}' with id: '{job_id}' and original id trigger: '{org_job_id}' now finished, time elapsed: {end_time - start_time}"
        )
