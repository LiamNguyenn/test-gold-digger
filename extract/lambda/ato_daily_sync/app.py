"""APP FOR DOWNLOAD AND IMPORT SUPERFUND ATO FILE."""

import logging
import os
from datetime import datetime
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import boto3
import polars as pl
import pytz  # type: ignore[import]
import requests
from chalice import Chalice, Rate
from chalicelib.drivers import RedshiftSqlDriver
from chalicelib.tools import AwsS3Client
from chalicelib.utils import (
    aws_init_assumed_session,
    common_rename_to_standard_columns_name,
    convert_polars_schema_info_to_redshift_datatype,
    get_polars_df_schema_info,
    read_fwf_to_dataframe,
)
from requests import HTTPError

app = Chalice(app_name="ato_daily_sync")

app.log.setLevel(logging.INFO)

SUPERFUND_ATO_DESTINATION_BUCKET = os.environ["SUPERFUND_ATO_DESTINATION_BUCKET"]
SUPERFUND_ATO_DESTINATION_SCHEMA = os.environ["SUPERFUND_ATO_DESTINATION_SCHEMA"]
SUPERFUND_ATO_DESTINATION_TABLE = os.environ["SUPERFUND_ATO_DESTINATION_TABLE"]
SUPERFUND_ATO_TEMP_SCHEMA = os.environ["SUPERFUND_ATO_TEMP_SCHEMA"]
SUPERFUND_ATO_BASE_URL = os.environ["SUPERFUND_ATO_BASE_URL"]
REDSHIFT_READ_S3_DESTINATION_ROLE = os.environ["REDSHIFT_READ_S3_DESTINATION_ROLE"]
REGION_NAME = os.environ["REGION_NAME"]
S3_UPLOAD_EXTRA_ARGS = {
    "ServerSideEncryption": "aws:kms",
    "SSEKMSKeyId": os.environ["S3_DESTINATION_BUCKET_KMS"],
}

ASSUMED_ACCOUNT_NUMBER = 979797940137
ASSUMED_ROLE_NAME = "lambda_trigger_role"
REDSHIFT_CLUSTER_NAME = "ehdw"
REDSHIFT_USER = "dbt_cloud"
REDSHIFT_DATABASE = "dev"
CHUNK_SIZE = 100  # determined based on API, memory constraints, experimentation


@app.schedule(Rate(24, Rate.HOURS))
def ato_daily_batch(event):  # noqa: ARG001, PLR0915
    """DAILY DOWNLOAD AND IMPORT SUPERFUND ATO FILE."""
    job_id = str(uuid4())
    app.log.info(f"Job 'push_process_event' with id: '{job_id}' now running")
    etl_date = datetime.now(tz=pytz.utc).date().strftime("%Y-%m-%d")
    start_date = datetime.now(tz=pytz.utc)
    local_tmp_path = f"/tmp/ABN_ATO_{job_id}_{etl_date}.txt"  # noqa: S108
    temp_table_name = f"temp_{SUPERFUND_ATO_DESTINATION_TABLE}_{job_id}"
    row_count: int = 0
    job_status = "PENDING"
    log_message = ""
    # construct client and driver
    s3_client = AwsS3Client(session=boto3.Session())
    assumed_session = aws_init_assumed_session(
        account_number=ASSUMED_ACCOUNT_NUMBER, role_name=ASSUMED_ROLE_NAME
    )
    redshift_driver = RedshiftSqlDriver(
        session=assumed_session,
        cluster_identifier=REDSHIFT_CLUSTER_NAME,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_USER,
    )
    try:
        r = requests.get(
            f"{SUPERFUND_ATO_BASE_URL}/Tools/DownloadUsiList?download=true",
            allow_redirects=True,
            timeout=120,
        )
        Path.open(Path(local_tmp_path), "wb").write(r.content)
        widths = [
            (0, 12),
            (12, 200),
            (213, 20),
            (234, 200),
            (435, 24),
            (460, 10),
            (471, 10),
        ]
        columns = [
            "ABN",
            "Fund name",
            "USI",
            "Product name",
            "Contribution restrictions",
            "From date",
            "To date",
        ]
        dtypes = [pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8]

        ato_df: pl.DataFrame = read_fwf_to_dataframe(
            local_tmp_path, widths, columns, dtypes
        )
        ato_df = ato_df[:-4]
        ato_df = ato_df.with_columns(
            _transaction_date=datetime.now(tz=pytz.utc).now(),
            _etl_date=datetime.now(tz=pytz.utc).now(),
        )
        ato_df.columns = common_rename_to_standard_columns_name(ato_df.columns)
        row_count = ato_df.select(pl.count())[0, 0]

        general_schema = get_polars_df_schema_info(ato_df)
        redshift_schema = convert_polars_schema_info_to_redshift_datatype(
            general_schema
        )

        csv_buffer = BytesIO()
        ato_df.write_csv(csv_buffer)
        prefix = f"{SUPERFUND_ATO_DESTINATION_SCHEMA}/{SUPERFUND_ATO_DESTINATION_TABLE}/ETL_DATE={etl_date}"
        file_name = f"ABN_ATO_{job_id}_refined.csv.gz"
        upload_info = s3_client.upload_and_compress_gzip_file_to_s3(
            byte_value=csv_buffer,
            bucket_name=SUPERFUND_ATO_DESTINATION_BUCKET,
            prefix=prefix,
            file_name=file_name,
            extra_args=S3_UPLOAD_EXTRA_ARGS,
        )
        upload_info.row_count = row_count
        redshift_driver.create_table(
            table=SUPERFUND_ATO_DESTINATION_TABLE,
            schema=SUPERFUND_ATO_DESTINATION_SCHEMA,
            columns_map=redshift_schema,
            create_if_not_exist=True,
        )
        redshift_driver.copy_from_s3_to_temp_table(
            temp_table_name=temp_table_name,
            temp_schema_name=SUPERFUND_ATO_TEMP_SCHEMA,
            schema=redshift_schema,
            s3_path=f"s3://{upload_info.s3_destination_bucket}/{upload_info.s3_destination_key}",
            iam_role=REDSHIFT_READ_S3_DESTINATION_ROLE,
            region=REGION_NAME,
            is_from_manifest=False,
        )
        redshift_driver.update_sink_table_structure_from_staging_table(
            sink_schema=SUPERFUND_ATO_DESTINATION_SCHEMA,
            sink_table=SUPERFUND_ATO_DESTINATION_TABLE,
            staging_table=temp_table_name,
            staging_schema=SUPERFUND_ATO_TEMP_SCHEMA,
        )
        redshift_driver.delete_insert(
            primary_table=SUPERFUND_ATO_DESTINATION_TABLE,
            primary_schema=SUPERFUND_ATO_DESTINATION_SCHEMA,
            staging_table=temp_table_name,
            staging_schema=SUPERFUND_ATO_TEMP_SCHEMA,
            keys=["abn", "usi"],
            columns=list(redshift_schema.keys()),
        )
        redshift_driver.drop_table(
            table=temp_table_name, schema=SUPERFUND_ATO_TEMP_SCHEMA
        )
        job_status = "SUCCESS"
        app.log.info(
            f"PROCESS COMPLETE FOR TABLE {SUPERFUND_ATO_DESTINATION_TABLE}, ROW PROCESSED: {row_count}"
        )
    except HTTPError as http_err:
        job_status = "HTTP FAIL"
        log_message = f"HTTP error occurred {http_err!s}"
        raise HTTPError(f"HTTP error occurred {http_err}") from http_err  # noqa: TRY003
    except Exception as err:  # noqa: BLE001
        job_status = "EXCEPTION FAIL"
        log_message = f"Other error occurred {err!s}"
        raise Exception(f"Other error occurred {err}") from err  # noqa: TRY003, TRY002
    finally:
        end_date = datetime.now(tz=pytz.utc)
        redshift_driver.insert_log(
            sync_log_table_name="sync_logs",
            sync_log_schema_name=SUPERFUND_ATO_DESTINATION_SCHEMA,
            job_id=job_id,
            org_job_id="",
            job_name="ato_daily_batch",
            schema_name=SUPERFUND_ATO_DESTINATION_SCHEMA,
            table_name=SUPERFUND_ATO_DESTINATION_TABLE,
            start_time=start_date,
            end_time=end_date,
            rows_updated_or_inserted=row_count,
            status=job_status,
            message=log_message,
        )
