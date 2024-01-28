# type: ignore
# ruff: noqa
import zipfile
from datetime import datetime
from io import BytesIO

import boto3
import pytest
import pytz
from eh_lambda_utils.models import S3GetObjectResponse, PolarsDataFrameColumnsInfo
from eh_lambda_utils.tools import AwsS3Client
from eh_lambda_utils.utils import (
    common_rename_to_standard_columns_name,
    get_polars_df_schema_info,
    get_max_value_of_multiple_schema,
    convert_polars_schema_info_to_redshift_datatype,
)

from ..chalicelib.helper_models.keypay_pipeline_models import LambdaKeypayEventPayLoad
import polars as pl
from polars import Utf8, Date, Datetime


class TestScanFileToProcess:
    def handle_zip_file_and_upload_one_by_one(
        self,
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
        general_schema = {}
        for filename in zip_file.namelist():
            try:
                scan_df = pl.read_csv(
                    zip_file.open(filename).read(), n_rows=10, infer_schema_length=None
                )
                current_df_cols = set(scan_df.columns)
                print(f"Scan {filename} success!")
                print(scan_df.schema)
                if set(general_schema.keys()) != current_df_cols:
                    general_schema.update(scan_df.schema)
                print(scan_df.columns)
            except pl.NoDataError:
                print(f"{filename} is an empty file!")
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
        print(general_schema_df)
        print(general_schema)
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

                    print("-----------------------------------")
                    print(filename)
                    print(zip_open.columns)
                    print(zip_open.select(pl.col("allocate_balance")).head(5))
                    print("-----------------------------------")

                    # CONSTRUCT STATISTIC AND VARIABLE FOR UPLOADING PROCESS
                    s3_upload_file_name = f"{filename}_refined.csv.gz"
                    # csv_buffer = BytesIO()
                    # zip_open.write_csv(csv_buffer)
                    # upload_info: S3UploadResponse = (
                    #     s3_client.upload_and_compress_gzip_file_to_s3(
                    #         byte_value=csv_buffer,
                    #         bucket_name=S3_SINK_BUCKET,
                    #         prefix=s3_upload_prefix,
                    #         file_name=s3_upload_file_name,
                    #         extra_args=S3_UPLOAD_EXTRA_ARGS,
                    #     )
                    # )
                    print(
                        f"SUCCESSFUL WRITE FILE TO s3://eh-data-team-refined-data/{s3_upload_prefix}/{s3_upload_file_name}"
                    )

                    # Construct for future schema comparing
                    df_schema_info: dict[
                        str, PolarsDataFrameColumnsInfo
                    ] = get_polars_df_schema_info(df=zip_open)
                    schema_info_list.append(df_schema_info)

                    # CONSTRUCT COMPLETE DATA RESPONSE
                    # upload_info.row_count = row_count
                    # detail_uploaded_files_info.append(upload_info)
                else:
                    continue
            except pl.NoDataError:
                print(f"{filename} is an empty file!")
            except Exception as e:  # noqa: BLE001
                print(f"Unexpected error when process {filename}: {e}")
                raise Exception(  # noqa: TRY003, TRY002
                    f"Unexpected error when process {filename}: {e}"
                ) from e
        return detail_uploaded_files_info, schema_info_list

    @pytest.fixture()
    def mock_session(self):
        session = boto3.Session(profile_name="979797940137_DataTeamDeveloper")
        return session

    @pytest.fixture()
    def mock_event(self):
        event = {
            "org_job_id": "4880371d-3370-4f4b-9715-3d19bb11d8ac",
            "job_id": "0081b439-33b4-47e6-bc27-32de54ac4642",
            "job_name": "keypay_user_whitelabel",
            "s3_process_payload": {
                "s3_source_bucket": "eh-keypay",
                "s3_key": "UserWhitelabel_BiMonthly_20231114-173514.Zip",
                "s3_last_modified": "2023-11-14 17:57:11+00:00",
                "s3_size": 2398594,
                "is_production_s3_source": True,
            },
            "redshift_s3_process_payload": {
                "s3_key": "",
                "s3_bucket": "",
                "redshift_schema": "stg_keypay",
                "redshift_temp_schema": "temp_keypay",
                "redshift_table": "user_whitelabel",
                "redshift_table_schema": {},
            },
        }
        return event

    def test_load_lambda_keypay_payload(self, mock_event):
        event_payload = LambdaKeypayEventPayLoad(**mock_event)

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

        print()
        print(org_job_id)
        print(job_id)
        print(job_name)
        print(s3_process_payload)
        print(redshift_s3_process_payload)

    def test_handle_zip_file(self, mock_session):
        s3_client = AwsS3Client(session=mock_session)
        s3_object_info = s3_client.get_s3_object(
            bucket="eh-keypay",
            key="EmployeeSuperFund_BiMonthly_20231114-173514.Zip",
        )
        # Remove timezone info -> Pandas parse not error
        s3_object_info.last_modified = s3_object_info.last_modified.replace(tzinfo=None)
        print()
        (
            detail_uploaded_files_info,
            schema_info_list,
        ) = self.handle_zip_file_and_upload_one_by_one(
            s3_client=s3_client,
            s3_object_info=s3_object_info,
            s3_upload_prefix="keypay",
        )
        print(schema_info_list)

    def test_return_to_redshift_process(self):
        schema_info_list = [
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=71),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=71),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=4
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
            {
                "user_id": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=7),
                "whitelabel_id": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=3
                ),
                "is_default_parent": PolarsDataFrameColumnsInfo(
                    datatype=Utf8, max_len_byte=0
                ),
                "userid": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=0),
                "_file": PolarsDataFrameColumnsInfo(datatype=Utf8, max_len_byte=70),
                "_transaction_date": PolarsDataFrameColumnsInfo(
                    datatype=Date, max_len_byte=0
                ),
                "_etl_date": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
                "_modified": PolarsDataFrameColumnsInfo(
                    datatype=Datetime(time_unit="us", time_zone=None), max_len_byte=0
                ),
            },
        ]
        general_schema = get_max_value_of_multiple_schema(schema_info_list)
        redshift_schema = convert_polars_schema_info_to_redshift_datatype(
            general_schema
        )
        print()
        print(redshift_schema)
