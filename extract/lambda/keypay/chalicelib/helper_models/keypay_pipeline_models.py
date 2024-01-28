"""Helper models for keypay pipeline."""
from pydantic import BaseModel


class RedshiftS3ProcessPayload(BaseModel):
    """Payload for lambda event of getting processing S3 files to Redshift."""

    s3_key: str = ""
    s3_bucket: str = ""
    redshift_schema: str = ""
    redshift_temp_schema: str = ""
    redshift_table: str = ""
    redshift_table_schema: dict = {}


class S3ProcessPayload(BaseModel):
    """Payload for lambda event of getting processing S3 files."""

    s3_source_bucket: str = ""
    s3_key: str = ""
    s3_last_modified: str = ""
    s3_size: int = 0
    is_production_s3_source: bool = False


class LambdaKeypayEventPayLoad(BaseModel):
    """Payload for lambda event of getting processing files."""

    org_job_id: str = ""
    job_id: str = ""
    job_name: str = ""
    s3_process_payload: S3ProcessPayload = S3ProcessPayload()
    redshift_s3_process_payload: RedshiftS3ProcessPayload = RedshiftS3ProcessPayload()
