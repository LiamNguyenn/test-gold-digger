"""Config models for Keypay pipelines."""
from pydantic import BaseModel


class KeypayConfig(BaseModel):
    """Keypay Config for all pipelines."""

    job_name: str = ""
    s3_source_bucket: str = ""
    is_production_s3_source_bucket: bool = False
    s3_prefix: str = ""
    s3_file_pattern: str = ""
    redshift_schema: str = ""
    redshift_temp_schema: str = ""
    redshift_table: str = ""
