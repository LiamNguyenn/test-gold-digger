"""S3 Models for upload, response, etc."""
from pydantic import BaseModel


class S3UploadResponse(BaseModel):
    """S3 response form when upload process is success."""

    file_name: str = ""
    row_count: int = 0
    s3_destination_bucket: str
    s3_destination_key: str
