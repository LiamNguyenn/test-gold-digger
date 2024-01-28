"""Contain models for Checkly API."""
from enum import Enum

from pydantic import BaseModel


class CheckType(str, Enum):
    """Checkly check results api's check type."""

    API = "API"
    BROWSER = "BROWSER"
    HEARTBEAT = "HEARTBEAT"
    MULTI_STEP = "MULTI_STEP"


class ResultType(str, Enum):
    """Checkly check results api's result type."""

    ALL = "ALL"
    FINAL = "FINAL"
    ATTEMPT = "ATTEMPT"


class PayLoadListAllCheckResult(BaseModel):
    """Payload for all checks results api."""

    limit: int = 10
    page: int = 1
    from_date: float | None = None
    to_date: float | None = None
    location: str | None = None
    checkType: CheckType | None = None  # noqa: N815
    hasFailures: bool | None = True  # noqa: N815
    resultType: ResultType | None = None  # noqa: N815

    class Config:
        """config class of payload, by default is False, its enable using enum."""

        use_enum_values = True  # <--


class PayLoadListAllChecks(BaseModel):
    """Payload for list all checks api."""

    limit: int = 10
    page: int = 1


class LambdaEventScanAllChecks(BaseModel):
    """Payload for lambda event of scan all checks."""

    job_name: str = ""
    chunk_size: int = 10
    redshift_schema: str = ""
    redshift_temp_schema: str = ""
    redshift_table: str = ""


class LambdaEventPayLoadListAllCheckResult(BaseModel):
    """Payload for lambda event of getting check results."""

    org_job_id: str = ""
    job_id: str = ""
    job_name: str = ""
    redshift_schema: str = ""
    redshift_temp_schema: str = ""
    redshift_table: str = ""
    check_id: str = ""
    payload: PayLoadListAllCheckResult = PayLoadListAllCheckResult()
