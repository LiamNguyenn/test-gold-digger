"""Model for Redshift Driver."""
from pydantic import BaseModel


class RedshiftColumnInfo(BaseModel):
    """Model for redshift column info when using boto3 describe table."""

    isCaseSensitive: bool  # noqa: N815
    isCurrency: bool  # noqa: N815
    isSigned: bool  # noqa: N815
    length: int
    name: str
    nullable: int
    precision: int
    scale: int
    schemaName: str  # noqa: N815
    tableName: str  # noqa: N815
    typeName: str  # noqa: N815
