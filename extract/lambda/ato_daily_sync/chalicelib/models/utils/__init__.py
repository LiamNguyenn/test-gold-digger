import polars as pl
from pydantic import BaseModel, ConfigDict


class PolarsDataFrameColumnsInfo(BaseModel):
    """Using for polar columns info consistency."""

    datatype: pl.datatypes.DataType | pl.datatypes.DataTypeClass
    max_len_byte: int
    model_config = ConfigDict(arbitrary_types_allowed=True)
