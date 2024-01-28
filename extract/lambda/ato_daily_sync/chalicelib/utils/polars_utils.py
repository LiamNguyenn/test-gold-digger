"""Polars utils for multiple purpose."""
import polars as pl
from polars import col

from ..models import PolarsDataFrameColumnsInfo  # noqa: TID252

MIN_LENGTH = 20
SMALL_LENGTH = 256
MEDIUM_LENGTH = 1000
LONG_LENGTH = 4000
LONG_LONG_LENGTH = 10000
MAX_LENGTH = 65535
DATA_TYPE_MAP = {
    "DECIMAL": "DECIMAL",
    "FLOAT32": "DOUBLE PRECISION",
    "FLOAT64": "DOUBLE PRECISION",
    "INT8": "BIGINT",
    "INT16": "BIGINT",
    "INT32": "BIGINT",
    "INT64": "BIGINT",
    "UTF8": "VARCHAR",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
    "DURATION": "TIME",
    "BOOLEAN": "BOOLEAN",
}


def get_polars_df_schema_info(
    df: pl.DataFrame,
) -> dict[str, PolarsDataFrameColumnsInfo]:
    """
    Get max possible length of each column and return dict of columns info.

    :param df: Dataframe to get info from
    :return:
    """
    return_columns_info: dict[str, PolarsDataFrameColumnsInfo] = {}
    for column_name, column_type in df.schema.items():
        length = -1
        if column_type == pl.Utf8:
            length = df[column_name].str.lengths().max()
        return_columns_info[column_name] = PolarsDataFrameColumnsInfo(
            datatype=column_type, max_len_byte=length
        )
    return return_columns_info


def get_max_value_of_multiple_schema(
    schema_list: list[dict[str, PolarsDataFrameColumnsInfo]]
) -> dict[str, PolarsDataFrameColumnsInfo] | None:
    """
    Compare list of dicts and construct most complete possible schema.

    @param schema_list: List of dict need to compare
    @return: bool for is valid or not
    """
    if schema_list:
        current_max: dict[str, PolarsDataFrameColumnsInfo] = schema_list[0]
        for schema in schema_list[1:]:
            for column_name, column_info in schema.items():
                if column_name not in current_max:
                    current_max[column_name] = column_info
                elif column_info.datatype != current_max[column_name].datatype:
                    current_max[column_name].datatype = pl.Utf8
                    current_max[column_name].max_len_byte = max(
                        current_max[column_name].max_len_byte, column_info.max_len_byte
                    )
                elif column_info.max_len_byte > current_max[column_name].max_len_byte:
                    current_max[column_name].max_len_byte = column_info.max_len_byte
        return current_max
    return None


def polars_redshift_datatype_map(
    datatype: pl.datatypes.DataTypeClass, length: int
) -> str:
    """
    Use for map datatype of polars to redshift.

    @param datatype: polars str datatype
    @param length: polars length for varchar2
    @return:
    """
    if datatype == pl.Utf8:
        if length < 0:
            new_length = MIN_LENGTH
        elif length <= SMALL_LENGTH:
            new_length = SMALL_LENGTH
        elif length <= MEDIUM_LENGTH:
            new_length = MEDIUM_LENGTH
        elif length <= LONG_LENGTH:
            new_length = LONG_LENGTH
        elif length <= LONG_LONG_LENGTH:
            new_length = LONG_LONG_LENGTH
        else:
            new_length = MAX_LENGTH
        return f"{DATA_TYPE_MAP.get('UTF8')}({new_length})"
    if datatype == pl.Datetime:
        return DATA_TYPE_MAP.get("DATETIME", "VARCHAR(256)")
    if datatype == pl.Date:
        return DATA_TYPE_MAP.get("DATE", "VARCHAR(256)")
    return DATA_TYPE_MAP.get(str(datatype).upper(), "VARCHAR(256)")


def convert_polars_schema_info_to_redshift_datatype(
    schema: dict[str, PolarsDataFrameColumnsInfo]
) -> dict[str, str]:
    """Convert polars schema info to redshift column:type."""
    return {
        k: polars_redshift_datatype_map(datatype=v.datatype, length=v.max_len_byte)
        for k, v in schema.items()
    }


def read_fwf_to_dataframe(filename, widths, columns, dtypes) -> pl.DataFrame:
    """
    Read text file and split by spaces, with of that col.

    @param filename:
    @param widths:
    @param columns:
    @param dtypes:
    @return:
    """
    column_information = [
        (*x, y, z) for x, y, z in zip(widths, columns, dtypes, strict=True)
    ]

    return pl.read_csv(
        filename, separator="\n", new_columns=["header"], has_header=False, skip_rows=2
    ).select(
        col("header").str.slice(col_offset, col_len).cast(col_type).alias(col_name)
        for col_offset, col_len, col_name, col_type in column_information
    )
