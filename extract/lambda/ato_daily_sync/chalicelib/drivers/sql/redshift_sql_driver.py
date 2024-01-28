"""Redshift SQL Driver base on BaseSqlDriver for easier handle redshift, base on boto3 redshift-data client."""
import logging
import time
from collections.abc import Mapping
from typing import Any

import boto3
from attrs import Factory, define, field

from ...models import RedshiftColumnInfo  # noqa: TID252
from .base_sql_driver import BaseSqlDriver

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s - %(asctime)s - %(message)s"
)

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
SYNC_LOGS_DEFAULT_TABLE_SCHEMA = {
    "job_id": "varchar(256)",
    "org_job_id": "varchar(256)",
    "job_name": "varchar(256)",
    "schema_name": "varchar(256)",
    "table_name": "varchar(256)",
    "start_time": "timestamp",
    "end_time": "timestamp",
    "rows_updated_or_inserted": "bigint",
    "status": "varchar(256)",
    "message": "varchar(4000)",
}


@define
class RedshiftSqlDriver(BaseSqlDriver):
    """Redshift SQL Driver base on BaseSqlDriver for easier handle redshift, base on boto3 redshift-data client."""

    database: str = field(kw_only=True)
    session: boto3.session = field(kw_only=True)
    cluster_identifier: str | None = field(default=None, kw_only=True)
    workgroup_name: str | None = field(default=None, kw_only=True)
    db_user: str | None = field(default=None, kw_only=True)
    database_credentials_secret_arn: str | None = field(default=None, kw_only=True)
    wait_for_query_completion_sec: float = field(default=0.3, kw_only=True)
    client: boto3.client = field(
        default=Factory(
            lambda self: self.session.client("redshift-data"), takes_self=True
        ),
        kw_only=True,
    )

    @workgroup_name.validator  # type: ignore[union-attr]
    def validate_params(
        self, _, workgroup_name: str | None  # noqa: ARG002
    ) -> None:  # force to have 3rd argument due to field validator
        """Use validator function of field decorator."""
        if not self.cluster_identifier and not self.workgroup_name:
            raise ValueError(  # noqa: TRY003
                "Provide a value for one of `cluster_identifier` or `workgroup_name`"
            )
        elif (  # noqa: RET506 - False positive
            self.cluster_identifier and self.workgroup_name
        ):
            raise ValueError(  # noqa: TRY003
                "Provide a value for either `cluster_identifier` or `workgroup_name`, but not both"
            )

    @classmethod
    def _process_rows_from_records(cls, records) -> list[list]:
        return [[c[next(iter(c.keys()))] for c in r] for r in records]

    @classmethod
    def _process_cells_from_rows_and_columns(
        cls, columns: list, rows: list[list]
    ) -> list[dict[str, Any]]:
        return [{column: r[idx] for idx, column in enumerate(columns)} for r in rows]

    @classmethod
    def _process_columns_from_column_metadata(cls, meta) -> list:
        return [k["name"] for k in meta]

    @classmethod
    def _post_process(cls, meta, records) -> list[dict[str, Any]]:
        columns = cls._process_columns_from_column_metadata(meta)
        rows = cls._process_rows_from_records(records)
        return cls._process_cells_from_rows_and_columns(columns, rows)

    def execute_query(self, query: str) -> list:
        """
        Handle query statement (SELECT) -> wait for results.

        @param query: sql query
        @return:
        """
        rows = self.execute_query_raw(query)
        if rows:
            return rows
        return []

    def execute_query_raw(self, query: str) -> list[dict[str, Any]]:
        """
        Handle query statement (SELECT) raw -> wait for results.

        @param query: sql query
        @return:
        """
        function_kwargs = {"Sql": query, "Database": self.database}
        if self.workgroup_name:
            function_kwargs["WorkgroupName"] = self.workgroup_name
        if self.cluster_identifier:
            function_kwargs["ClusterIdentifier"] = self.cluster_identifier
        if self.db_user:
            function_kwargs["DbUser"] = self.db_user
        if self.database_credentials_secret_arn:
            function_kwargs["SecretArn"] = self.database_credentials_secret_arn

        response = self.client.execute_statement(**function_kwargs)
        response_id = response["Id"]

        statement = self.client.describe_statement(Id=response_id)

        while statement["Status"] not in ["FINISHED", "ABORTED", "FAILED"]:
            time.sleep(self.wait_for_query_completion_sec)
            statement = self.client.describe_statement(Id=response_id)

        if statement["Status"] == "FINISHED":
            statement_result = self.client.get_statement_result(Id=response_id)
            results = statement_result.get("Records", [])

            while "NextToken" in statement_result:
                statement_result = self.client.get_statement_result(
                    Id=response_id, NextToken=statement_result["NextToken"]
                )
                results = results + response.get("Records", [])

            return self._post_process(statement_result["ColumnMetadata"], results)
        return []

    def get_table_schema(self, table: str, schema: str | None = None) -> list:
        """
        Return table schema detail - name, datatype, etc.

        @param table: table to look for
        @param schema: schema of table to look for
        @return:
        """
        function_kwargs = {"Database": self.database, "Table": table}
        if schema:
            function_kwargs["Schema"] = schema
        if self.workgroup_name:
            function_kwargs["WorkgroupName"] = self.workgroup_name
        if self.cluster_identifier:
            function_kwargs["ClusterIdentifier"] = self.cluster_identifier
        if self.db_user:
            function_kwargs["DbUser"] = self.db_user
        if self.database_credentials_secret_arn:
            function_kwargs["SecretArn"] = self.database_credentials_secret_arn
        response = self.client.describe_table(**function_kwargs)
        return response.get("ColumnList", [])

    def execute_statement_raw(self, query: str):
        """
        Handle execute statement (CREATE, DELETE, etc) raw -> wait for response.

        @param query: sql statement
        @return:
        """
        function_kwargs = {"Sql": query, "Database": self.database}
        if self.workgroup_name:
            function_kwargs["WorkgroupName"] = self.workgroup_name
        if self.cluster_identifier:
            function_kwargs["ClusterIdentifier"] = self.cluster_identifier
        if self.db_user:
            function_kwargs["DbUser"] = self.db_user
        if self.database_credentials_secret_arn:
            function_kwargs["SecretArn"] = self.database_credentials_secret_arn

        response = self.client.execute_statement(**function_kwargs)
        response_id = response["Id"]

        statement = self.client.describe_statement(Id=response_id)
        while statement["Status"] not in ["FINISHED", "ABORTED", "FAILED"]:
            time.sleep(self.wait_for_query_completion_sec)
            statement = self.client.describe_statement(Id=response_id)
        if statement["Status"] != "FINISHED":
            raise Exception(  # noqa: TRY002, TRY003
                f"execute statement error {statement}"
            )

    def create_table(
        self,
        table: str,
        schema: str | None,
        columns_map: Mapping,
        create_if_not_exist: bool = True,  # noqa: FBT001, FBT002
    ) -> None:
        """
        Use for create table in redshift.

        @param table: target table name
        @param schema: target table schema
        @param columns_map: usually dict[str, str] with key is column name and value is data type (of redshift)
        @param create_if_not_exist:
        @return:
        """
        columns_part: list[str] = [
            f'"{column_name}" {data_type}'
            for column_name, data_type in columns_map.items()
        ]
        query = f"""
            CREATE TABLE {'IF NOT EXISTS' if create_if_not_exist else ''} "{self.database}"."{schema}"."{table}" (
                {",".join(columns_part)}
                );
            """
        self.execute_statement_raw(query=query)
        logging.info(f"CREATE TABLE COMPLETE, NAME: {schema}.{table}")

    def drop_table(self, table: str, schema: str):
        """
        Drop table redshift.

        :param table: table name
        :param schema: schema of that table
        :return:
        """
        drop = f"""
            DROP TABLE IF EXISTS "{self.database}"."{schema}"."{table}";
        """
        self.execute_statement_raw(query=drop)
        logging.info(f"DROP TABLE COMPLETE, NAME: {schema}.{table}")

    def copy_from_s3_manifest(
        self,
        table: str,
        schema: str,
        columns: list,
        s3_path: str,
        iam_role: str,
        region: str,
    ) -> None:
        """
        Use for copy data from manifest rule file to target table.

        @param table: target table name
        @param schema: target table schema
        @param columns: columns list to insert
        @param s3_path: s3 path of manifest file
        @param iam_role: AWS's iam role if needed for cross account
        @param region: aws region of s3 file
        @return:
        """
        sql_copy_from_s3 = f"""
            COPY "{self.database}"."{schema}"."{table}" ({', '.join([f'"{column}"' for column in columns])})
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            REGION '{region}'
            FORMAT AS CSV GZIP
            MANIFEST
            IGNOREHEADER 1
            TIMEFORMAT 'auto'
            DATEFORMAT 'auto'
            TRUNCATECOLUMNS
            ACCEPTINVCHARS
        """
        self.execute_statement_raw(query=sql_copy_from_s3)
        logging.info(
            f'COPY DATA TO TABLE "{self.database}"."{schema}"."{table}" COMPLETE'
        )

    def copy_from_s3_csv_gz(
        self,
        table: str,
        schema: str,
        columns: list,
        s3_path: str,
        iam_role: str,
        region: str,
    ) -> None:
        """
        Use for copy data from csv gz file to target table.

        @param table: target table name
        @param schema: target table schema
        @param columns: columns list to insert
        @param s3_path: s3 path of manifest file
        @param iam_role: AWS's iam role if needed for cross account
        @param region: aws region of s3 file
        @return:
        """
        sql_copy_from_s3 = f"""
            COPY "{self.database}"."{schema}"."{table}" ({', '.join([f'"{column}"' for column in columns])})
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            REGION '{region}'
            FORMAT AS CSV GZIP
            IGNOREHEADER 1
            TIMEFORMAT 'auto'
            DATEFORMAT 'auto'
            TRUNCATECOLUMNS
            ACCEPTINVCHARS
        """
        self.execute_statement_raw(query=sql_copy_from_s3)
        logging.info(
            f'COPY DATA TO TABLE "{self.database}"."{schema}"."{table}" COMPLETE'
        )

    def copy_from_s3_to_temp_table(
        self,
        temp_table_name: str,
        temp_schema_name: str,
        schema: Mapping,
        s3_path: str,
        iam_role: str,
        region: str,
        is_from_manifest: bool = False,  # noqa: FBT001, FBT002
    ):
        """
        Copy from S3 to temp table.

        @param temp_table_name:
        @param temp_schema_name:
        @param schema:
        @param s3_path:
        @param iam_role:
        @param region:
        @param is_from_manifest:
        @return:
        """
        self.create_table(
            table=temp_table_name,
            schema=temp_schema_name,
            columns_map=schema,
            create_if_not_exist=False,
        )
        columns = list(schema.keys())
        if is_from_manifest:
            self.copy_from_s3_manifest(
                table=temp_table_name,
                schema=temp_schema_name,
                columns=columns,
                s3_path=s3_path,
                iam_role=iam_role,
                region=region,
            )
        else:
            self.copy_from_s3_csv_gz(
                table=temp_table_name,
                schema=temp_schema_name,
                columns=columns,
                s3_path=s3_path,
                iam_role=iam_role,
                region=region,
            )
        logging.info(
            f"COPY DATA TO TEMP TABLE {temp_schema_name}.{temp_table_name} COMPLETE"
        )

    def insert(self, table: str, schema: str, columns: list, values: list):
        """
        Insert a record to table.

        :param table:
        :param schema:
        :param columns:
        :param values:
        :return:
        """
        sql_insert_into = f"""
                INSERT INTO "{self.database}"."{schema}"."{table}"
                ({', '.join([f'"{column}"' for column in columns])})
                VALUES ({', '.join([f"'{value}'" for value in values])});
            """  # noqa: S608
        self.execute_statement_raw(query=sql_insert_into)

    def insert_log(
        self,
        sync_log_table_name: str = "sync_logs",
        sync_log_schema_name: str = "public",
        sync_log_schema: dict = SYNC_LOGS_DEFAULT_TABLE_SCHEMA,
        **kwargs,
    ):
        """
        Insert logs into sync_logs table.

        @param sync_log_table_name:
        @param sync_log_schema_name:
        @param sync_log_schema:
        @param kwargs:
        @return:
        """
        self.create_table(
            table=sync_log_table_name,
            schema=sync_log_schema_name,
            columns_map=sync_log_schema,
            create_if_not_exist=True,
        )
        schema_columns = set(sync_log_schema.keys())
        user_input_columns = set(kwargs.keys())
        if not schema_columns == user_input_columns:
            raise ValueError(  # noqa: TRY003
                "User columns not match with table schema columns"
            )
        self.insert(
            table=sync_log_table_name,
            schema=sync_log_schema_name,
            columns=list(kwargs.keys()),
            values=list(kwargs.values()),
        )
        logging.info(
            f"INSERT LOG TO {sync_log_schema_name}.{sync_log_table_name} COMPLETE"
        )

    def construct_alter_table(
        self,
        table: str,
        schema: str,
        column_name: str,
        new_data_type: str,
        new_length: int = 0,
    ):
        """
        Create alter table command for new datatype.

        :param table: table name
        :param schema: schema name
        :param column_name: column name to change data type
        :param new_data_type: new data type
        :param new_length: new length datatype if needed
        :return:
        """
        new_column_name = f"__temp_{column_name}__"
        if new_data_type.lower() == "varchar":
            sql = f'ALTER TABLE {self.database}.{schema}.{table} ALTER COLUMN "{column_name}" TYPE {new_data_type}({new_length});'
        else:
            sql = f"""
                ALTER TABLE {self.database}.{schema}.{table} ADD COLUMN "{new_column_name}" {new_data_type};
                UPDATE {self.database}.{schema}.{table} SET "{new_column_name}" = "{column_name}";
                ALTER TABLE {self.database}.{schema}.{table} DROP COLUMN "{column_name}";
                ALTER TABLE {self.database}.{schema}.{table} RENAME COLUMN "{new_column_name}" TO "{column_name}";
            """  # noqa: S608
        return sql

    def construct_add_column_to_table(
        self, table: str, schema: str, column_name: str, data_type: str, length: int
    ):
        """
        Construct add new column to table.

        :param table_name: table name include database, schema, table eg: dev.stg_checkly.all_checks
        :param column_name: column name to change data type
        :param data_type: new data type
        :param length: new length datatype if needed
        :return:
        """
        if data_type.lower() == "varchar":
            return f'ALTER TABLE {self.database}.{schema}.{table} ADD COLUMN "{column_name}" {data_type}({length});'
        return f'ALTER TABLE {self.database}.{schema}.{table} ADD COLUMN "{column_name}" {data_type};'

    def update_sink_table_structure_from_staging_table(
        self, sink_table: str, sink_schema: str, staging_table: str, staging_schema: str
    ):
        """
        Update sink table structure compare with staging(temp) table if needed.

        :param sink_table:
        :param sink_schema:
        :param staging_table:
        :param staging_schema:
        :return:
        """
        sink_table_column_list = self.get_table_schema(
            table=sink_table, schema=sink_schema
        )
        staging_table_column_list = self.get_table_schema(
            table=staging_table, schema=staging_schema
        )

        sink_columns_details: dict[str, RedshiftColumnInfo] = {
            column_info.get("name"): RedshiftColumnInfo(**column_info)
            for column_info in sink_table_column_list
        }
        staging_columns_details: dict[str, RedshiftColumnInfo] = {
            column_info.get("name"): RedshiftColumnInfo(**column_info)
            for column_info in staging_table_column_list
        }
        alter_statement_list = []
        for column_name, column_info in staging_columns_details.items():
            if column_name in sink_columns_details:
                sink_column_detail: RedshiftColumnInfo = sink_columns_details.get(
                    column_name, column_info
                )
                if column_info.length > sink_column_detail.length:
                    sql = self.construct_alter_table(
                        table=sink_table,
                        schema=sink_schema,
                        column_name=column_name,
                        new_data_type=column_info.typeName,
                        new_length=column_info.length,
                    )
                    alter_statement_list.append(sql)
            else:
                sql = self.construct_add_column_to_table(
                    table=sink_table,
                    schema=sink_schema,
                    column_name=column_name,
                    data_type=column_info.typeName,
                    length=column_info.length,
                )
                alter_statement_list.append(sql)
        if alter_statement_list:
            self.execute_statement_raw(" ".join(alter_statement_list))
        logging.info(
            f"Update structure from staging table: {staging_schema}.{staging_table}, primary table: {sink_schema}.{sink_table} COMPLETE"
        )

    def delete_insert(
        self,
        primary_table: str,
        primary_schema: str,
        staging_table: str,
        staging_schema: str,
        keys: list,
        columns: list,
    ) -> None:
        """
        Use for insert + delete from temp table to final table.

        @param primary_table: target table name
        @param primary_schema: target table schema
        @param staging_table: temp table name
        @param staging_schema: temp table schema
        @param keys: keys for identify duplication for delete
        @param columns: columns list
        @return:
        """
        temp_select_name = "_datateam_staging_"
        sql_delete_from_staging = f"""
                DELETE FROM "{self.database}"."{primary_schema}"."{primary_table}"
                USING (
                    SELECT {', '.join([f'"{key}"' for key in keys])}
                    FROM "{self.database}"."{staging_schema}"."{staging_table}"
                ) AS {temp_select_name}
                WHERE {' AND '.join([f'"{primary_table}"."{key}" = "{temp_select_name}"."{key}"' for key in keys])};
            """  # noqa: S608
        self.execute_statement_raw(query=sql_delete_from_staging)
        sql_insert_into_from_staging = f"""
                INSERT INTO "{self.database}"."{primary_schema}"."{primary_table}"
                ({', '.join([f'"{column}"' for column in columns])})
                SELECT {', '.join([f'"{column}"' for column in columns])}
                FROM "{self.database}"."{staging_schema}"."{staging_table}"
            """  # noqa: S608
        self.execute_statement_raw(query=sql_insert_into_from_staging)
        logging.info(
            f"Delete insert from staging table: {staging_schema}.{staging_table}, primary table: {primary_schema}.{primary_table}"
        )
