# type: ignore
# ruff: noqa
import boto3
import pytest
from eh_lambda_utils.drivers import RedshiftSqlDriver
from eh_lambda_utils.models import RedshiftColumnInfo


class TestProcessS3ToRedshift:
    REDSHIFT_CLUSTER_NAME = "ehdw"
    REDSHIFT_DATABASE = "dev"
    REDSHIFT_USER = "dbt_cloud"
    database = REDSHIFT_DATABASE

    @pytest.fixture()
    def mock_session_prod(self):
        session = boto3.Session(profile_name="979797940137_DataTeamDeveloper")
        return session

    @pytest.fixture()
    def mock_session_analytics(self):
        session = boto3.Session(profile_name="979797940137_DataTeamDeveloper")
        return session

    def construct_alter_table(
        self,
        table: str,
        schema: str,
        column_name: str,
        old_data_type: str,
        new_data_type: str,
        new_length: int = 0,
    ):
        """
        Create alter table command for new datatype.

        :param table: table name
        :param schema: schema name
        :param column_name: column name to change data type
        :param old_data_type: old data type
        :param new_data_type: new data type
        :param new_length: new length datatype if needed
        :return:
        """
        new_column_name = f"__temp_{column_name}__"
        if new_data_type.lower() == old_data_type.lower() == "varchar":
            sql = f'ALTER TABLE {self.database}.{schema}.{table} ALTER COLUMN "{column_name}" TYPE {new_data_type}({new_length});'
        elif old_data_type.lower() == "varchar" and new_data_type.lower() == "bool":
            sql = f"""
                ALTER TABLE {self.database}.{schema}.{table} ADD COLUMN "{new_column_name}" {new_data_type};
                UPDATE {self.database}.{schema}.{table} SET "{new_column_name}" = "{column_name}"::bool;
                ALTER TABLE {self.database}.{schema}.{table} DROP COLUMN "{column_name}";
                ALTER TABLE {self.database}.{schema}.{table} RENAME COLUMN "{new_column_name}" TO "{column_name}";
            """
        elif old_data_type.lower() == "bool" and new_data_type.lower() == "varchar":
            sql = f"""
                ALTER TABLE {self.database}.{schema}.{table} ADD COLUMN "{new_column_name}" {new_data_type}({new_length});
                UPDATE {self.database}.{schema}.{table} SET "{new_column_name}" = decode("{column_name}", true, 'true', false, 'false');
                ALTER TABLE {self.database}.{schema}.{table} DROP COLUMN "{column_name}";
                ALTER TABLE {self.database}.{schema}.{table} RENAME COLUMN "{new_column_name}" TO "{column_name}";
            """
        else:
            sql = f"""
                ALTER TABLE {self.database}.{schema}.{table} ADD COLUMN "{new_column_name}" {new_data_type};
                UPDATE {self.database}.{schema}.{table} SET "{new_column_name}" = "{column_name}";
                ALTER TABLE {self.database}.{schema}.{table} DROP COLUMN "{column_name}";
                ALTER TABLE {self.database}.{schema}.{table} RENAME COLUMN "{new_column_name}" TO "{column_name}";
            """  # noqa: S608
        return sql

    def test_schema_merge(self, mock_session_prod):
        redshift_driver = RedshiftSqlDriver(
            session=mock_session_prod,
            cluster_identifier=self.REDSHIFT_CLUSTER_NAME,
            database=self.REDSHIFT_DATABASE,
            db_user=self.REDSHIFT_USER,
        )
        print()
        print("--------------------------")
        sink_table = "employee_super_fund"
        sink_schema = "stg_keypay"
        staging_table = "temp_employee_super_fund_670cd8bc-83c9-48bb-9da1-4f73394c3e80"
        staging_schema = "temp_keypay"
        staging_table_column_list = redshift_driver.get_table_schema(
            table=staging_table, schema=staging_schema
        )
        sink_table_column_list = redshift_driver.get_table_schema(
            table=sink_table, schema=sink_schema
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
                sink_column_detail = sink_columns_details[column_name]
                if column_info.length > sink_column_detail.length:
                    sql = self.construct_alter_table(
                        table=sink_table,
                        schema=sink_schema,
                        column_name=column_name,
                        old_data_type=sink_column_detail.typeName,
                        new_data_type=column_info.typeName,
                        new_length=column_info.length,
                    )
                    alter_statement_list.append(sql)
            else:
                sql = redshift_driver.construct_add_column_to_table(
                    table=sink_table,
                    schema=sink_schema,
                    column_name=column_name,
                    data_type=column_info.typeName,
                    length=column_info.length,
                )
                alter_statement_list.append(sql)
        print(alter_statement_list)
        print("--------------------------")
