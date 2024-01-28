#!/usr/bin/env python3
"""Clone DBT models from Prod to Local."""
import argparse
import json
import logging
import typing
from os import environ as env

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import ProgrammingError

if typing.TYPE_CHECKING:
    from sqlalchemy.engine import Engine

DBT_DATA_ANALYTICS_ROLE = "dbt_data_analytics"


# config logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DbtModelClone:
    """Clone DBT models from Prod to Local."""

    def __init__(self, config_vars: dict):
        self.prod_database = config_vars["DBT_DATABASE"]
        self.dbt_user = config_vars["DBT_USER"]
        self.engine = self.create_sqlalchemy_engine(config_vars)

    def create_sqlalchemy_engine(self, config_vars: dict) -> "Engine":
        """Create a SQLAlchemy engine for the given config vars."""
        if "DBT_DATABASE" not in config_vars:
            raise ValueError(  # noqa: TRY003
                "DBT_DATABASE is not set in the environment"
            )

        if "DBT_REDSHIFT_HOST" not in config_vars:
            raise ValueError(  # noqa: TRY003
                "DBT_REDSHIFT_HOST is not set in the environment"
            )
        url = URL.create(
            drivername="redshift+redshift_connector",
            host=config_vars["DBT_REDSHIFT_HOST"],  # Amazon Redshift cluster endpoint
            database=config_vars["DBT_DATABASE"],  # Amazon Redshift database
            username=config_vars["DBT_USER"],
            password=config_vars["DBT_PASSWORD"],
            port=5439,
        )

        conn_params = {
            "ssl_insecure": True,  # ensures certificate verification occurs for idp_host
        }

        return typing.cast("Engine", create_engine(url, connect_args=conn_params))

    def query_executor(self, *queries: str) -> typing.Any:
        """Execute a query on the engine."""
        result = None
        with self.engine.begin() as conn:
            for q in queries:
                result = conn.execute(text(q))
        return result

    def create_schema(self, schema_name: str):  # noqa: D102
        query = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
        self.query_executor(query)
        return True

    def clean_dbt_input(self, model_input: list[str]) -> list[dict[str, str]]:
        """Clean the input from dbt to be used in the dependency resolver."""
        joined = " ".join(model_input)

        # Handle multiple dependencies.
        delimeter = '{"database"'

        # Clone the models that current model_input depends on.
        input_list = [delimeter + x for x in joined.split(delimeter) if x]

        list_of_dicts: list[dict] = []
        for i in input_list:
            loaded_dict = json.loads(i)
            actual_dependencies = [
                n for n in loaded_dict.get("depends_on").get("nodes") if "seed" not in n
            ]
            loaded_dict["actual_dependencies"] = actual_dependencies
            list_of_dicts.append(loaded_dict)

        sorted_output = [
            {"id": i.get("unique_id"), "dependencies": i.get("actual_dependencies")}
            for i in list_of_dicts
        ]

        dep_resolver = DependencyResolver()
        resolved_dependencies = dep_resolver.simple_resolution(sorted_output)
        sorted_list = []

        for resolved_dependency in resolved_dependencies:
            for d in list_of_dicts:
                if d.get("unique_id") == resolved_dependency.name:
                    sorted_list.append(d)  # noqa: PERF401

        return sorted_list

    def get_prod_schema(self, schema_name):
        """Get the schema from prod."""
        return schema_name.replace(f"{self.dbt_user}__", "")

    def clone_dbt_models(self, model_input: list[str]):
        """Clone dbt models."""
        sorted_list = self.clean_dbt_input(model_input)
        for i in sorted_list:
            database_name = i["database"]
            schema_name = i["schema"]
            table_name = i["name"]
            alias = i["alias"]
            logger.info(
                f"Processing schema: db={database_name}, schema={schema_name}, table={table_name}"
            )

            if "dev" in database_name:
                database_name = "dev"

            if alias:
                table_name = alias

            # Get prod schema, eg if local schema is <user_name>__<schema_name> then produ would be <schema_name>
            prod_schema = self.get_prod_schema(schema_name)

            full_name = f""""{database_name}"."{prod_schema}"."{table_name}" """.strip()
            output_schema_name = f"{self.dbt_user}__{prod_schema}"
            output_table_name = (
                f""""{database_name}"."{output_schema_name}"."{table_name}" """.strip()
            )

            query = f"""
            SELECT
                table_type
            FROM
                svv_tables
            WHERE
                "table_name" = '{table_name}'
                AND table_schema = '{prod_schema}'
                AND table_catalog = '{database_name}'
            """
            res = self.query_executor(query)
            try:
                table_or_view = res.fetchall()[0][0]
            except IndexError:
                logger.warning(
                    f"Table/view {output_table_name} does not exist in PROD yet and must be created with "
                    f"regular dbt"
                )
                continue

            self.create_schema(output_schema_name)

            if table_or_view == "VIEW":
                query = f"""CREATE OR REPLACE VIEW {output_table_name} AS SELECT * FROM {full_name}  WITH NO SCHEMA BINDING;"""  # noqa: S608
                self.query_executor(query)
                continue

            try:
                drop_statement = f"""DROP TABLE IF EXISTS {output_table_name};"""
                clone_table_definition = f"CREATE VIEW {output_table_name} AS SELECT * FROM {full_name} WITH NO SCHEMA BINDING;"  # noqa: S608
                self.query_executor(drop_statement, clone_table_definition)
                logger.info(f"Table {output_table_name} is being cloned")
            except ProgrammingError as error:
                logger.warning(f"Problem processing {output_table_name}")
                logger.warning(str(error))
                continue


class Node:
    """A node in graph."""

    def __init__(self, name):
        self.name = name
        self.edges = []
        self.edge_names = []

    def add_edge(self, node):  # noqa: D102
        self.edges.append(node)

    def add_edge_name(self, node):  # noqa: D102
        self.edge_names.append(node)

    def to_string(self):  # noqa: D102
        return f"{self.name} -> {[edge.name for edge in self.edges]}"


class CircularReferenceError(Exception):
    """Circular reference exception."""

    def __init__(self, node, edge):
        super().__init__(f"Circular reference detected: {node.name} -> {edge.name}")


class DependencyResolver:
    """A dependency resolver."""

    def __init__(self):
        self.node_list = []
        self.resolved_list = []

    def process_dependency_list(self, dependency_list):
        """Add nodes and edges to the dependency graph."""
        for nod in dependency_list:
            n = Node(nod.get("id"))

            for dependency in nod.get("dependencies"):
                n.add_edge_name(dependency)

            self.node_list.append(n)

        for node in self.node_list:
            for name in node.edge_names:
                dep_node = self.get_node(name)
                if dep_node:
                    node.add_edge(dep_node)
                else:
                    node.add_edge(Node(name))

    def get_node(self, node_name):
        """Get a node by name."""
        for node in self.node_list:
            if node.name == node_name:
                return node
        return None

    def dep_resolve(self, node: Node, resolved, unresolved):
        """Resolve dependencies."""
        unresolved.append(node)
        for edge in node.edges:
            if edge not in resolved:
                if edge in unresolved:
                    raise CircularReferenceError(node, edge)
                self.dep_resolve(edge, resolved, unresolved)
        resolved.append(node)
        unresolved.remove(node)

    def simple_resolution(self, dependency_list):
        """Resolve dependencies."""
        """
        format [{"id": 1, "dependencies": [1,2,3]}]
        """
        self.process_dependency_list(dependency_list)
        resolved = []
        for node in self.node_list:
            if node.name not in [r.name for r in resolved]:
                self.dep_resolve(node, resolved, [])

        clean_resolved = [
            r for r in resolved if r.name in [s.name for s in self.node_list]
        ]

        return clean_resolved


if __name__ == "__main__":
    """Usage: python clone_dbt_models.py <json manifest>"""
    parser = argparse.ArgumentParser()
    parser.add_argument("INPUT", nargs="+")
    args = parser.parse_args()

    cloner = DbtModelClone(env.copy())
    cloner.clone_dbt_models(args.INPUT)
