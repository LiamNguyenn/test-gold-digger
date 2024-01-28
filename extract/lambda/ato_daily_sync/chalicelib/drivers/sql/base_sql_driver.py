"""BaseSqlDriver abstract class for further inherit of other sql driver."""
from abc import ABC, abstractmethod
from typing import Any

from attrs import define


@define
class BaseSqlDriver(ABC):
    """BaseSqlDriver abstract class for further inherit of other sql driver."""

    @abstractmethod
    def execute_query(self, query: str) -> list | None:
        """
        Abstract function for sql query for result.

        @param query: query string
        @return:
        """
        ...

    @abstractmethod
    def execute_query_raw(self, query: str) -> list[dict[str, Any]] | None:
        """
        Abstract function for sql query raw for result.

        @param query: query string
        @return:
        """
        ...

    @abstractmethod
    def get_table_schema(self, table: str, schema: str | None = None) -> list | None:
        """
        Abstract function for get table schema detail - name, datatype, etc.

        @param table: table to look for
        @param schema: schema of table to look for
        @return:
        """
        ...
