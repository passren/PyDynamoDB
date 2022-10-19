# -*- coding: utf-8 -*-
import logging
import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Any

from pydynamodb.model import ColumnInfo

from .dml_select import SupersetSelect
from ..converter import Converter
from ..model import Statements, Statement
from ..util import RetryConfig
from ..cursor import Cursor
from ..result_set import DynamoDBResultSet
from ..executor import BaseExecutor, DmlStatementExecutor

if TYPE_CHECKING:
    from ..connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class SupersetCursor(Cursor):
    def __init__(self, **kwargs) -> None:
        super(SupersetCursor, self).__init__(**kwargs)
        self._result_set_class = SupersetResultSet

    def _prepare_statement(self, operation: str) -> Statement:
        return Statement(operation, SupersetSelect)


class SupersetResultSet(DynamoDBResultSet):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        arraysize: int,
        retry_config: RetryConfig,
        is_transaction: bool = False,
        executor_class: BaseExecutor = None,
    ) -> None:
        super(SupersetResultSet, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            arraysize=arraysize,
            retry_config=retry_config,
            is_transaction=is_transaction,
            executor_class=SupersetStatementExecutor,
        )


class SupersetStatementExecutor(DmlStatementExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        self._temp_database = "SUPERSET_QUERY_%s" % datetime.now().strftime(
            "%Y%m%d%H%M%S%f"
        )
        self._temp_table = "TEMP_QUERY"
        super(DmlStatementExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def pre_execute(self) -> None:
        self._statement = self._statements[0]
        self._create_query_table()
        self.execute()

    def process_rows(self, response: Dict[str, Any]) -> None:
        super(SupersetStatementExecutor, self).process_rows(response)
        columns = ",".join('"' + c + '"' for c in self._metadata.keys())
        values = ",".join("?" for c in self._metadata.keys())
        sqlite_conn = sqlite3.connect(self._temp_database)
        try:
            data = [r for r in self._rows]
            sqlite_conn.executemany(
                "INSERT INTO %s (%s) VALUES (%s)" % (self._temp_table, columns, values),
                data,
            )
            sqlite_conn.commit()
        finally:
            sqlite_conn.close()

    def post_execute(self) -> None:
        self._metadata.clear()
        self._rows.clear()

        parser = self._statement.sql_parser.parser
        superset_sql = "SELECT %s FROM %s %s" % (
            ",".join(c for c in parser.outer_columns),
            self._temp_table,
            parser.outer_exprs,
        )
        sqlite_conn = sqlite3.connect(self._temp_database)
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute(superset_sql)
        self._rows.extend(sqlite_cursor.fetchall())
        for desc in sqlite_cursor.description:
            self._metadata.update(ColumnInfo(desc[0], desc[0]))

    def _create_query_table(self) -> None:
        self._process_predef_metadata(self._statement.sql_parser)
        columns = ",".join('"' + c + '" text' for c in self._metadata.keys())
        sqlite_conn = sqlite3.connect(self._temp_database)
        try:
            sqlite_conn.execute("CREATE TABLE %s (%s)" % (self._temp_table, columns))
        finally:
            sqlite_conn.close()
