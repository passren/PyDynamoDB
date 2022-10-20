# -*- coding: utf-8 -*-
import logging
import sqlite3
from typing import TYPE_CHECKING, Dict, Any, List, Tuple, Optional

from .dml_select import SupersetSelect
from ..sql.common import DataTypes
from ..converter import Converter
from ..model import Statements, Statement, ColumnInfo, Metadata
from ..util import RetryConfig
from ..cursor import Cursor, synchronized
from ..result_set import DynamoDBResultSet
from ..executor import BaseExecutor, DmlStatementExecutor
from ..error import OperationalError

if TYPE_CHECKING:
    from ..connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class SupersetCursor(Cursor):
    def __init__(self, **kwargs) -> None:
        super(SupersetCursor, self).__init__(**kwargs)
        self._result_set_class = SupersetResultSet

    @synchronized
    def execute(
        self: Cursor, operation: str, parameters: Optional[List[Dict[str, Any]]] = None
    ) -> Cursor:
        statement = Statement(operation, SupersetSelect)
        return self.execute_statement(statement, parameters)


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
        self._superset_table = "SUPERSET_QUERY"
        self._sqlite_conn = None
        super(DmlStatementExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    @property
    def sqlite_conn(self):
        if self._sqlite_conn is not None:
            return self._sqlite_conn

        self._sqlite_conn = sqlite3.connect(":memory:")
        return self._sqlite_conn

    def pre_execute(self) -> None:
        self._statement = self._statements[0]
        self.execute()

    def execute(self, **kwargs) -> None:
        try:
            parser = self._statement.sql_parser.parser
            if parser.is_nested:
                with self.connection.cursor() as cursor:
                    cursor.result_set_class = DynamoDBResultSet
                    cursor.execute_statement(self._statement)
                    self._load_into_memory_db(cursor.result_set)
            else:
                super(SupersetStatementExecutor, self).execute(**kwargs)
        except Exception as e:
            _logger.exception("Failed to execute statement.")
            raise OperationalError(*e.args) from e
        finally:
            self.sqlite_conn.close()

    def _load_into_memory_db(self, ddb_result_set: DynamoDBResultSet) -> None:
        raw_data = ddb_result_set.fetchall()

        if len(raw_data) > 0:
            self._create_query_table(ddb_result_set.metadata)
        self._write_raw_data(ddb_result_set.metadata, raw_data)

        parser = self._statement.sql_parser.parser
        superset_sql = "SELECT %s FROM %s %s" % (
            parser.outer_columns,
            self._superset_table,
            parser.outer_exprs,
        )
        sqlite_cursor = self.sqlite_conn.cursor()
        sqlite_cursor.execute(superset_sql)
        self._rows.extend(sqlite_cursor.fetchall())
        for desc in sqlite_cursor.description:
            self._metadata.update(ColumnInfo(desc[0], desc[0]))

    def _create_query_table(self, metadata: Metadata) -> None:
        columns = list()
        for col_info in metadata:
            col_name = col_info.alias if col_info.alias is not None else col_info.name
            col_type = "TEXT"
            if (
                col_info.type_code == DataTypes.BOOL
                or col_info.type_code == DataTypes.NULL
            ):
                col_type = "INTEGER"
            elif col_info.type_code == DataTypes.NUMBER:
                col_type = "REAL"

            columns.append('"%s" %s' % (col_name, col_type))

        self.sqlite_conn.execute(
            "CREATE TABLE %s (%s)" % (self._superset_table, ",".join(columns))
        )

    def _write_raw_data(
        self, metadata: Metadata, raw_data: Optional[List[Tuple[Any]]]
    ) -> None:
        columns = ",".join('"' + c + '"' for c in metadata.keys())
        values = ",".join("?" for c in metadata.keys())

        self.sqlite_conn.executemany(
            "INSERT INTO %s (%s) VALUES (%s)" % (self._superset_table, columns, values),
            [r for r in raw_data],
        )
        self.sqlite_conn.commit()
