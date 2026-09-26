# -*- coding: utf-8 -*-
import json
import logging
from typing import TYPE_CHECKING, Dict, Any, List, Optional

from .helper import QueryDBHelper
from .querydb import QueryDB
from .dml_select import SupersetSelect
from ..converter import Converter
from ..model import Statements, Statement, ColumnInfo
from ..util import RetryConfig, synchronized
from ..cursor import Cursor
from ..result_set import DynamoDBResultSet
from ..executor import BaseExecutor, DmlStatementExecutor
from ..error import NotSupportedError, OperationalError

if TYPE_CHECKING:
    from ..connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class SupersetCursor(Cursor):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._result_set_class = SupersetResultSet

    @synchronized
    def execute(
        self: Cursor, operation: str, parameters: Optional[List[Dict[str, Any]]] = None
    ) -> Cursor:
        statement = Statement(operation, SupersetSelect)
        if parameters and statement.sql_parser.parser.is_flat:
            # Only the PartiQL scan is sent to DynamoDB; the placeholders are in
            # the part evaluated by the query DB, which receives no parameters.
            raise NotSupportedError(
                "Bind parameters are not supported in a SELECT evaluated by the "
                "query DB (aggregates, GROUP BY, aliases); use literal values."
            )
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
        **kwargs
    ) -> None:
        super().__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            arraysize=arraysize,
            retry_config=retry_config,
            is_transaction=is_transaction,
            executor_class=SupersetStatementExecutor,
            **kwargs
        )


class SupersetStatementExecutor(DmlStatementExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
        **kwargs
    ) -> None:
        self._superset_table: str = "SUPERSET_QUERY"
        self._statement: Statement = statements[0]
        self._query_db: QueryDB = QueryDBHelper.create(self._statement, **kwargs)
        self._querydb_load_batch_size: int = self._query_db.config.load_batch_size
        super().__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
            **kwargs
        )

    def pre_execute(self) -> None:
        self.execute()

    def execute(self, **kwargs) -> None:
        try:
            parser = self._statement.sql_parser.parser
            if parser.is_nested:
                if not self._query_db.has_cache():
                    self._query_db.init_cache_table()
                    self._query_db.drop_query_table()
                    with self.connection.cursor() as cursor:
                        cursor.result_set_class = DynamoDBResultSet
                        cursor.execute_statement(self._statement)
                        self._load_into_query_db(cursor.result_set)

                desc_, results_ = self._query_db.query()
                self._rows.extend(results_)
                for d in desc_:
                    self._metadata.update(
                        ColumnInfo(
                            d[0],
                            d[0],
                            None,
                            None,
                            d[1],
                        )
                    )

            else:
                super().execute(**kwargs)
        except Exception as e:
            self._query_db.rollback()
            _logger.exception("Failed to execute statement.")
            raise OperationalError(*e.args) from e
        finally:
            self._query_db.close()

    def _load_into_query_db(self, ddb_result_set: DynamoDBResultSet) -> None:
        # Items are schemaless: with SELECT * a later page can add attributes,
        # so read every page before creating the table from the final metadata,
        # and pad rows read before an attribute first appeared.
        rows = []
        raw_data = ddb_result_set.fetchmany(self._querydb_load_batch_size)
        while len(raw_data) > 0:
            rows.extend(raw_data)
            raw_data = ddb_result_set.fetchmany(self._querydb_load_batch_size)

        metadata = ddb_result_set.metadata
        self._query_db.create_query_table(metadata)
        width = len(metadata)
        size = self._querydb_load_batch_size
        for start in range(0, len(rows), size):
            batch = [
                tuple(_to_query_db_value(value) for value in row)
                + (None,) * (width - len(row))
                for row in rows[start : start + size]  # noqa: E203
            ]
            self._query_db.write_raw_data(metadata, batch)


def _to_query_db_value(value: Any) -> Any:
    """Map/list/set attributes are stored as JSON text in the query DB."""
    if isinstance(value, (dict, list, set, frozenset)):
        return json.dumps(_to_json(value), ensure_ascii=False)
    return value


def _to_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _to_json(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted((_to_json(item) for item in value), key=repr)
    if isinstance(value, list):
        return [_to_json(item) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value
