# -*- coding: utf-8 -*-
import logging
import json
from datetime import datetime
from abc import ABCMeta, abstractmethod
from collections import OrderedDict, deque
from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional, Tuple, cast

from .sql.common import QueryType
from .converter import Converter
from .common import Statements
from .error import OperationalError, DataError
from .util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class BaseExecutor(metaclass=ABCMeta):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        self._connection: Optional["Connection"] = connection
        self._converter = converter
        self._statements = statements
        assert (
            self._statements and len(self._statements) > 0
        ), "Required statements not found."

        self._retry_config = retry_config
        self._next_token: Optional[str] = None
        self._metadata: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._rows: Deque[Dict[str, Optional[Any]]] = deque()
        self._errors: List[Dict[str, str]] = list()
        self._pre_execute()

    @property
    def connection(self) -> "Connection":
        return self._connection

    @property
    def next_token(self) -> Optional[str]:
        return self._next_token

    @property
    def metadata(self) -> "OrderedDict[str, Dict[str, Any]]":
        return self._metadata

    @property
    def rows(self) -> Deque[Dict[str, Optional[Any]]]:
        return self._rows

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._errors

    def _pre_execute(self) -> None:
        self.execute()

    @abstractmethod
    def execute(self, **kwargs) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def process_rows(self, response: Dict[str, Any]) -> None:
        raise NotImplementedError  # pragma: no cover

    def _dispatch_api_call(
        self, api_call_func, request: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            response = retry_api_call(
                api_call_func,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to execute statement.")
            raise OperationalError(*e.args) from e
        else:
            return cast(Dict[str, Any], response)

    def close(self) -> None:
        self._metadata.clear()
        self._rows.clear()
        self._errors.clear()
        self._next_token = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def dispatch_executor(
    connection: "Connection",
    converter: Converter,
    statements: Statements,
    retry_config: RetryConfig,
    is_transaction: bool = False,
) -> BaseExecutor:
    if statements is None or len(statements) == 0:
        return None

    executor_class = None
    if statements.query_type[0] == "DML":
        if len(statements) > 1:
            if is_transaction:
                executor_class = DmlTransactionExecutor
            else:
                executor_class = DmlBatchExecutor
        else:
            executor_class = DmlStatementExecutor
    elif statements.query_type == QueryType.CREATE:
        executor_class = DdlCreateExecutor
    elif statements.query_type == QueryType.ALTER:
        executor_class = DdlAlterExecutor
    elif statements.query_type == QueryType.DROP:
        executor_class = DdlDropExecutor
    elif statements.query_type == QueryType.LIST:
        executor_class = UtilListTablesExecutor
    elif statements.query_type == QueryType.DESC:
        executor_class = UtilDescTableExecutor
    elif statements.query_type == QueryType.CREATE_GLOBAL:
        executor_class = DdlCreateGlobalExecutor
    elif statements.query_type == QueryType.DROP_GLOBAL:
        executor_class = DdlDropGlobalExecutor
    elif statements.query_type == QueryType.LIST_GLOBAL:
        executor_class = UtilListGlobalTablesExecutor
    elif statements.query_type == QueryType.DESC_GLOBAL:
        executor_class = UtilDescGlobalTableExecutor
    else:
        raise LookupError(
            "Not support executor for query type: %s" % str(statements.query_type)
        )

    return executor_class(connection, converter, statements, retry_config)


class DmlStatementExecutor(BaseExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DmlStatementExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        if self.next_token:
            request.update({"NextToken": self.next_token})

        response = self._dispatch_api_call(
            self.connection.client.execute_statement, request
        )
        self.process_rows(response)

    def process_rows(self, response: Dict[str, Any]) -> None:
        rows = response.get("Items", None)
        if rows is None:
            raise DataError("KeyError `Items`")

        processed_rows = list()
        for row in rows:
            row_ = self._process_row_item(row)
            processed_rows.append(row_)

        self._rows.extend(processed_rows)
        self._next_token = response.get("NextToken", None)

    def _process_row_item(self, row) -> Optional[Tuple]:
        row_ = dict()
        for col, val in row.items():
            type_ = next(iter(val.keys()))
            val_ = self._converter.deserialize(val)
            col_index = self._process_metadata(col, type_)
            row_[col_index] = val_

        row__ = list()
        for i in range(len(self._metadata)):
            row__.append(None)
        for i, v in row_.items():
            row__[i] = v

        return tuple(row__)

    def _process_metadata(self, col_name: str, type: str) -> int:
        if col_name in self._metadata:
            self._metadata[col_name]["type"].add(type)
        else:
            types_ = set()
            types_.add(type)
            self._metadata[col_name] = {
                "name": col_name,
                "type": types_,
            }

        return list(self._metadata).index(col_name)


class DmlBatchExecutor(DmlStatementExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DmlBatchExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        request = {
            "Statements": [statement_.api_request for statement_ in self._statements]
        }

        response = self._dispatch_api_call(
            self.connection.client.batch_execute_statement, request
        )
        self.process_rows(response)

    def process_rows(self, response: Dict[str, Any]) -> None:
        rows = response.get("Responses", None)
        if rows is None:
            raise DataError("KeyError `Responses` in BatchStatementResponse")

        processed_error_rows = list()
        processed_rows = list()
        for row in rows:
            error = row.get("Error", None)
            if error:
                processed_error_rows.append(
                    {
                        error.get("Code"): error.get("Message"),
                    }
                )
                continue

            item = row.get("Item", None)
            if item:
                row_ = self._process_row_item(item)
                processed_rows.append(row_)

        self._errors.extend(processed_error_rows)
        self._rows.extend(processed_rows)


class DmlTransactionExecutor(DmlBatchExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DmlTransactionExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        request = {
            "TransactStatements": [
                statement_.api_request for statement_ in self._statements
            ]
        }

        next_token = kwargs.get("NextToken", None)
        if next_token:
            request.update({"ClientRequestToken": next_token})

        response = self._dispatch_api_call(
            self.connection.client.execute_transaction, request
        )
        self.process_rows(response)


class DdlExecutor(BaseExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DdlExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def process_rows(self, response: Dict[str, Any]) -> None:
        self.rows.extend(
            (k, json.dumps(v, default=self._handle_conversion))
            for k, v in response.items()
        )
        self.metadata.update(
            {
                "response_name": {
                    "name": "response_name",
                    "type": ["S"],
                },
                "response_value": {
                    "name": "response_value",
                    "type": ["S"],
                },
            }
        )

    def _handle_conversion(self, val: Any) -> str:
        if isinstance(val, datetime):
            return str(val)
        else:
            return val


class DdlCreateExecutor(DdlExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DdlCreateExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        response = self._dispatch_api_call(self.connection.client.create_table, request)
        self.process_rows(response)


class DdlAlterExecutor(DdlExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DdlAlterExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        response = self._dispatch_api_call(self.connection.client.update_table, request)
        self.process_rows(response)


class DdlDropExecutor(DdlExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DdlDropExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        response = self._dispatch_api_call(self.connection.client.delete_table, request)
        self.process_rows(response)


class DdlCreateGlobalExecutor(DdlExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DdlCreateGlobalExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        response = self._dispatch_api_call(
            self.connection.client.create_global_table, request
        )
        self.process_rows(response)


class DdlDropGlobalExecutor(DdlExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(DdlDropGlobalExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        response = self._dispatch_api_call(
            self.connection.client.update_global_table, request
        )
        self.process_rows(response)


class UtilListTablesExecutor(BaseExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(UtilListTablesExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        if self.next_token:
            request.update({"ExclusiveStartTableName": self.next_token})

        response = self._dispatch_api_call(self.connection.client.list_tables, request)
        self.process_rows(response)

    def process_rows(self, response: Dict[str, Any]) -> None:
        tables = response.get("TableNames", None)
        if tables is None:
            raise DataError("KeyError `TableNames`")

        self._rows.extend((t) for t in tables)
        self._next_token = response.get("LastEvaluatedTableName", None)
        self.metadata.update(
            {
                "table_name": {
                    "name": "table_name",
                    "type": ["S"],
                },
            }
        )


class UtilDescTableExecutor(DdlExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(UtilDescTableExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        response = self._dispatch_api_call(
            self.connection.client.describe_table, request
        )
        self.process_rows(response)


class UtilListGlobalTablesExecutor(BaseExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(UtilListGlobalTablesExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        if self.next_token:
            request.update({"ExclusiveStartGlobalTableName": self.next_token})

        response = self._dispatch_api_call(
            self.connection.client.list_global_tables, request
        )
        self.process_rows(response)

    def process_rows(self, response: Dict[str, Any]) -> None:
        tables = response.get("GlobalTables", None)
        if tables is None:
            raise DataError("KeyError `GlobalTables`")

        for table in tables:
            table_name = table["GlobalTableName"]
            regions = ",".join([r["RegionName"] for r in table["ReplicationGroup"]])
            self._rows.append((table_name, regions))

        self._next_token = response.get("LastEvaluatedGlobalTableName", None)
        self.metadata.update(
            {
                "table_name": {
                    "name": "table_name",
                    "type": ["S"],
                },
                "region_names": {
                    "name": "region_names",
                    "type": ["S"],
                },
            }
        )


class UtilDescGlobalTableExecutor(DdlExecutor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        retry_config: RetryConfig,
    ) -> None:
        super(UtilDescGlobalTableExecutor, self).__init__(
            connection=connection,
            converter=converter,
            statements=statements,
            retry_config=retry_config,
        )

    def execute(self, **kwargs) -> None:
        statement_ = self._statements[0]
        request = statement_.api_request

        response = self._dispatch_api_call(
            self.connection.client.describe_global_table, request
        )
        self.process_rows(response)
