# -*- coding: utf-8 -*-
import logging
from copy import deepcopy

from pyparsing import ParseException
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast, TypeVar, Tuple

from .converter import Converter
from .common import BaseCursor, CursorIterator
from .model import Statement, Statements
from .sql.dml_select import DmlSelect
from .sql.rewrite import unalias_select
from .result_set import DynamoDBResultSet, DynamoDBDictResultSet
from .error import NotSupportedError, ProgrammingError
from .util import RetryConfig, synchronized

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore
_T = TypeVar("_T", bound="Cursor")


def unaliased_statement(operation: str, parser_class: Any = None) -> Any:
    """A Statement for the alias-resolved form of ``operation``, else None."""
    unaliased = unalias_select(operation)
    if unaliased is None:
        return None
    try:
        Statement(unaliased, DmlSelect).api_request
    except ParseException:
        return None
    return Statement(unaliased, parser_class) if parser_class else Statement(unaliased)


class Cursor(BaseCursor, CursorIterator):
    NO_RESULT_SET = "No result set."

    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        retry_config: RetryConfig,
        **kwargs
    ) -> None:
        super().__init__(
            connection=connection,
            converter=converter,
            retry_config=retry_config,
            **kwargs,
        )
        self._result_set: Optional[DynamoDBResultSet] = None
        self._result_set_class = DynamoDBResultSet
        self._statements: Statements = Statements()
        self._transaction_statements: Statements = Statements()
        self._is_pooling: bool = False
        self._kwargs = kwargs

    @property
    def result_set(self) -> Optional[CursorIterator]:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def has_result_set(self) -> bool:
        return self._result_set is not None

    @property
    def result_set_class(self) -> Optional[CursorIterator]:
        return self._result_set_class

    @result_set_class.setter
    def result_set_class(self, val) -> None:
        self._result_set_class = val

    @property
    def rowcount(self) -> int:
        return self._result_set.rowcount if self._result_set else -1

    @property
    def rownumber(self) -> Optional[int]:
        return self._result_set.rownumber if self._result_set else None

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        return self._result_set.description

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._result_set.errors

    @synchronized
    def execute(
        self: _T, operation: str, parameters: Optional[List[Dict[str, Any]]] = None
    ) -> _T:
        try:
            statement = Statement(operation)
        except ParseException as e:
            if operation.lstrip()[:6].upper() != "SELECT":
                raise
            # A single-table alias is resolved and runs as plain PartiQL.
            statement = unaliased_statement(operation)
            if statement is None:
                raise NotSupportedError(
                    "SELECT is not supported by PartiQL as written (subqueries, "
                    "joins, GROUP BY and aggregates require connector=superset): "
                    "%s" % e
                ) from e
        return self.execute_statement(statement, parameters)

    @synchronized
    def execute_statement(
        self: _T,
        statement: Statement,
        parameters: Optional[List[Dict[str, Any]]] = None,
    ) -> _T:
        try:
            if parameters:
                statement.api_request.update(
                    {
                        "Parameters": [
                            self._converter.serialize(parameter)
                            for parameter in parameters
                        ],
                    }
                )
            if self.connection.in_transaction:
                self._transaction_statements.append(statement)
            else:
                self._statements.append(statement)

            if not self._is_pooling and self.connection.autocommit:
                self.flush()

        except Exception as e:
            if self.connection.in_transaction:
                self.connection.in_transaction = False
                self.connection.autocommit = True
            raise e

        return self

    @synchronized
    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Dict[str, Any]]],
    ) -> None:
        self._is_pooling = True
        for i, parameters in enumerate(seq_of_parameters):
            if i == len(seq_of_parameters) - 1:
                self._is_pooling = False
            self.execute(operation, parameters)

    @synchronized
    def execute_transaction(self) -> None:
        if len(self._transaction_statements) > 0:
            try:
                self._reset_state()
                self._result_set = self._result_set_class(
                    self._connection,
                    self._converter,
                    deepcopy(self._transaction_statements),
                    self.arraysize,
                    self._retry_config,
                    is_transaction=True,
                    **self._kwargs,
                )
            finally:
                self._transaction_statements.clear()

    @synchronized
    def flush(self) -> None:
        if len(self._statements) > 0:
            self._reset_state()
            try:
                self._result_set = self._result_set_class(
                    self._connection,
                    self._converter,
                    deepcopy(self._statements),
                    self.arraysize,
                    self._retry_config,
                    is_transaction=False,
                    **self._kwargs,
                )
            finally:
                self._statements.clear()
                self._is_pooling = False
                self.connection.autocommit = True

    def fetchone(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]:
        if not self.has_result_set:
            raise ProgrammingError(Cursor.NO_RESULT_SET)
        result_set = cast(DynamoDBResultSet, self._result_set)
        return result_set.fetchone()

    def fetchmany(self, size: int = None) -> Optional[Dict[Any, Optional[Any]]]:
        if not self.has_result_set:
            raise ProgrammingError(Cursor.NO_RESULT_SET)
        result_set = cast(DynamoDBResultSet, self._result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]:
        if not self.has_result_set:
            raise ProgrammingError(Cursor.NO_RESULT_SET)
        result_set = cast(DynamoDBResultSet, self._result_set)
        return result_set.fetchall()

    def cancel(self) -> None:
        raise NotSupportedError

    def close(self) -> None:
        self._reset_state()
        self._statements = None
        self._transaction_statements = None
        # Leave the connection's pool: otherwise a long-lived connection keeps
        # every closed cursor, and commit() fails on their cleared statements.
        pool = self.connection.cursor_pool
        if self in pool:
            pool.remove(self)

    def _reset_state(self) -> None:
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()
        self.result_set = None  # type: ignore


class DictCursor(Cursor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        retry_config: RetryConfig,
        **kwargs
    ) -> None:
        super().__init__(
            connection=connection,
            converter=converter,
            retry_config=retry_config,
            **kwargs,
        )
        self._result_set: Optional[DynamoDBDictResultSet] = None
        self._result_set_class = DynamoDBDictResultSet
