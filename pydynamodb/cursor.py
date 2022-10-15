# -*- coding: utf-8 -*-
import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast, TypeVar, Tuple

from .converter import Converter
from .common import BaseCursor, CursorIterator, Statement, Statements
from .result_set import DynamoDBResultSet, DynamoDBDictResultSet
from .error import NotSupportedError, ProgrammingError
from .util import RetryConfig, synchronized

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore
_T = TypeVar("_T", bound="Cursor")


class Cursor(BaseCursor, CursorIterator):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        retry_config: RetryConfig,
        **kwargs,
    ) -> None:
        super(Cursor, self).__init__(
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

    @property
    def result_set(self) -> Optional[DynamoDBResultSet]:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def has_result_set(self) -> bool:
        return self._result_set is not None

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
            statement_ = Statement(operation)

            if parameters:
                statement_.api_request.update(
                    {
                        "Parameters": [
                            self._converter.serialize(parameter)
                            for parameter in parameters
                        ],
                    }
                )
            if self.connection.in_transaction:
                self._transaction_statements.append(statement_)
            else:
                self._statements.append(statement_)

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
                )
            finally:
                self._statements.clear()
                self._is_pooling = False
                self.connection.autocommit = True

    def fetchone(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(DynamoDBResultSet, self._result_set)
        return result_set.fetchone()

    def fetchmany(self, size: int = None) -> Optional[Dict[Any, Optional[Any]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(DynamoDBResultSet, self._result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(DynamoDBResultSet, self._result_set)
        return result_set.fetchall()

    def cancel(self) -> None:
        raise NotSupportedError

    def close(self) -> None:
        self._reset_state()
        self._statements = None
        self._transaction_statements = None

    def _reset_state(self) -> None:
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()
        self.result_set = None  # type: ignore


class DictCursor(Cursor):
    def __init__(self, **kwargs) -> None:
        super(DictCursor, self).__init__(**kwargs)
        self._result_set_class = DynamoDBDictResultSet
        if "dict_type" in kwargs:
            DynamoDBDictResultSet.dict_type = kwargs["dict_type"]
