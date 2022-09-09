# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast, TypeVar, Tuple

from .converter import Converter
from .common import BaseCursor, CursorIterator
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
    def description(self) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        return self._result_set.description

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._result_set.errors


    @synchronized
    def execute(
        self: _T,
        operation: str,
        parameters: Optional[List[Dict[str, Any]]] = None,
        limit: int = BaseCursor.DEFAULT_LIMIT_SIZE,
        consistent_read: bool = False
    ) -> _T:
        statement_ = {
            "Statement": operation, 
        }

        if parameters:
            statement_.update({
                "Parameters": [
                    self._converter.serialize(parameter)
                    for parameter in parameters
                ],
                "ConsistentRead": consistent_read,
                "Limit": limit,
            })
        statements = [statement_]
        self._result_set = self._result_set_class(
                self._connection,
                self._converter,
                statements,
                self.arraysize,
                self._retry_config,
        )

        return self

    @synchronized
    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Dict[str, Any]]],
        consistent_read: bool = False
    ) -> None:
        statements = [
            {
                "Statement": operation, 
                "Parameters": [
                    self._converter.serialize(parameter)
                    for parameter in parameters
                ],
                "ConsistentRead": consistent_read,
            }
            for parameters in seq_of_parameters
        ]
        self._result_set = self._result_set_class(
                self._connection,
                self._converter,
                statements,
                self.arraysize,
                self._retry_config,
        )

    def fetchone(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(DynamoDBResultSet, self._result_set)
        return result_set.fetchone()

    def fetchmany(
        self,
        size: int = None
    ) -> Optional[Dict[Any, Optional[Any]]]:
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
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

class DictCursor(Cursor):
    def __init__(self, **kwargs) -> None:
        super(DictCursor, self).__init__(**kwargs)
        self._result_set_class = DynamoDBDictResultSet
        if "dict_type" in kwargs:
            DynamoDBDictResultSet.dict_type = kwargs["dict_type"]