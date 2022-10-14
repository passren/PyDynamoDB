# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, cast

from .converter import Converter
from .common import CursorIterator, Statements
from .executor import dispatch_executor
from .error import ProgrammingError
from .util import RetryConfig

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class DynamoDBResultSet(CursorIterator):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: Statements,
        arraysize: int,
        retry_config: RetryConfig,
        is_transaction: bool = False,
    ) -> None:
        super(DynamoDBResultSet, self).__init__(arraysize=arraysize)
        assert statements and len(statements) > 0, "Required statements not found."
        self._connection: Optional["Connection"] = connection
        self._arraysize = arraysize
        self._statements = statements
        self._executor = dispatch_executor(
            connection, converter, statements, retry_config, is_transaction
        )
        assert self._executor is not None, "Executor is not specified"

    @property
    def connection(self) -> "Connection":
        if self.is_closed:
            raise ProgrammingError("DynamoDBResultSet is closed.")
        return cast("Connection", self._connection)

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._executor.errors

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        if self._executor.metadata is None:
            return None
        return [
            (
                info["name"],
                ",".join(info["type"]),
                None,
                None,
                None,
                None,
                None,
            )
            for col, info in self._executor.metadata.items()
        ]

    def fetchone(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]:
        limit_ = self._statements.limit
        if not self._executor.rows and self._executor.next_token:
            if not limit_ or (limit_ and self._rownumber < limit_):
                self._executor.execute()
        if not self._executor.rows:
            return None
        else:
            if self._rownumber is None:
                self._rownumber = 0
            if limit_ and self._rownumber >= limit_:
                self._executor.rows.clear()
                return None
            self._rownumber += 1
            return self._executor.rows.popleft()

    def fetchmany(self, size: Optional[int] = None) -> List[Dict[Any, Optional[Any]]]:
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def fetchall(self) -> List[Dict[Any, Optional[Any]]]:
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    @property
    def is_closed(self) -> bool:
        return self._connection is None

    def close(self) -> None:
        self._connection = None
        self._executor = None
        self._rownumber = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DynamoDBDictResultSet(DynamoDBResultSet):

    # You can override this to use OrderedDict or other dict-like types.
    dict_type: Type[Any] = dict
