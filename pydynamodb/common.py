# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from .sql.common import QueryType
from .sql.parser import SQLParser
from .converter import Converter, DefaultTypeConverter
from .error import ProgrammingError, OperationalError
from .util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class BaseCursor(metaclass=ABCMeta):

    DEFAULT_LIST_TABLES_LIMIT_SIZE: int = 100

    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        retry_config: RetryConfig,
        **kwargs,
    ) -> None:
        super(BaseCursor, self).__init__()
        self._connection = connection
        self._converter = converter
        self._retry_config = retry_config

    @property
    def connection(self) -> "Connection":
        return self._connection

    @staticmethod
    def get_default_converter(unload: bool = False) -> Union[DefaultTypeConverter, Any]:
        return DefaultTypeConverter()

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        return None

    def _list_tables(
        self,
        next_token: Optional[str] = None,
        limit: int = DEFAULT_LIST_TABLES_LIMIT_SIZE,
    ) -> Tuple[Optional[str], List[str]]:
        request: Dict[str, Any] = {"Limit": limit}

        if next_token:
            request.update({"ExclusiveStartTableName": next_token})

        try:
            response = retry_api_call(
                self.connection._client.list_tables,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to list tables.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("LastEvaluatedTableName", None), response.get(
                "TableNames", []
            )

    def list_tables(self) -> List[str]:
        tables_ = []
        next_token = None
        while True:
            next_token, response = self._list_tables(next_token)
            tables_.extend(response)
            if not next_token:
                break
        return tables_

    def _get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        request: Dict[str, Any] = {"TableName": table_name}

        try:
            response = retry_api_call(
                self.connection._client.describe_table,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("Table", {})

    def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        return self._get_table_metadata(table_name)

    @abstractmethod
    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]]
    ) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CursorIterator(metaclass=ABCMeta):

    DEFAULT_FETCH_SIZE: int = 1000

    def __init__(self, **kwargs) -> None:
        super(CursorIterator, self).__init__()
        self.arraysize: int = kwargs.get("arraysize", self.DEFAULT_FETCH_SIZE)
        self._rownumber: Optional[int] = 0

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                f"MaxResults is more than maximum allowed length {self.DEFAULT_FETCH_SIZE}."
            )
        self._arraysize = value

    @property
    def rownumber(self) -> Optional[int]:
        return self._rownumber

    @property
    def rowcount(self) -> int:
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @abstractmethod
    def fetchone(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchmany(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchall(self):
        raise NotImplementedError  # pragma: no cover

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        else:
            return row

    def __iter__(self):
        return self


class Statement(metaclass=ABCMeta):
    def __init__(self, operation: str) -> None:
        self._sql_parser = SQLParser(operation)
        self._api_request = self._sql_parser.transform()

    @property
    def sql_parser(self) -> SQLParser:
        return self._sql_parser

    @property
    def api_request(self) -> Optional[Dict[str, Any]]:
        return self._api_request


class Statements(metaclass=ABCMeta):
    def __init__(self, statements: List[Statement] = None) -> None:
        if statements is not None:
            for statement_ in statements:
                self._validate(statement_)
            self._statments = statements
        else:
            self._statments = list()

        self._query_type = None
        self._query_category = None
        self._operation_type = None
        self._limit = None

    @property
    def query_type(self) -> QueryType:
        return self._query_type

    @property
    def query_category(self) -> str:
        return self._query_category

    @property
    def operation_type(self) -> str:
        return self._operation_type

    @property
    def limit(self) -> str:
        return self._limit

    def _validate(self, statement: Statement) -> bool:
        if statement is not None:
            query_type_ = statement.sql_parser.query_type
            query_category_ = statement.sql_parser.query_category
            operation_type_ = statement.sql_parser.operation_type
            if self._query_type is None:
                self._query_type = query_type_
                self._query_category = query_category_
                self._operation_type = operation_type_

                if (
                    self._query_type == QueryType.SELECT
                    or self._query_type == QueryType.LIST
                ):
                    self._limit = statement.sql_parser.parser.limit

                return True
            else:
                if query_category_ == "DML" and operation_type_ == self._query_type[1]:
                    return True
                elif query_category_ == "DDL" or query_category_ == "UTIL":
                    raise OperationalError("Not support batch DDL/Utility statements")
                else:
                    raise OperationalError(
                        "Not support both read and write operation statements in the request"
                    )

    def append(self, statement: Statement) -> None:
        if self._validate(statement):
            self._statments.append(statement)

    def extend(self, statements: "Statements") -> None:
        if statements is not None:
            for statement_ in statements:
                self.append(statement_)

    def clear(self) -> None:
        self._statments.clear()
        self._query_type = None

    def __len__(self):
        return len(self._statments)

    def __getitem__(self, key):
        return self._statments[key]

    __iterator_index = 0

    def __next__(self):
        if self.__iterator_index < len(self._statments):
            ret = self._statments[self.__iterator_index]
            self.__iterator_index += 1
            return ret
        raise StopIteration

    def __iter__(self):
        return self
