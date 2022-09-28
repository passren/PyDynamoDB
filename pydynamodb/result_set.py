import logging
from collections import OrderedDict, deque
from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional, Tuple, Type, cast

from .converter import Converter
from .common import CursorIterator
from .error import ProgrammingError, OperationalError, DataError
from .util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class DynamoDBResultSet(CursorIterator):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        statements: List[Dict[str, Any]],
        limit: int,
        arraysize: int,
        retry_config: RetryConfig,
        is_transaction: bool = False,
    ) -> None:
        super(DynamoDBResultSet, self).__init__(arraysize=arraysize)
        self._connection: Optional["Connection"] = connection
        self._converter = converter
        self._statements = statements
        self._limit = limit

        assert (
            self._statements or len(self._statements) > 0
        ), "Required statement not found."

        self._is_batch_execute = True if len(self._statements) > 1 else False
        self._retry_config = retry_config
        self._arraysize = arraysize
        self._metadata: Optional[OrderedDict[str, Dict[str, Any]]] = OrderedDict()
        self._rows: Deque[Dict[str, Optional[Any]]] = deque()
        self._errors: List[Dict[str, str]] = list()
        self._next_token: Optional[str] = None
        self._rownumber = 0
        self._is_transaction = is_transaction
        self._pre_fetch()

    @property
    def connection(self) -> "Connection":
        if self.is_closed:
            raise ProgrammingError("DynamoDBResultSet is closed.")
        return cast("Connection", self._connection)

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._errors

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        if self._metadata is None:
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
            for col, info in self._metadata.items()
        ]

    def __batch_fetch(self) -> Dict[str, Any]:

        if self._is_transaction:
            request = {
                "TransactStatements": [
                    {
                        "Statement": statement_["Statement"],
                        "Parameters": statement_["Parameters"],
                    }
                    for statement_ in self._statements
                ]
            }
            api_call_func = self.connection.client.execute_transaction
        else:
            request = {"Statements": self._statements}
            api_call_func = self.connection.client.batch_execute_statement

        try:
            response = retry_api_call(
                api_call_func,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to fetch result set.")
            raise OperationalError(*e.args) from e
        else:
            return cast(Dict[str, Any], response)

    def __fetch(self, next_token: Optional[str] = None) -> Dict[str, Any]:
        request = self._statements[0]

        if next_token:
            request.update({"NextToken": next_token})
        try:
            response = retry_api_call(
                self.connection.client.execute_statement,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to fetch result set.")
            raise OperationalError(*e.args) from e
        else:
            return cast(Dict[str, Any], response)

    def _fetch(self) -> None:
        if not self._next_token:
            raise ProgrammingError("NextToken is none or empty.")
        response = self.__fetch(self._next_token)
        self._process_rows(response)

    def _pre_fetch(self) -> None:
        if self._is_batch_execute:
            response = self.__batch_fetch()
            self._process_batch_rows(response)
        else:
            response = self.__fetch()
            self._process_rows(response)

    def fetchone(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]:
        if not self._rows and self._next_token:
            if not self._limit or (self._limit and self._rownumber < self._limit):
                self._fetch()
        if not self._rows:
            return None
        else:
            if self._rownumber is None:
                self._rownumber = 0
            if self._limit and self._rownumber >= self._limit:
                self._rows.clear()
                return None
            self._rownumber += 1
            return self._rows.popleft()

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

    def fetchall(
        self,
    ) -> List[Dict[Any, Optional[Any]]]:
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _process_batch_rows(self, response: Dict[str, Any]) -> None:
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

    def _process_rows(self, response: Dict[str, Any]) -> None:
        rows = response.get("Items", None)
        if rows is None:
            raise DataError("KeyError `Items`")

        processed_rows = list()
        for row in rows:
            row_ = self._process_row_item(row)
            processed_rows.append(row_)

        self._rows.extend(processed_rows)
        self._next_token = response.get("NextToken", None)

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

    @property
    def is_closed(self) -> bool:
        return self._connection is None

    def close(self) -> None:
        self._connection = None
        self._statements.clear()
        self._is_transaction = None
        self._is_batch_execute = None
        self._metadata.clear()
        self._rows.clear()
        self._next_token = None
        self._rownumber = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DynamoDBDictResultSet(DynamoDBResultSet):

    # You can override this to use OrderedDict or other dict-like types.
    dict_type: Type[Any] = dict

    def _process_row_item(self, row) -> Optional[Dict]:

        return self.dict_type(
            [(col, self._converter.deserialize(val)) for col, val in row.items()]
        )
