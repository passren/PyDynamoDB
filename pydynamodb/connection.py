# -*- coding: utf-8 -*-
import logging
from boto3.session import Session
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

from .converter import Converter
from .cursor import BaseCursor, Cursor
from .error import NotSupportedError
from .util import RetryConfig


if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger = logging.getLogger(__name__)


class Connection:

    _SESSION_PASSING_ARGS: List[str] = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "region_name",
        "botocore_session",
        "profile_name",
    ]
    _CLIENT_PASSING_ARGS: List[str] = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "config",
        "api_version",
        "use_ssl",
        "verify",
        "endpoint_url",
    ]

    def __init__(
        self,
        region_name: Optional[str] = None,
        profile_name: Optional[str] = None,
        session: Optional[Session] = None,
        converter: Optional[Converter] = None,
        retry_config: Optional[RetryConfig] = None,
        cursor_class: Type[BaseCursor] = Cursor,
        cursor_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        self._kwargs = {
            **kwargs,
        }

        self.region_name = region_name
        self.profile_name = profile_name

        if session:
            self._session = session
        else:
            self._session = Session(
                region_name=self.region_name,
                profile_name=self.profile_name,
                **self._session_kwargs,
            )

        self._client = self._session.client(
            "dynamodb", region_name=self.region_name, **self._client_kwargs
        )

        self._converter = converter
        self._retry_config = retry_config if retry_config else RetryConfig()
        self.cursor_class = cursor_class
        self.cursor_kwargs = cursor_kwargs if cursor_kwargs else dict()
        self._cursor_pool = list()
        self._autocommit = True
        self._in_transaction = False

    @property
    def _session_kwargs(self) -> Dict[str, Any]:
        return {
            k: v for k, v in self._kwargs.items() if k in self._SESSION_PASSING_ARGS
        }

    @property
    def _client_kwargs(self) -> Dict[str, Any]:
        return {k: v for k, v in self._kwargs.items() if k in self._CLIENT_PASSING_ARGS}

    @property
    def session(self) -> Session:
        return self._session

    @property
    def client(self) -> "BaseClient":
        return self._client

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        try:
            if not self._autocommit and value:
                self._autocommit = True
                for cursor_ in self.cursor_pool:
                    cursor_.flush()
        finally:
            self._autocommit = value

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction

    @in_transaction.setter
    def in_transaction(self, value: bool) -> bool:
        self._in_transaction = False

    @property
    def cursor_pool(self) -> List[Optional[BaseCursor]]:
        return self._cursor_pool

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def cursor(self, cursor: Optional[Type[BaseCursor]] = None, **kwargs) -> BaseCursor:
        kwargs.update(self.cursor_kwargs)
        if not cursor:
            cursor = self.cursor_class
        converter = kwargs.pop("converter", self._converter)
        if not converter:
            converter = cursor.get_default_converter()
        cursor_ = cursor(
            connection=self,
            converter=converter,
            retry_config=kwargs.pop("retry_config", self._retry_config),
            **kwargs,
        )

        self._cursor_pool.append(cursor_)
        return cursor_

    def close(self) -> None:
        self._session = None
        self._client = None
        self._autocommit = True
        self._in_transaction = False
        self.cursor_pool.clear()

    def begin(self) -> None:
        self._autocommit = False
        self._in_transaction = True

    def commit(self) -> None:
        try:
            if self._in_transaction:
                for cursor_ in self.cursor_pool:
                    cursor_.execute_transaction()
        finally:
            self._autocommit = True
            self._in_transaction = False

    def rollback(self) -> None:
        raise NotSupportedError
