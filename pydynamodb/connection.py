# -*- coding: utf-8 -*-
import imp
import logging

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

from .converter import Converter
from .cursor import BaseCursor, Cursor
from .error import NotSupportedError
from .util import RetryConfig

from boto3.session import Session

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

    @property
    def _session_kwargs(self) -> Dict[str, Any]:
        return {k: v for k, v in self._kwargs.items() if k in self._SESSION_PASSING_ARGS}

    @property
    def _client_kwargs(self) -> Dict[str, Any]:
        return {k: v for k, v in self._kwargs.items() if k in self._CLIENT_PASSING_ARGS}

    @property
    def session(self) -> Session:
        return self._session

    @property
    def client(self) -> "BaseClient":
        return self._client

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
        return cursor(
            connection=self,
            converter=converter,
            retry_config=kwargs.pop("retry_config", self._retry_config),
            **kwargs,
        )

    def close(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        raise NotSupportedError