# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta
from .common import QueryType, get_query_type
from .base import Base
from .ddl_create import DdlCreate
from .ddl_alter import DdlAlter
from .ddl_drop import DdlDrop
from .dml import DmlBase
from typing import Any, Dict

_logger = logging.getLogger(__name__)  # type: ignore

class SQLParser(metaclass=ABCMeta):
    _statement = None
    _query_type = None
    _root_parse_results = None

    def __init__(self, statement: str = "") -> None:
        self._statement = statement

    @property
    def statement(self):
        return self._statement

    @property
    def query_type(self):
        if self._statement is None:
            raise ValueError("Statement is not specified")

        if self._query_type is not None:
            return self._query_type
        
        self._query_type = get_query_type(self._statement)
        return self._query_type
    
    def _get_parse_class(self) -> Base:
        _parse_class = None
        if self.query_type == QueryType.CREATE:
            _parse_class = DdlCreate
        elif self.query_type == QueryType.ALTER:
            _parse_class = DdlAlter
        elif self.query_type == QueryType.DROP:
            _parse_class = DdlDrop
        elif (self.query_type == QueryType.INSERT
                or self.query_type == QueryType.UPDATE
                or self.query_type == QueryType.SELECT
                or self.query_type == QueryType.DELETE):
            _parse_class = DmlBase

        return _parse_class

    def transform(self) -> Dict[str, Any]:
        parser = self._get_parse_class()(self.statement)
        return parser.transform()
