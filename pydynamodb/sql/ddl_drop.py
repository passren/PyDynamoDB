# -*- coding: utf-8 -*-
"""
Syntext of DDB table dropping with SQL:
DROP TABLE tbl_name

Sample SQL of Dropping Table:
-----------------------------
DROP TABLE Issues
"""
import logging
from .ddl_sql import DdlBase
from .common import KeyWords, Tokens
from pyparsing import Forward
from typing import Any, Dict

_logger = logging.getLogger(__name__)  # type: ignore


class DdlDrop(DdlBase):
    _DROP_TABLE_STATEMENT = (KeyWords.DROP + KeyWords.TABLE + Tokens.TABLE_NAME)(
        "drop_statement"
    ).set_name("drop_statement")

    _DDL_DROP_EXPR = Forward()
    _DDL_DROP_EXPR <<= _DROP_TABLE_STATEMENT

    def __init__(self, statement: str) -> None:
        self._statement = statement

    @property
    def syntex_def(self) -> Forward:
        return DdlDrop._DDL_DROP_EXPR

    def transform(self) -> Dict[str, Any]:
        if self.root_parse_results is None:
            raise ValueError("Statement was not parsed yet")

        request = dict()
        request.update({"TableName": self.root_parse_results["table"]})

        return request
