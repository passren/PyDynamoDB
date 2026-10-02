# -*- coding: utf-8 -*-
"""
Syntax of PartiQL
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html
------------------
INSERT INTO table VALUE item

Sample SQL of Inserting:
------------------------
INSERT INTO "Music" value {'Artist' : 'Acme Band','SongTitle' : 'PartiQL Rocks'}
"""

import logging
from .dml_sql import DmlBase
from .common import KeyWords, Tokens
from pyparsing import Forward, Group, QuotedString, nested_expr, original_text_for
from typing import Any, Dict

_logger = logging.getLogger(__name__)  # type: ignore


class DmlInsert(DmlBase):

    # The whole item, braces included, as written. Braces must balance so that
    # nested maps ({'a': {'b': 1}}) are kept intact; braces inside quoted
    # strings are ignored.
    _ITEM = Group(
        original_text_for(
            nested_expr(
                "{",
                "}",
                ignore_expr=QuotedString("'", esc_quote="''", unquote_results=False)
                | QuotedString('"', esc_quote='""', unquote_results=False),
            )
        )("item_text")
    )("item").set_name("item")

    _INSERT_STATEMENT = (
        KeyWords.INSERT + KeyWords.INTO + Tokens.TABLE_NAME + KeyWords.VALUE + _ITEM
    )("insert_statement").set_name("insert_statement")

    _DML_INSERT_EXPR = Forward()
    _DML_INSERT_EXPR <<= _INSERT_STATEMENT

    def __init__(self, statement: str) -> None:
        super().__init__(statement)

    @property
    def syntax_def(self) -> Forward:
        return DmlInsert._DML_INSERT_EXPR

    def transform(self) -> Dict[str, Any]:
        table_name_ = self.root_parse_results["table"]
        item_ = self.root_parse_results["item"]["item_text"]

        table_ = '"%s"' % table_name_

        statement_ = "INSERT INTO {table} VALUE {item}"
        statement_ = statement_.format(
            table=table_,
            item=item_,
        )
        request = {"Statement": statement_.strip()}

        return request
