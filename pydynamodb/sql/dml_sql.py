# -*- coding: utf-8 -*-
import logging
from .base import Base
from typing import Any, Dict
from .common import KeyWords, Tokens
from pyparsing import (
    Word,
    CaselessKeyword,
    alphanums,
    quotedString,
    Group,
    ZeroOrMore,
    opAssoc,
    Opt,
    delimited_list,
    one_of,
    infix_notation,
    pyparsing_common as ppc,
)

_logger = logging.getLogger(__name__)  # type: ignore


class DmlBase(Base):
    _CONSISTENT_READ, _RETURN_CONSUMED_CAPACITY = map(
        CaselessKeyword,
        [
            "ConsistentRead",
            "ReturnConsumedCapacity",
        ],
    )

    _COLUMN = (
        Opt(KeyWords.SUPPRESS_QUOTE)
        + (KeyWords.STAR ^ Word(alphanums + "_.[]+-"))("column").set_name("column")
        + Opt(KeyWords.SUPPRESS_QUOTE)
    )

    _COLUMNS = delimited_list(_COLUMN)("columns").set_name("columns")

    _COLUMN_RVAL = (
        ppc.real() | ppc.signed_integer() | quotedString | _COLUMN | KeyWords.QUESTION
    )("column_rvalue").set_name("column_rvalue")

    _WHERE_CONDITION = Group(
        (_COLUMN + KeyWords.COMPARISON_OPERATORS + _COLUMN_RVAL)
        ^ (
            _COLUMN
            + KeyWords.IN
            + Group(
                "["
                + delimited_list(_COLUMN_RVAL)("in_values_list").set_name(
                    "in_values_list"
                )
                + "]"
            )
        )
        ^ (_COLUMN + KeyWords.BETWEEN + _COLUMN_RVAL + KeyWords.AND + _COLUMN_RVAL)
        ^ (_COLUMN + KeyWords.IS + Tokens.PARTIQL_DATA_TYPE)
        ^ (_COLUMN + KeyWords.IS + KeyWords.NOT + Tokens.PARTIQL_DATA_TYPE)
    )("where_condition").set_name("where_condition")

    _WHERE_CONDITIONS = infix_notation(
        _WHERE_CONDITION,
        [
            (KeyWords.NOT, 1, opAssoc.RIGHT),
            (KeyWords.AND, 2, opAssoc.LEFT),
            (KeyWords.OR, 2, opAssoc.LEFT),
            ((KeyWords.BETWEEN, KeyWords.AND), 3, opAssoc.RIGHT),
        ],
    )("where_conditions").set_name("where_expression")

    _RAW_SUPPORTED_OPTIONS = ZeroOrMore(
        Group(KeyWords.ORDER_BY + _COLUMN + Tokens.ORDER_BY_VALUE)(
            "raw_supported_option"
        ).set_name("raw_supported_option")
    )("raw_supported_options").set_name("raw_supported_options")

    _OPTIONS = ZeroOrMore(
        Group(
            KeyWords.LIMIT + Opt(KeyWords.EQUALS) + Tokens.INT_VALUE
            ^ _CONSISTENT_READ + Opt(KeyWords.EQUALS) + Tokens.BOOL_VALUE
            ^ _RETURN_CONSUMED_CAPACITY
            + Opt(KeyWords.EQUALS)
            + one_of("INDEXES TOTAL NONE")
        )("option").set_name("option")
    )("options").set_name("options")

    def __init__(self, statement: str) -> None:
        super(DmlBase, self).__init__(statement)
        self._limit = None
        self._consistent_read = False
        self._return_consumed_capacity = "NONE"

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def consistent_read(self) -> bool:
        return self._consistent_read

    @property
    def return_consumed_capacity(self) -> str:
        return self._return_consumed_capacity

    @property
    def syntax_def(self) -> None:
        return None

    def transform(self) -> Dict[str, Any]:
        return {"Statement": self._statement}
