# -*- coding: utf-8 -*-
from ..sql.dml_select import DmlSelect
from ..sql.common import KeyWords
from ..sql.util import flatten_list
from pyparsing import (
    ZeroOrMore,
    Forward,
    Word,
    printables,
    OneOrMore,
)
from typing import Any, Dict


class SupersetSelect(DmlSelect):
    _NESTED_SELECT_STATEMENT = (
        DmlSelect._SELECT_STATEMENT
        | KeyWords.SELECT
        + OneOrMore(Word(printables), stop_on=KeyWords.FROM)("outer_columns").set_name(
            "outer_columns"
        )
        + KeyWords.FROM
        + KeyWords.LPAR
        + DmlSelect._SELECT_STATEMENT
        + KeyWords.RPAR
        + ZeroOrMore(Word(printables))("outer_exprs").set_name("outer_exprs")
    )("nested_select_statement").set_name("nested_select_statement")

    _SUPERSET_SELECT_EXPR = Forward()
    _SUPERSET_SELECT_EXPR <<= _NESTED_SELECT_STATEMENT

    def __init__(self, statement: str) -> None:
        super().__init__(statement)
        self._outer_columns = None
        self._outer_exprs = None
        self._is_nested = False

    @property
    def outer_columns(self) -> str:
        return self._outer_columns

    @property
    def outer_exprs(self) -> str:
        return self._outer_exprs

    @property
    def is_nested(self) -> bool:
        return self._is_nested

    @property
    def syntax_def(self) -> Forward:
        return SupersetSelect._SUPERSET_SELECT_EXPR

    def transform(self) -> Dict[str, Any]:
        converted_ = super().transform()

        outer_columns = self.root_parse_results.get("outer_columns", None)
        if outer_columns is not None:
            outer_columns = outer_columns.as_list()
            self._outer_columns = " ".join(flatten_list(outer_columns))
            self._is_nested = True

        outer_exprs = self.root_parse_results.get("outer_exprs", None)
        if outer_exprs is not None:
            self._outer_exprs = " ".join(o for o in outer_exprs)

        return converted_
