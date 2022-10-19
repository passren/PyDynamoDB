# -*- coding: utf-8 -*-
from ..sql.dml_select import DmlSelect
from ..sql.common import KeyWords
from pyparsing import ZeroOrMore, Forward, Word, alphanums, printables, delimited_list
from typing import Any, Dict, List


class SupersetSelect(DmlSelect):
    _NESTED_SELECT_STATEMENT = (
        KeyWords.SELECT
        + delimited_list(
            Word(alphanums + "._-[]()\"'")("outer_column").set_name("outer_column")
        )("outer_columns").set_name("outer_columns")
        + KeyWords.FROM
        + KeyWords.LPAR
        + DmlSelect._SELECT_STATEMENT
        + KeyWords.RPAR
        + ZeroOrMore(Word(printables))("outer_exprs").set_name("outer_exprs")
    )("nested_select_statement").set_name("nested_select_statement")

    _SUPERSET_SELECT_EXPR = Forward()
    _SUPERSET_SELECT_EXPR <<= _NESTED_SELECT_STATEMENT

    def __init__(self, statement: str) -> None:
        super(SupersetSelect, self).__init__(statement)
        self._outer_columns = list()
        self._outer_exprs = None

    @property
    def outer_columns(self) -> List[str]:
        return self._outer_columns

    @property
    def outer_exprs(self) -> List[str]:
        return self._outer_exprs

    @property
    def syntax_def(self) -> Forward:
        return SupersetSelect._SUPERSET_SELECT_EXPR

    def transform(self) -> Dict[str, Any]:
        converted_ = super(SupersetSelect, self).transform()

        outer_columns = self.root_parse_results["outer_columns"]
        self._outer_columns.extend(o for o in outer_columns)

        outer_exprs = self.root_parse_results["outer_exprs"]
        self._outer_exprs = " ".join(o for o in outer_exprs)

        return converted_
