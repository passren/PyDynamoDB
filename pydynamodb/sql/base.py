# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from pyparsing import Forward, Literal, Opt, ParseResults, StringEnd
from typing import Any, Dict

_logger = logging.getLogger(__name__)  # type: ignore

# Optional trailing semicolon, then nothing but whitespace.
_STATEMENT_END = Opt(Literal(";")).suppress() + StringEnd()


class Base(metaclass=ABCMeta):
    # When True the whole statement must match ``syntax_def``. Otherwise pyparsing
    # stops at the first unrecognized token and silently ignores the rest.
    _parse_all = False

    def __init__(self, statement: str) -> None:
        self._statement = statement
        self._executed_statement = statement
        self._root_parse_results = None

        self.preprocess()

    @property
    def statement(self):
        return self._statement

    @property
    def executed_statement(self):
        return self._executed_statement

    @property
    def root_parse_results(self) -> ParseResults:
        if self._root_parse_results is not None:
            return self._root_parse_results

        if self._statement is None:
            raise ValueError("Statement is not specified")

        syntax_def = self.syntax_def
        if self._parse_all:
            syntax_def = syntax_def + _STATEMENT_END
        self._root_parse_results = syntax_def.parseString(self._executed_statement)
        return self._root_parse_results

    @abstractmethod
    def syntax_def(self) -> Forward:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def transform(self) -> Dict[str, Any]:
        raise NotImplementedError  # pragma: no cover

    def preprocess(self) -> None:
        pass  # pragma: no cover
