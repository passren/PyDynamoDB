# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from pyparsing import Forward, Literal, Opt, ParseResults, StringEnd
from typing import Any, Dict

from .rewrite import strip_sql_comments

_logger = logging.getLogger(__name__)  # type: ignore

# Optional trailing semicolon, then nothing but whitespace.
_STATEMENT_END = Opt(Literal(";")).suppress() + StringEnd()


class Base(metaclass=ABCMeta):
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

        # The whole statement must match. Without the end anchor pyparsing stops
        # at the first unrecognized token and silently ignores the rest, so e.g.
        # "SELECT ... FROM t AS a WHERE ..." ran without its WHERE clause and
        # "DROP TABLE a b" dropped table a.
        # Comments are not statement tokens (callers often append a trailing
        # "-- tag"). A ";" swallowed by a comment is restored.
        executed = self._executed_statement
        statement = strip_sql_comments(executed).rstrip()
        if executed.rstrip().endswith(";") and not statement.endswith(";"):
            statement += ";"
        self._root_parse_results = (self.syntax_def + _STATEMENT_END).parseString(
            statement
        )
        return self._root_parse_results

    @abstractmethod
    def syntax_def(self) -> Forward:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def transform(self) -> Dict[str, Any]:
        raise NotImplementedError  # pragma: no cover

    def preprocess(self) -> None:
        pass  # pragma: no cover
