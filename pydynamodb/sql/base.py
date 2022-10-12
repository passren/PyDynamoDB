# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from pyparsing import ParseResults, Forward
from typing import Any, Dict

_logger = logging.getLogger(__name__)  # type: ignore


class Base(metaclass=ABCMeta):
    _statement = None
    _root_parse_results = None

    def __init__(self, statement: str) -> None:
        self._statement = statement

    @property
    def statement(self):
        return self._statement

    @property
    def root_parse_results(self) -> ParseResults:
        if self._root_parse_results is not None:
            return self._root_parse_results

        if self._statement is None:
            raise ValueError("Statement is not specified")

        self._root_parse_results = self.syntex_def.parseString(self._statement)
        return self._root_parse_results

    @abstractmethod
    def syntex_def(self) -> Forward:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def transform(self) -> Dict[str, Any]:
        raise NotImplementedError  # pragma: no cover
