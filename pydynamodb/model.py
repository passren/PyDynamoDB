# -*- coding: utf-8 -*-
import logging
from typing import List, Optional, Any
from collections import OrderedDict
from .sql.dml_sql import DmlFunction

_logger = logging.getLogger(__name__)  # type: ignore


class ColumnInfo:
    def __init__(
        self,
        name: str,
        original: str,
        alias: str = None,
        function: DmlFunction = None,
        type_code: str = None,
        display_size: str = None,
        internal_size: str = None,
        precision: str = None,
        scale: str = None,
        null_ok: str = None,
    ) -> None:
        self._name = name
        self._original = original
        self._alias = alias
        self._function = function
        self._type_code = type_code
        self._display_size = display_size
        self._internal_size = internal_size
        self._precision = precision
        self._scale = scale
        self._null_ok = null_ok

    @property
    def name(self) -> str:
        return self._name

    @property
    def original(self) -> str:
        return self._original

    @property
    def alias(self) -> str:
        return self._alias

    @alias.setter
    def alias(self, value: str) -> None:
        self._alias = value

    @property
    def function(self) -> DmlFunction:
        return self._function

    @property
    def type_code(self) -> str:
        return self._type_code

    @type_code.setter
    def type_code(self, value: str) -> None:
        self._type_code = value

    @property
    def display_size(self) -> str:
        return self._display_size

    @display_size.setter
    def display_size(self, value: str) -> None:
        self._display_size = value

    @property
    def internal_size(self) -> str:
        return self._internal_size

    @internal_size.setter
    def internal_size(self, value: str) -> None:
        self._internal_size = value

    @property
    def precision(self) -> str:
        return self._precision

    @precision.setter
    def precision(self, value: str) -> None:
        self._precision = value

    @property
    def scale(self) -> str:
        return self._scale

    @scale.setter
    def scale(self, value: str) -> None:
        self._scale = value

    @property
    def null_ok(self) -> str:
        return self._null_ok

    @null_ok.setter
    def null_ok(self, value: str) -> None:
        self._null_ok = value

    def __str__(self) -> str:
        return "%s | %s | %s | %s | %s | %s | %s | %s | %s" % (
            self.name,
            self.original,
            self.alias,
            self.type_code,
            self.display_size,
            self.internal_size,
            self.precision,
            self.scale,
            self.null_ok,
        )


class Metadata:
    def __init__(self, column_infos: List[Optional[ColumnInfo]] = None) -> None:
        self._column_infos = OrderedDict()

        if column_infos is not None:
            for column_info in column_infos:
                self._column_infos.update({column_info.name: column_info})

    def get(self, name: str, default: Any = None) -> ColumnInfo:
        return self._column_infos.get(name, default)

    def append(self, column_info: ColumnInfo) -> None:
        self._column_infos.update({column_info.name: column_info})

    def update(self, column_info: ColumnInfo) -> None:
        self._column_infos.update({column_info.name: column_info})

    def clear(self) -> None:
        self._column_infos.clear()

    def index(self, key: str) -> int:
        return list(self._column_infos).index(key)

    def __len__(self):
        return len(self._column_infos.keys())

    def __getitem__(self, key: str) -> ColumnInfo:
        return self._column_infos[key]

    def __contains__(self, key: str):
        return key in self._column_infos

    __iterator_index = 0

    def __next__(self):
        keys = list(self._column_infos)
        if self.__iterator_index < len(keys):
            ret = self._column_infos[keys[self.__iterator_index]]
            self.__iterator_index += 1
            return ret
        raise StopIteration

    def __iter__(self):
        return self
