# -*- coding: utf-8 -*-
import ast
import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Callable, Dict, Optional, Set, List, Union, Type

_logger = logging.getLogger(__name__)  # type: ignore


class Serializer(metaclass=ABCMeta):
    def __init__(
        self,
        mapping: Optional[Dict[Any, Callable[[Optional[str]], Optional[Any]]]] = None,
    ) -> None:
        if mapping is None:
            self._mapping = self.get_default_converters()
        else:
            self._mapping = mapping

    @property
    def mapping(self) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        return self._mapping

    def _to_string(self, value: Optional[str]) -> Optional[Dict[str, str]]:
        if value is None:
            return None
        return {"S": value}

    def _to_number(
        self, value: Optional[Union[int, float]]
    ) -> Optional[Dict[str, str]]:
        if value is None:
            return None
        return {"N": str(value)}

    def _to_binary(self, value: Optional[bytes]) -> Optional[Dict[str, str]]:
        if value is None:
            return None
        return {"B": value.decode()}

    def _to_set(self, value: Optional[Set[Any]]) -> Optional[Dict[str, List[str]]]:
        if value is None:
            return None

        value_ = next(iter(value))
        if isinstance(value_, type(1)) or isinstance(value_, type(1.0)):
            return {"NS": [str(v) for v in value]}
        elif isinstance(value_, type(b"")):
            return {"BS": [v.decode() for v in value]}
        else:
            return {"SS": [v for v in value]}

    def _to_map(self, value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if value is None:
            return None

        converted_ = {}
        for k, v in value.items():
            type_ = type(v)
            converted_[k] = self._mapping.get(type_, None)(v)
        return {"M": converted_}

    def _to_list(self, value: Optional[List[Any]]) -> Optional[Dict[str, Any]]:
        if value is None:
            return None

        converted_ = []
        for v in value:
            type_ = type(v)
            converted_.append(self._mapping.get(type_, None)(v))
        return {"L": converted_}

    def _to_null(self, value: Optional[Any]) -> Optional[bool]:
        return {"NULL": False if value else True}

    def _to_bool(self, value: Optional[bool]) -> Optional[bool]:
        return {"BOOL": value}

    def _to_default(self, value: Optional[Any]) -> Optional[str]:
        return {"S": str(value)}

    def get(self, type_: Type) -> Callable[[Optional[str]], Optional[Any]]:
        return self._mapping.get(type_, self._to_default)

    def get_default_converters(
        self,
    ) -> Dict[Type[Any], Callable[[Optional[str]], Optional[Any]]]:
        return {
            str: self._to_string,
            int: self._to_number,
            float: self._to_number,
            bytes: self._to_binary,
            set: self._to_set,
            dict: self._to_map,
            list: self._to_list,
            type(None): self._to_null,
            bool: self._to_bool,
        }


class Deserializer(metaclass=ABCMeta):
    def __init__(
        self,
        mapping: Optional[Dict[Any, Callable[[Optional[str]], Optional[Any]]]] = None,
    ) -> None:
        if mapping is None:
            self._mapping = self.get_default_converters()
        else:
            self._mapping = mapping

    @property
    def mapping(self) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        return self._mapping

    def _to_number(self, value: Optional[str]) -> Optional[Union[int, float]]:
        if value is None:
            return None
        return ast.literal_eval(value)

    def _to_binary(self, value: Optional[str]) -> Optional[bytes]:
        if value is None:
            return None
        return value

    def _to_string_set(self, value: Optional[List[str]]) -> Optional[Set[str]]:
        if value is None:
            return None
        return set([v for v in value])

    def _to_number_set(self, value: Optional[List[str]]) -> Optional[Set[float]]:
        if value is None:
            return None
        return set([float(v) for v in value])

    def _to_binary_set(self, value: Optional[List[str]]) -> Optional[Set[bytes]]:
        if value is None:
            return None
        return set([v for v in value])

    def _to_map(self, value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if value is None:
            return None

        converted_ = {}
        for k, v in value.items():
            type_, value_ = next(iter(v.items()))
            converted_[k] = self._mapping.get(type_, None)(value_)
        return converted_

    def _to_list(self, value: Optional[List[Any]]) -> Optional[List[Any]]:
        if value is None:
            return None

        coverted_ = []
        for v in value:
            type_, value_ = next(iter(v.items()))
            coverted_.append(self._mapping.get(type_, None)(value_))
        return coverted_

    def _to_null(self, value: Optional[bool]) -> Optional[bool]:
        return value

    def _to_bool(self, value: Optional[bool]) -> Optional[bool]:
        return value

    def _to_default(self, value: Optional[Any]) -> Optional[str]:
        return value

    def get(self, type_: str) -> Callable[[Optional[str]], Optional[Any]]:
        return self._mapping.get(type_, self._to_default)

    def get_default_converters(
        self,
    ) -> Dict[Any, Callable[[Optional[str]], Optional[Any]]]:
        return {
            "S": self._to_default,
            "N": self._to_number,
            "B": self._to_binary,
            "SS": self._to_string_set,
            "NS": self._to_number_set,
            "BS": self._to_binary_set,
            "M": self._to_map,
            "L": self._to_list,
            "NULL": self._to_null,
            "BOOL": self._to_bool,
        }


class Converter(metaclass=ABCMeta):
    def __init__(
        self,
        serializer: Serializer,
        deserializer: Deserializer,
    ) -> None:
        self._serializer = serializer
        self._deserializer = deserializer

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    @property
    def deserializer(self) -> Deserializer:
        return self._deserializer

    def get_serialize_converter(
        self, type_: str
    ) -> Callable[[Optional[str]], Optional[Any]]:
        return self._serializer.get(type_)

    def get_deserialize_converter(
        self, type_: str
    ) -> Callable[[Optional[str]], Optional[Any]]:
        return self._deserializer.get(type_)

    @abstractmethod
    def serialize(self, value: Optional[Any]) -> Optional[Any]:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def deserialize(self, value: Optional[Any]) -> Optional[Any]:
        raise NotImplementedError  # pragma: no cover


class DefaultTypeConverter(Converter):
    def __init__(self) -> None:
        super(DefaultTypeConverter, self).__init__(
            serializer=Serializer(),
            deserializer=Deserializer(),
        )

    def serialize(self, value: Optional[Any]) -> Optional[Any]:
        type_ = type(value)
        converter = self.get_serialize_converter(type_)
        return converter(value)

    def deserialize(self, value: Optional[Any]) -> Optional[Any]:
        type_, value_ = next(iter(value.items()))
        converter = self.get_deserialize_converter(type_)
        return converter(value_)
