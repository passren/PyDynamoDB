# -*- coding: utf-8 -*-
import logging
from .base import Base
from pyparsing import Forward
from typing import Any, Dict, Optional

_logger = logging.getLogger(__name__)  # type: ignore

class DmlBase(Base):
    def __init__(self, statement: str) -> None:
        self._statement = statement

    @property
    def syntex_def(self) -> Forward:
        return None

    def transform(self) -> Dict[str, Any]:
        return {
            "Statement": self._statement
        }