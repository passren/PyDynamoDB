# -*- coding: utf-8 -*-
import logging
import sqlite3
from datetime import datetime, date
from contextlib import closing
from typing import Any, Type

from .model import QueryDB, QueryDBConfig
from ..model import Statement

_logger = logging.getLogger(__name__)  # type: ignore


class SqliteFileQueryDB(QueryDB):
    def __init__(
        self,
        statement: Statement,
        config: QueryDBConfig,
        **kwargs,
    ) -> None:
        super().__init__(statement, config, **kwargs)
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = sqlite3.connect(
                self.config.db_url,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )

        return self._connection

    def type_conversion(self, type: Type[Any]) -> str:
        type_mapping = {
            str: "TEXT",
            int: "INTEGER",
            float: "REAL",
            bytes: "TEXT",
            bool: "INTEGER",
            datetime: "TIMESTAMP",
            date: "DATE",
        }

        return type_mapping.get(type, "TEXT")

    def has_table(self, table: str) -> bool:
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(
                "SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            result = cursor.fetchone()
            if result and result == (1,):
                return True

        return False


class SqliteMemQueryDB(SqliteFileQueryDB):
    def __init__(
        self,
        statement: Statement,
        config: QueryDBConfig,
        **kwargs,
    ) -> None:
        super().__init__(statement, config, **kwargs)

    def has_cache(self) -> bool:
        return False

    def init_cache_table(self) -> None:
        """Does nothing with memory model"""
        pass

    def add_cache(self) -> None:
        """Does nothing with memory model"""
        pass

    def set_cache_last_updated(self) -> None:
        """Does nothing with memory model"""
        pass

    def set_cache_queried_times(self) -> None:
        """Does nothing with memory model"""
        pass
