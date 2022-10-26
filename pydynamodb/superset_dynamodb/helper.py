# -*- coding: utf-8 -*-
import os
import logging
from abc import ABCMeta
from typing import Union

from .model import (
    QueryDBConfig,
    QueryDB,
    SUPPORTED_QUERYDB_CONFIG,
    DEFAULT_QUERYDB_TYPE,
    DEFAULT_QUERYDB_URL,
    DEFAULT_QUERYDB_LOAD_BATCH_SIZE,
    DEFAULT_QUERYDB_EXPIRE_TIME,
)
from ..model import Statement
from ..error import NotSupportedError
from .querydb_sqlite import SqliteMemQueryDB, SqliteFileQueryDB

_logger = logging.getLogger(__name__)  # type: ignore


class QueryDBHelper(metaclass=ABCMeta):
    @staticmethod
    def create(statement: Statement, config: QueryDBConfig = None, **kwargs) -> QueryDB:
        if config is None:
            _config = QueryDBHelper.get_default_config(**kwargs)
        else:
            _config = config

        _query_db = None
        if _config.db_type.lower() == DEFAULT_QUERYDB_TYPE:
            if _config.db_url.lower() == DEFAULT_QUERYDB_URL:
                _query_db = SqliteMemQueryDB(statement, _config, **kwargs)
            else:
                _query_db = SqliteFileQueryDB(statement, _config, **kwargs)

        if _query_db is None:
            raise NotSupportedError
        else:
            return _query_db

    @staticmethod
    def get_default_config(**kwargs):
        db_type = QueryDBHelper.get_config_value("querydb_type", **kwargs)
        db_url = QueryDBHelper.get_config_value("querydb_url", **kwargs)
        load_batch_size = QueryDBHelper.get_config_value(
            "querydb_load_batch_size", **kwargs
        )
        expire_time = QueryDBHelper.get_config_value("querydb_expire_time", **kwargs)

        _db_type = db_type if db_type is not None else DEFAULT_QUERYDB_TYPE
        _db_url = db_url if db_url is not None else DEFAULT_QUERYDB_URL
        _load_batch_size = (
            int(load_batch_size)
            if load_batch_size is not None
            else DEFAULT_QUERYDB_LOAD_BATCH_SIZE
        )
        _expire_time = (
            int(expire_time) if expire_time is not None else DEFAULT_QUERYDB_EXPIRE_TIME
        )

        return QueryDBConfig(_db_type, _db_url, _load_batch_size, _expire_time)

    @staticmethod
    def get_config_value(config_name: str, **kwargs) -> Union[str, int]:
        if config_name in kwargs:
            return kwargs[config_name]
        else:
            if config_name in SUPPORTED_QUERYDB_CONFIG:
                (env_name, default_val) = SUPPORTED_QUERYDB_CONFIG[config_name]
                env_val = os.getenv(env_name, None)
                if env_val is None:
                    return default_val
                else:
                    return env_val
            else:
                return None
