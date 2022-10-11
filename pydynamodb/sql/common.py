# -*- coding: utf-8 -*-
import re
from enum import Enum
from .util import strtobool
from pyparsing import (Word, nums, alphanums, CaselessKeyword,
                        Opt, one_of, Group, Suppress,
                        pyparsing_common as ppc)

class KeyWords():
    # Common sign definition
    LPAR, RPAR, COMMA, SEMICOLON, COLON, DOT, SINGLEQUOTE, \
        DOUBLEQUOTE, BACKQUOTE, EQUALS, SPACE = map(
        Suppress, "(),;:.'\"`= "
    )
    SUPPRESS_QUOTE = (BACKQUOTE | DOUBLEQUOTE).set_name("quote")

    # Common key word definition
    (
        CREATE, ALTER, DROP, TABLE, GLOBAL,
        PARTITION_KEY, SORT_KEY, HASH, RANGE,
        UPDATE, DELETE, INDEX, REPLICA 
    ) = map(
        CaselessKeyword,
        [
            "CREATE", "ALTER", "DROP", "TABLE", "GLOBAL",
            "PARTITION KEY", "SORT KEY", "HASH", "RANGE",
            "UPDATE", "DELETE", "INDEX", "REPLICA"
        ],
    )

class Tokens():
    # Common token definition
    TABLE_NAME = (
        Opt(KeyWords.SUPPRESS_QUOTE)
        + Word(alphanums + "_<>")("table").set_name("table")
        + Opt(KeyWords.SUPPRESS_QUOTE)
    )
    ATTRIBUTE_NAME = (
        Opt(KeyWords.SUPPRESS_QUOTE)
        + Word(alphanums + "_")("attribute_name").set_name("attribute_name")
        + Opt(KeyWords.SUPPRESS_QUOTE)
    )
    DATA_TYPE = one_of(
        "NUMERIC STRING BINARY", caseless=True
    )("data_type").set_name("data_type")
    OPT_KEY_TYPE = one_of([
        "PARTITION KEY", "SORT KEY", "HASH", "RANGE", ""
    ])("key_type").set_name("key_type")
    KEY_TYPE = one_of([
        "PARTITION KEY", "SORT KEY", "HASH", "RANGE"
    ])("key_type").set_name("key_type")
    INDEX_NAME = Word(alphanums + "_")("index_name").set_name("index_name")
    INDEX_TYPE = one_of("GLOBAL LOCAL", caseless=True)("index_type").set_name("index_type")
    TAG = Group(
        Opt(KeyWords.SINGLEQUOTE)
            + Word(alphanums + "._$")("tag_key").set_name("tag_key")
        + Opt(KeyWords.SINGLEQUOTE)
        + ":"
        + Opt(KeyWords.SINGLEQUOTE)
            + Word(alphanums + "._$")("tag_value").set_name("tag_value")
        + Opt(KeyWords.SINGLEQUOTE)
    )("tag").set_name("tag")
    REGION_NAME = Word(alphanums + "-")("region_name").set_name("region_name")
    KMSMASTERKEYID = Word(alphanums + "+-=*_$")
    INT_VALUE = Word(nums).set_parse_action(ppc.convert_to_integer)
    BOOL_VALUE = one_of("True False", caseless=True).set_parse_action(lambda toks: strtobool(toks[0]))

class QueryType(str, Enum):
    """
    Types of supported queries
    """
    CREATE = "CREATE TABLE"
    ALTER = "ALTER TABLE"
    DROP = "DROP TABLE"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SELECT = "SELECT"

SUPPORTED_QUERY_TYPES = {
    r"^\s*(CREATE)\s+(TABLE).*": QueryType.CREATE,
    r"^\s*(ALTER)\s+(TABLE).*": QueryType.ALTER,
    r"^\s*(DROP)\s+(TABLE).*": QueryType.DROP,
    r"^\s*(INSERT).*": QueryType.INSERT,
    r"^\s*(UPDATE).*": QueryType.UPDATE,
    r"^\s*(SELECT).*": QueryType.SELECT,
    r"^\s*(DELETE).*": QueryType.DELETE,
}

def get_query_type(sql: str) -> QueryType:
    for type_pattern, type in SUPPORTED_QUERY_TYPES.items():
        pattern_ = re.compile(type_pattern, re.IGNORECASE)
        match_ = pattern_.search(sql)

        if match_:
            return type
    
    raise ValueError("Not supported query type")