# -*- coding: utf-8 -*-
import logging
import re
from typing import Tuple

_logger = logging.getLogger(__name__)  # type: ignore


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to True or False.

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


def parse_limit_expression(statement: str) -> Tuple[str, int]:
    pattern_limit_ = re.compile(r"\w*(LIMIT)\s*(\d+)\w*", re.IGNORECASE)
    match_ = pattern_limit_.search(statement)
    if match_:
        limit_exp_ = match_.group()
        limit_groups_ = match_.groups()
        if len(limit_groups_) == 2 and limit_groups_[0].upper() == "LIMIT":

            return (statement.replace(limit_exp_, ""), int(limit_groups_[1]))
    return statement, None
