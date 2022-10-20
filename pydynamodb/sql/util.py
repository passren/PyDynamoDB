# -*- coding: utf-8 -*-
import logging
from typing import Any, List

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


def flatten_list(lst: List[Any]) -> List[Any]:
    items = list()
    for item in lst:
        if isinstance(item, list):
            if len(item) > 2 and item[0] == "[" and item[-1] == "]":
                items.append(
                    "[%s]" % (",".join(str(item[i]) for i in range(1, len(item) - 1)))
                )
                return items

            items.extend(flatten_list(item))
        else:
            items.append(item)
    return items
