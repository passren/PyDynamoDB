# -*- coding: utf-8 -*-
import functools
import logging
import re
import threading
from typing import Any, Callable, Iterable, Tuple

import tenacity
from tenacity import after_log, retry_if_exception, stop_after_attempt, wait_exponential

_logger = logging.getLogger(__name__)  # type: ignore


def parse_limit_expression(statement: str) -> Tuple[str, int]:
    pattern_limit_ = re.compile(r"\w*(LIMIT)\s*(\d+)\w*", re.IGNORECASE)
    match_ = pattern_limit_.search(statement)
    if match_:
        limit_exp_ = match_.group()
        limit_groups_ = match_.groups()
        if len(limit_groups_) == 2 and limit_groups_[0].upper() == "LIMIT":

            return (statement.replace(limit_exp_, ""), int(limit_groups_[1]))
    return statement, None


def synchronized(wrapped: Callable[..., Any]) -> Any:
    """The missing @synchronized decorator

    https://git.io/vydTA"""
    _lock = threading.RLock()

    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        with _lock:
            return wrapped(*args, **kwargs)

    return _wrapper


class RetryConfig:
    def __init__(
        self,
        exceptions: Iterable[str] = (
            "ThrottlingException",
            "TooManyRequestsException",
        ),
        attempt: int = 5,
        multiplier: int = 1,
        max_delay: int = 100,
        exponential_base: int = 2,
    ) -> None:
        self.exceptions = exceptions
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def retry_api_call(
    func: Callable[..., Any],
    config: RetryConfig,
    logger: logging.Logger = None,
    *args,
    **kwargs,
) -> Any:
    retry = tenacity.Retrying(
        retry=retry_if_exception(
            lambda e: getattr(e, "response", {}).get("Error", {}).get("Code", None)
            in config.exceptions
            if e
            else False
        ),
        stop=stop_after_attempt(config.attempt),
        wait=wait_exponential(
            multiplier=config.multiplier,
            max=config.max_delay,
            exp_base=config.exponential_base,
        ),
        after=after_log(logger, logger.level) if logger else None,  # type: ignore
        reraise=True,
    )
    return retry(func, *args, **kwargs)
