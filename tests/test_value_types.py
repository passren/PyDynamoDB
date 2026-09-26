# -*- coding: utf-8 -*-
from datetime import date, datetime
from decimal import Decimal

import pytest

from pydynamodb.executor import merge_value_type, value_type


@pytest.mark.parametrize(
    "values, expected",
    [
        ([None, None], None),
        ([1, Decimal("1.5"), None], "NUMBER"),
        ([True, False], "BOOL"),
        (["a", None], "STRING"),
        (["2024-01-01", "2024-01-02"], "DATE"),
        (["2024-01-01T10:00:00", "2024-01-02 11:00:00.123456789Z"], "DATETIME"),
        (["2024-01-01", "2024-01-02T03:04:05+02:00"], "DATETIME"),
        ([datetime(2024, 1, 1), date(2024, 1, 2)], "DATETIME"),
        (["2024-02-30"], "STRING"),
        (["2024-01-01T25:00:00"], "STRING"),
        (["2024-01-01", "x"], "STRING"),
        ([1, "1"], "STRING"),
        ([{"k": 1}], "STRING"),
        ([[1, 2]], "STRING"),
    ],
)
def test_merge_value_type(values, expected):
    seen = None
    for value in values:
        seen = merge_value_type(seen, value)
    assert seen == expected


def test_bool_is_not_a_number():
    assert value_type(True) == "BOOL"
    assert value_type(0) == "NUMBER"
