# -*- coding: utf-8 -*-
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlalchemy as sa

from pydynamodb.connection import Connection
from pydynamodb.converter import DefaultTypeConverter
from pydynamodb.error import OperationalError
from pydynamodb.executor import DmlBatchExecutor
from pydynamodb.sqlalchemy_dynamodb.pydynamodb import DynamoDBDialect
from pydynamodb.superset_dynamodb.querydb import QueryDB, _to_sqlite

EXACT = Decimal("123456789012345678.12345678901234567890")


def test_numbers_round_trip_exactly():
    c = DefaultTypeConverter()
    assert c.serialize(EXACT) == {"N": "123456789012345678.12345678901234567890"}
    assert c.deserialize(c.serialize(EXACT)) == EXACT
    assert c.deserialize({"N": "9223372036854775807"}) == 2**63 - 1
    assert type(c.deserialize({"N": "10"})) is int
    assert c.serialize(1.25) == {"N": "1.25"}
    assert c.serialize({"k": EXACT, "l": [Decimal("0.1")]}) == {
        "M": {"k": {"N": str(EXACT)}, "l": {"L": [{"N": "0.1"}]}}
    }
    assert c.deserialize({"M": {"k": {"N": "0.1"}, "s": {"NS": ["1", "2.5"]}}}) == {
        "k": Decimal("0.1"),
        "s": {1, Decimal("2.5")},
    }
    assert c.serialize({Decimal("0.1")}) == {"NS": ["0.1"]}
    assert c.serialize(frozenset({"a"})) == {"SS": ["a"]}
    assert c.serialize(time(3, 4, 5)) == {"S": "03:04:05"}


@pytest.mark.parametrize(
    "value", [Decimal("1." + "1" * 38), Decimal("NaN"), Decimal("Infinity")]
)
def test_numbers_dynamodb_cannot_hold_raise(value):
    with pytest.raises((ValueError, ArithmeticError)):
        DefaultTypeConverter().serialize(value)


def processors(type_):
    dialect = DynamoDBDialect()
    impl = type_.dialect_impl(dialect)
    return impl.bind_processor(dialect), impl.result_processor(dialect, None)


def test_numeric_and_temporal_columns_keep_their_types():
    bind, result = processors(sa.Numeric(38, 20))
    assert bind is None and result(EXACT) is EXACT and result(3) == Decimal(3)
    _, result = processors(sa.Float())
    assert result(Decimal("1.25")) == 1.25 and result(None) is None
    _, result = processors(sa.DateTime(timezone=True))
    assert result("2024-01-02T03:04:05.678901+02:00") == datetime(
        2024, 1, 2, 3, 4, 5, 678901, timezone(timedelta(hours=2))
    )
    _, result = processors(sa.Date())
    assert result("2024-01-02") == date(2024, 1, 2)
    _, result = processors(sa.Time())
    assert result("03:04:05") == time(3, 4, 5)


def test_query_db_values_are_sqlite_bindable():
    assert _to_sqlite(Decimal("1.5")) == 1.5
    assert _to_sqlite(2**63) == float(2**63)
    assert _to_sqlite(2**63 - 1) == 2**63 - 1
    assert _to_sqlite(True) is True


class FakeClient:
    def __init__(self):
        self.calls = []

    def batch_execute_statement(self, Statements):
        self.calls.append(len(Statements))
        return {
            "Responses": [
                (
                    {"Error": {"Code": "DuplicateItem", "Message": "Duplicate"}}
                    if statement["Statement"] == "dup"
                    else {}
                )
                for statement in Statements
            ]
        }


def run_batch(statements):
    client = FakeClient()
    executor = SimpleNamespace(
        _statements=[SimpleNamespace(api_request={"Statement": s}) for s in statements],
        _errors=[],
        _rows=[],
        _next_token=None,
        connection=SimpleNamespace(client=client),
        _dispatch_api_call=lambda call, request: call(**request),
        post_execute=Mock(),
        BATCH_LIMIT=DmlBatchExecutor.BATCH_LIMIT,
    )
    executor.process_rows = lambda response: DmlBatchExecutor.process_rows(
        executor, response
    )
    DmlBatchExecutor.execute(executor)
    return client


def test_batch_respects_the_limit():
    assert run_batch(["ok"] * 60).calls == [25, 25, 10]


def test_batch_raises_on_a_failed_statement():
    with pytest.raises(OperationalError, match=r"1 of 61 .*#30 DuplicateItem.*#49"):
        run_batch(["ok"] * 30 + ["dup"] + ["ok"] * 30)


def test_closed_cursors_leave_the_pool():
    connection = Connection(
        region_name="us-east-1",
        aws_access_key_id="local",
        aws_secret_access_key="local",
        endpoint_url="http://127.0.0.1:9",
    )
    for _ in range(3):
        connection.cursor().close()
    open_cursor = connection.cursor()
    assert connection.cursor_pool == [open_cursor]
    connection.begin()
    connection.commit()  # nothing queued; no request is sent
    open_cursor.close()
    assert connection.cursor_pool == []


def test_query_db_cache_expiry_counts_whole_days():
    stale = datetime.now() - timedelta(days=1, seconds=10)
    query_db = SimpleNamespace(
        cache_enabled=True,
        config=SimpleNamespace(expire_time=300),
        has_table=lambda name: True,
        get_cache=lambda: ("statement", stale, stale),
    )
    assert QueryDB.has_cache(query_db) is False
