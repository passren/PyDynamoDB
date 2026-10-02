"""The advertised schema is synthetic and must not qualify PartiQL tables."""

import pytest
from sqlalchemy import Column, MetaData, String, Table, select
from pydynamodb.sqlalchemy_dynamodb.pydynamodb import (
    DynamoDBDialect,
    DynamoDBRestDialect,
)


def statement(table, operation):
    if operation == "select":
        return select(table.c.id).where(table.c.id == "seed")
    if operation == "insert":
        return table.insert().values(id="seed")
    if operation == "update":
        return table.update().where(table.c.id == "seed").values(id="changed")
    return table.delete().where(table.c.id == "seed")


@pytest.mark.parametrize("dialect_class", [DynamoDBDialect, DynamoDBRestDialect])
@pytest.mark.parametrize("operation", ["select", "insert", "update", "delete"])
@pytest.mark.parametrize("name", ["fixture", "user", "Mixed-Name", "table.with.dot"])
def test_synthetic_schema_compiles_like_unqualified_table(
    dialect_class, operation, name
):
    dialect = dialect_class()
    metadata = MetaData()
    qualified = Table(name, metadata, Column("id", String), schema="default")
    plain = Table(name, MetaData(), Column("id", String))
    actual = statement(qualified, operation).compile(dialect=dialect)
    expected = statement(plain, operation).compile(dialect=dialect)
    assert str(actual) == str(expected)
    assert actual.params == expected.params
    assert actual.positiontup == expected.positiontup
    assert qualified.schema == "default"
    assert metadata.tables[f"default.{name}"] is qualified


@pytest.mark.parametrize("schema", [None, "other", "Default"])
def test_non_synthetic_schemas_are_not_redirected(schema):
    table = Table("fixture", MetaData(), Column("id", String), schema=schema)
    dialect = DynamoDBDialect()
    sql = str(select(table).compile(dialect=dialect))
    expected_table = (
        "fixture"
        if schema is None
        else f"{dialect.identifier_preparer.quote_schema(schema)}.fixture"
    )
    assert f"FROM {expected_table}" in sql


def test_reflected_default_schema_select(engine):
    from tests.conftest import boto3_connect

    client = boto3_connect()
    name = "pydynamodb_test_case02"
    seed_keys = ("schema_seed", "schema_other")
    for key in seed_keys:
        client.put_item(
            TableName=name,
            Item={"key_partition": {"S": key}, "key_sort": {"N": "0"}},
        )
    try:
        engine, conn = engine
        schema = engine.dialect.get_schema_names(conn)[0]
        table = Table(name, MetaData(), schema=schema, autoload_with=engine)
        rows = conn.execute(
            select(table.c.key_partition).where(table.c.key_partition == "schema_seed")
        ).all()
        assert rows == [("schema_seed",)]
    finally:
        # This test shares TESTCASE02_TABLE with other test modules (see
        # test_sqlalchemy_dynamodb.py::test_reflect_table, which asserts an
        # exact row count). Remove the seeded rows so this test doesn't
        # leak state into others regardless of execution order.
        for key in seed_keys:
            client.delete_item(
                TableName=name,
                Key={"key_partition": {"S": key}, "key_sort": {"N": "0"}},
            )
