# -*- coding: utf-8 -*-
from unittest.mock import Mock
from types import SimpleNamespace

import pytest
from pyparsing import ParseException

from pydynamodb.cursor import Cursor
from pydynamodb.error import NotSupportedError
from pydynamodb.sql.parser import SQLParser
from pydynamodb.sql.rewrite import strip_sql_comments, unalias_select
from pydynamodb.superset_dynamodb.dml_select import SupersetSelect
from pydynamodb.superset_dynamodb.pydynamodb import SupersetCursor

TAG = "\n-- tracking: abc\n-- 6dcd92a04feb50f14bbcf07c661680ba"

STATEMENTS = [
    "CREATE TABLE Issues (IssueId numeric PARTITION KEY)",
    "DROP TABLE Issues",
    "INSERT INTO Issues VALUE {'IssueId': 1, 'Title': 'x'}",
    "UPDATE Issues SET Title='y' WHERE IssueId=1",
    "DELETE FROM Issues WHERE IssueId=1",
    "SELECT IssueId FROM Issues WHERE IssueId=1",
    "LIST TABLES",
    "DESC Issues",
]


def transform(sql, parser_class=None):
    return SQLParser(sql, parser_class=parser_class).transform()


@pytest.mark.parametrize("sql", STATEMENTS)
def test_trailing_comments_are_not_statement_tokens(sql):
    assert transform(sql + TAG) == transform(sql)
    assert transform(sql + " /* note */;") == transform(sql)


def test_superset_select_with_trailing_comment_stays_partiql():
    sql = "SELECT id FROM t WHERE id = 'x'"
    parser = SQLParser(sql + TAG, parser_class=SupersetSelect)
    assert parser.transform() == transform(sql)
    assert not parser.parser.is_flat


def test_comment_markers_inside_quotes_are_kept():
    sql = "SELECT id FROM t WHERE txt = 'a -- b /* c */' -- tail"
    assert strip_sql_comments(sql) == "SELECT id FROM t WHERE txt = 'a -- b /* c */'  "
    assert "'a -- b /* c */'" in transform(sql)["Statement"]


@pytest.mark.parametrize(
    "clause, expected",
    [
        ("ORDER BY id", "ORDER BY id"),
        ("ORDER BY id DESC, n", "ORDER BY id DESC, n"),
        ("ORDER BY id ASC", "ORDER BY id ASC"),
    ],
)
def test_order_by_keys_with_optional_direction(clause, expected):
    sql = f"SELECT id FROM t WHERE id = 'x' {clause}"
    assert (
        transform(sql)["Statement"] == f"SELECT id FROM \"t\" WHERE id = 'x' {expected}"
    )
    assert transform(sql, SupersetSelect) == transform(sql)


@pytest.mark.parametrize(
    "sql, expected",
    [
        (
            "SELECT a.id, a.txt FROM t AS a WHERE a.id = 'x'",
            "SELECT id, txt FROM t WHERE id = 'x'",
        ),
        ("SELECT A.id FROM t a WHERE a.n > 1", "SELECT id FROM t WHERE n > 1"),
        ("SELECT a.* FROM t AS a", "SELECT * FROM t"),
        (
            'SELECT "q".id, q.m.k FROM "t"."idx" AS "q" WHERE "q".id = ?',
            'SELECT id, m.k FROM "t"."idx" WHERE id = ?',
        ),
        (
            "SELECT a.id FROM t AS a WHERE a.txt = 'a.b -- x' AND b.a = 1",
            "SELECT id FROM t WHERE txt = 'a.b -- x' AND b.a = 1",
        ),
    ],
)
def test_single_table_alias_is_resolved(sql, expected):
    assert unalias_select(sql) == expected


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM t WHERE id = 'x'",
        "SELECT id FROM t",
        "SELECT a.id FROM t a JOIN u b ON a.id = b.id",
        "SELECT a.id FROM t a, u b",
        "SELECT id FROM (SELECT id FROM t) AS v",
        "DELETE FROM t a WHERE a.id = 'x'",
    ],
)
def test_other_statements_are_not_rewritten(sql):
    assert unalias_select(sql) is None


def test_default_cursor_runs_an_aliased_select_as_partiql():
    cursor = SimpleNamespace(execute_statement=Mock(return_value="ran"))
    assert Cursor.execute(cursor, "SELECT a.id FROM t AS a WHERE a.id = 'x'" + TAG)
    statement = cursor.execute_statement.call_args.args[0]
    assert statement.api_request == {"Statement": "SELECT id FROM \"t\" WHERE id = 'x'"}
    with pytest.raises(NotSupportedError, match="connector=superset"):
        Cursor.execute(cursor, "SELECT id FROM (SELECT id FROM t) AS v")
    with pytest.raises(ParseException):
        Cursor.execute(cursor, "DROP TABLE t extra")


def test_superset_cursor_runs_an_aliased_select_with_parameters():
    cursor = SimpleNamespace(execute_statement=Mock(return_value="ran"))
    sql = "SELECT a.id FROM t AS a WHERE a.id = ?"
    assert SupersetCursor.execute(cursor, sql, [{"S": "x"}]) == "ran"
    statement = cursor.execute_statement.call_args.args[0]
    assert not statement.sql_parser.parser.is_flat
    assert statement.api_request == {"Statement": 'SELECT id FROM "t" WHERE id = ?'}
