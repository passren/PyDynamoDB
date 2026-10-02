# -*- coding: utf-8 -*-
"""Every statement parser must consume the whole statement.

Previously pyparsing stopped at the first unrecognized token and the rest of the
statement was silently ignored, so the driver could act on a different target
or with fewer conditions than written (e.g. ``DROP TABLE a b`` dropped ``a``).
"""

import pytest
from pyparsing import ParseException

from pydynamodb.sql.common import QueryType
from pydynamodb.sql.parser import SQLParser
from pydynamodb.superset_dynamodb.dml_select import SupersetSelect

STATEMENTS = {
    QueryType.CREATE: "CREATE TABLE Issues (IssueId numeric PARTITION KEY)",
    QueryType.CREATE_GLOBAL: (
        "CREATE GLOBAL TABLE Issues ReplicationGroup (us-east-1, us-west-2)"
    ),
    QueryType.ALTER: (
        "ALTER TABLE Issues (IssueId numeric, DueDate string, "
        "UPDATE INDEX DueDateIndex GLOBAL "
        "ProvisionedThroughput.ReadCapacityUnits 10) "
        "BillingMode PAY_PER_REQUEST"
    ),
    QueryType.DROP: "DROP TABLE Issues",
    QueryType.DROP_GLOBAL: (
        "DROP GLOBAL TABLE Issues ReplicationGroup (us-east-1, us-west-2)"
    ),
    QueryType.INSERT: "INSERT INTO Issues VALUE {'IssueId': 1, 'Title': 'x'}",
    QueryType.UPDATE: "UPDATE Issues SET Title='y' WHERE IssueId=1",
    QueryType.DELETE: "DELETE FROM Issues WHERE IssueId=1",
    QueryType.SELECT: "SELECT IssueId FROM Issues WHERE IssueId=1",
    QueryType.LIST: "LIST TABLES",
    QueryType.LIST_GLOBAL: "LIST GLOBAL TABLES",
    QueryType.DESC: "DESC Issues",
    QueryType.DESC_GLOBAL: "DESC GLOBAL Issues",
}
CASES = list(STATEMENTS.items())
IDS = [query_type[1] + "_" + query_type[2] for query_type, _ in CASES]


@pytest.mark.parametrize("query_type, sql", CASES, ids=IDS)
def test_complete_statement_parses(query_type, sql):
    parser = SQLParser(sql)
    assert parser.query_type == query_type
    expected = parser.transform()
    for variant in (sql + ";", "\n  " + sql + " ;\n"):
        assert SQLParser(variant).transform() == expected


@pytest.mark.parametrize("query_type, sql", CASES, ids=IDS)
@pytest.mark.parametrize(
    "suffix", [" Unexpected", " Unexpected Tokens", "; DROP TABLE Other", ";;"]
)
def test_trailing_tokens_are_rejected(query_type, sql, suffix):
    with pytest.raises(ParseException):
        SQLParser(sql + suffix).transform()


def test_superset_select_rejects_a_second_statement():
    # With connector=superset a clause PartiQL cannot express is evaluated by
    # the query DB (see test_superset_dml_select), but never a second statement.
    sql = "SELECT IssueId FROM Issues WHERE IssueId=1"
    assert SQLParser(sql, parser_class=SupersetSelect).transform()
    for suffix in ("; DROP TABLE Other", ";;"):
        with pytest.raises(ParseException):
            SQLParser(sql + suffix, parser_class=SupersetSelect).transform()


@pytest.mark.parametrize(
    "item",
    [
        "{'a': {'b': '1', 'c': {'d': 2}}, 'e': 3}",
        "{'a': 'brace } inside', 'b': 'quote '' and { brace'}",
        '{"a": {"b": 1}}',
    ],
)
def test_insert_keeps_nested_items_intact(item):
    ret = SQLParser("INSERT INTO Issues VALUE %s" % item).transform()
    assert ret == {"Statement": 'INSERT INTO "Issues" VALUE %s' % item}


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO Issues VALUE {'a': {'b': 1}",
        "INSERT INTO Issues VALUE {'a': 1}}",
    ],
)
def test_insert_rejects_unbalanced_items(sql):
    with pytest.raises(ParseException):
        SQLParser(sql).transform()
