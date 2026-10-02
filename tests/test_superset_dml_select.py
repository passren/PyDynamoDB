# -*- coding: utf-8 -*-
import pytest
from pyparsing import ParseException

from pydynamodb.sql.parser import SQLParser
from pydynamodb.sql.common import QueryType
from pydynamodb.superset_dynamodb.dml_select import SupersetSelect


class TestSupersetDmlSelect:
    def test_parse_base_select(self):
        sql = """
            SELECT col_list[1], col_map.A FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert parser.query_type == QueryType.SELECT
        assert len(parser.parser.columns) == 2
        assert ret == {
            "Statement": "SELECT col_list[1],col_map.A FROM \"Issues\" WHERE key_partition = 'row_1'"
        }
        assert parser.parser.inner_columns == None
        assert parser.parser.inner_exprs == None
        assert parser.parser.outer_columns == None
        assert parser.parser.outer_exprs == None

    def test_parse_nested_select_case_1(self):
        sql = """
            SELECT "col_list[1]", min("A"), max(A) FROM (
                SELECT col_list[1], col_map.A FROM Issues WHERE key_partition='row_1'
            ) AS virtual_table
            GROUP BY "col_list[1]","A"
            ORDER BY "AVG(A)" DESC
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        CLOUMN_LIST_1 = "col_list[1]"
        assert parser.query_type == QueryType.SELECT
        assert len(parser.parser.columns) == 2
        assert parser.parser.columns[0].request_name == CLOUMN_LIST_1
        assert parser.parser.columns[0].result_name == CLOUMN_LIST_1
        assert parser.parser.columns[1].request_name == "col_map.A"
        assert parser.parser.columns[1].result_name == "A"
        assert parser.parser.outer_columns == '"col_list[1]", min("A"), max(A)'
        assert parser.parser.outer_exprs == """AS virtual_table
            GROUP BY "col_list[1]","A"
            ORDER BY "AVG(A)" DESC"""
        assert ret == {
            "Statement": "SELECT col_list[1],col_map.A FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_2(self):
        sql = """
            SELECT DATETIME("col_datetime", 'start of day'),
                "col_str", max("col_num"), count(DISTINCT "col_num"),
                count("col_num"), min("col_num"), sum("col_num")
            FROM
            (
                SELECT col_str, DATETIME(col_datetime), NUMBER(col_num)
                FROM Issues WHERE key_partition='row_1'
            ) AS virtual_table
            GROUP BY "col_str", DATETIME("col_datetime", 'start of day')
            ORDER BY "MAX(col_num)" DESC LIMIT 10000
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert len(parser.parser.columns) == 3
        assert (
            parser.parser.outer_columns == """DATETIME("col_datetime", 'start of day'),
                "col_str", max("col_num"), count(DISTINCT "col_num"),
                count("col_num"), min("col_num"), sum("col_num")"""
        )
        assert parser.parser.outer_exprs == """AS virtual_table
            GROUP BY "col_str", DATETIME("col_datetime", 'start of day')
            ORDER BY "MAX(col_num)" DESC LIMIT 10000"""
        assert ret == {
            "Statement": "SELECT col_str,col_datetime,col_num "
            + "FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_3(self):
        sql = """
            SELECT col_str_1, col_str_2, col_datetime, col_num FROM (
                SELECT SUBSTR(col_str, 1, 2) col_str_1, REPLACE(col_str, '-', '_') col_str_2,
                    DATETIME(col_datetime), NUMBER(col_num)
                FROM Issues WHERE key_partition='row_1'
            ) GROUP BY col_str_1
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert len(parser.parser.columns) == 4
        assert ret == {
            "Statement": "SELECT col_str,col_datetime,col_num "
            + "FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_4(self):
        sql = """
            SELECT col_str_1, col_str_2, col_datetime, col_num FROM (
                SELECT SUBSTR(col_str, 1, 2) col_str_1, REPLACE(col_str, '-', '_') col_str_2,
                    col_datetime, col_num
                FROM (
                    SELECT col_str, DATETIME(col_datetime), NUMBER(col_num)
                    FROM Issues WHERE key_partition='row_1'
                ) WHERE col_num > 100 AND col_num < 200
            ) AS virtual_table
            GROUP BY col_str_1
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert len(parser.parser.columns) == 3
        assert (
            parser.parser.outer_columns == "col_str_1, col_str_2, col_datetime, col_num"
        )
        assert parser.parser.outer_exprs == """AS virtual_table
            GROUP BY col_str_1"""
        assert (
            parser.parser.inner_columns
            == """SUBSTR(col_str, 1, 2) col_str_1, REPLACE(col_str, '-', '_') col_str_2,
                    col_datetime, col_num"""
        )
        assert parser.parser.inner_exprs == "WHERE col_num > 100 AND col_num < 200"
        assert ret == {
            "Statement": "SELECT col_str,col_datetime,col_num "
            + "FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_4(self):
        sql = """
        SELECT text_split(id,'.',1) FROM (
            SELECT id FROM "lakefront-ingest-stg-config-table"
        )
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert ret == {
            "Statement": 'SELECT id FROM "lakefront-ingest-stg-config-table"'
        }
        assert parser.parser.inner_columns == None
        assert parser.parser.inner_exprs == None
        assert parser.parser.outer_columns == "text_split(id,'.',1)"
        assert parser.parser.outer_exprs == ""

    def test_parse_nested_select_case_5(self):
        sql = """
        SELECT id FROM (
            SELECT text_split(id,'.',1) FROM (
                SELECT id FROM "lakefront-ingest-stg-config-table"
            )
        ) AS virtual_table
        GROUP BY id
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert ret == {
            "Statement": 'SELECT id FROM "lakefront-ingest-stg-config-table"'
        }
        assert parser.parser.inner_columns == "text_split(id,'.',1)"
        assert parser.parser.inner_exprs == ""
        assert parser.parser.outer_columns == "id"
        assert parser.parser.outer_exprs == "AS virtual_table\n        GROUP BY id"

    def test_parse_aliased_select_is_evaluated_in_query_db(self):
        # A base SELECT used to lose the alias and everything after it,
        # including the WHERE. It is now scanned and evaluated as a whole.
        sql = "SELECT id FROM Issues AS aliased WHERE key_partition='row_1'"
        parser = SQLParser(sql, parser_class=SupersetSelect)
        assert parser.transform() == {"Statement": 'SELECT * FROM "Issues"'}
        assert parser.parser.is_flat and parser.parser.is_nested
        assert parser.parser.outer_columns == "id"
        assert parser.parser.outer_exprs == "AS aliased WHERE key_partition='row_1'"

    @pytest.mark.parametrize(
        "sql, outer_columns, outer_exprs",
        [
            (
                "SELECT txt, COUNT(*) \nFROM Issues \nWHERE id = 'a' "
                "GROUP BY txt LIMIT 100;",
                "txt, COUNT(*)",
                "WHERE id = 'a' GROUP BY txt LIMIT 100",
            ),
            (
                'SELECT SUM("n") AS "SUM(n)" FROM "Issues"."Idx" GROUP BY k',
                'SUM("n") AS "SUM(n)"',
                "GROUP BY k",
            ),
        ],
    )
    def test_parse_flat_aggregate_select(self, sql, outer_columns, outer_exprs):
        parser = SQLParser(sql, parser_class=SupersetSelect)
        request = parser.transform()["Statement"]
        assert request.startswith('SELECT * FROM "Issues"')
        assert parser.parser.is_flat
        assert parser.parser.outer_columns == outer_columns
        assert parser.parser.outer_exprs == outer_exprs

    def test_parse_base_select_is_unchanged(self):
        sql = "SELECT id FROM Issues WHERE key_partition='row_1' LIMIT 5"
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert (
            ret["Statement"]
            == "SELECT id FROM \"Issues\" WHERE key_partition = 'row_1'"
        )
        assert not parser.parser.is_flat and not parser.parser.is_nested

    def test_parse_rejects_second_statement(self):
        sql = "SELECT id FROM Issues WHERE id = 'a'; DELETE FROM Issues"
        with pytest.raises(ParseException):
            SQLParser(sql, parser_class=SupersetSelect).transform()

    def test_parse_nested_select_keeps_outer_alias_and_predicate(self):
        sql = """
        SELECT id FROM (SELECT id FROM Issues) AS virtual_table WHERE id = 'a'
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert ret == {"Statement": 'SELECT id FROM "Issues"'}
        assert parser.parser.outer_exprs == "AS virtual_table WHERE id = 'a'"
