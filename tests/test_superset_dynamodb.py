# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser
from pydynamodb.sql.common import QueryType
from pydynamodb.superset.dml_select import SupersetSelect

TESTCASE04_TABLE = "pydynamodb_test_case04"

class TestSupersetDynamoDB:
    def test_parse_nested_select(self):
        sql = """
            SELECT "col_list[1]", min("A"), max(A) FROM (
                SELECT col_list[1], col_map.A FROM Issues WHERE key_partition='row_1'
            ) AS virtual_table
            GROUP BY "col_list[1]","A"
            ORDER BY "AVG(A)" DESC
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert parser.query_type == QueryType.SELECT
        assert len(parser.parser.columns) == 2
        assert parser.parser.columns[0].request_name == "col_list[1]"
        assert parser.parser.columns[0].result_name == "col_list[1]"
        assert parser.parser.columns[1].request_name == "col_map.A"
        assert parser.parser.columns[1].result_name == "A"
        assert parser.parser.outer_columns == ['"col_list[1]"', 'min("A")', "max(A)"]
        assert parser.parser.outer_exprs == 'AS virtual_table GROUP BY "col_list[1]","A" ORDER BY "AVG(A)" DESC'
        assert ret == {
            "Statement": 'SELECT col_list[1],col_map.A FROM "Issues" WHERE key_partition = \'row_1\''
        }

    def test_insert_test_data(self, cursor):
        sql = """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_list': ?, 'col_map': ?
            }
        """ % TESTCASE04_TABLE
        params_1 = ["row_1", 1, [
                        "A", "B", {"A": 1, "B": 2}
                    ], {
                        "A": 1, "B": ["B-1", "B-2"]
                    }]
        params_2 = ["row_1", 2, [
                        "C", "D", {"C": 3, "D": 4}
                    ], {
                        "A": 2, "B": ["D-1", "D-2"]
                    }]
        params_3 = ["row_1", 3, [
                        "E", "F", {"E": 1, "F": 2}
                    ], {
                        "A": 3, "B": ["F-1", "F-2"]
                    }]
        cursor.executemany(sql, [params_1, params_2, params_3])

    def test_execute_nested_select(self, superset_cursor):
        superset_cursor.execute("""
            SELECT "col_list[1]", SUM("A") FROM (
                SELECT col_list[1], NUMBER(col_map.A) FROM %s WHERE key_partition='row_1'
            )
        """ % TESTCASE04_TABLE)
        ret = superset_cursor.fetchall()
        assert len(ret) == 1
        assert ret == [("B", 6.0)]
