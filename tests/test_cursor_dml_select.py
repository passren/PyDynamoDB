# -*- coding: utf-8 -*-

TESTCASE03_TABLE = "pydynamodb_test_case03"


class TestCursorDMLSelect:
    def test_insert_nested_data(self, cursor):
        sql = """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_list': ?, 'col_map': ?
            }
        """ % TESTCASE03_TABLE
        params_1 = ["row_1", 1, [
                        "A", "B", {"A": 1, "B": 2}
                    ], {
                        "A": "A-1", "B": ["B-1", "B-2"]
                    }]
        params_2 = ["row_1", 2, [
                        "C", "D", {"C": 3, "D": 4}
                    ], {
                        "A": "B-1", "B": ["D-1", "D-2"]
                    }]
        params_3 = ["row_1", 3, [
                        "E", "F", {"E": 1, "F": 2}
                    ], {
                        "A": "C-1", "B": ["F-1", "F-2"]
                    }]
        cursor.executemany(sql, [params_1, params_2, params_3])

    def test_select_simple_columns(self, cursor):
        cursor.execute("SELECT * FROM %s WHERE key_partition='row_1'" % TESTCASE03_TABLE)
        ret = cursor.fetchall()
        assert len(ret) == 3

        cursor.execute("""
            SELECT col_list FROM %s WHERE key_partition='row_1'
        """ % TESTCASE03_TABLE)
        ret = cursor.fetchall()
        assert len(ret) == 3

        cursor.execute("""
            SELECT col_list FROM %s WHERE key_partition='row_1' AND key_sort=1
        """ % TESTCASE03_TABLE)
        ret = cursor.fetchall()
        assert len(ret) == 1

    def test_select_nested_columns(self, cursor):
        cursor.execute("""
            SELECT col_list[0] FROM %s WHERE key_partition='row_1'
        """ % TESTCASE03_TABLE)
        ret = cursor.fetchall()
        assert len(ret) == 3
        assert ret == [("A",), ("C",), ("E",)]

        cursor.execute("""
            SELECT col_list[1], col_map.A FROM %s WHERE key_partition='row_1'
        """ % TESTCASE03_TABLE)
        ret = cursor.fetchall()
        assert len(ret) == 3
        assert [d[0] for d in cursor.description] == ["col_list[1]", "A"]
        assert ret == [("B","A-1"), ("D","B-1"), ("F","C-1")]

    def test_nested_select_statement(self, cursor):
        cursor.execute("""
            SELECT col1, col2 FROM (
                SELECT col_list[1], col_map.A FROM %s WHERE key_partition='row_1'
            )
        """ % TESTCASE03_TABLE)
        ret = cursor.fetchall()
        assert len(ret) == 3
        assert [d[0] for d in cursor.description] == ["col1", "col2"]
        assert ret == [("B","A-1"), ("D","B-1"), ("F","C-1")]