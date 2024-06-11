# -*- coding: utf-8 -*-

TESTCASE03_TABLE = "pydynamodb_test_case03"


class TestCursorDMLSelect:
    def test_insert_nested_data(self, cursor):
        sql = (
            """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_list': ?, 'col_map': ?
            }
        """
            % TESTCASE03_TABLE
        )
        params_1 = [
            "row_1",
            1,
            "Services that require ad hoc query access",
            ["A", "B", {"A": 1, "B": 2}],
            {"A": "A-1", "B": ["B-1", "B-2"]},
        ]
        params_2 = [
            "row_1",
            2,
            "Online analytical processing (OLAP)/data warehouse implementations",
            ["C", "D", {"C": 3, "D": 4}],
            {"A": "B-1", "B": ["D-1", "D-2"]},
        ]
        params_3 = [
            "row_1",
            3,
            "Binary large object (BLOB) storage",
            ["E", "F", {"E": 1, "F": 2}],
            {"A": "C-1", "B": ["F-1", "F-2"]},
        ]
        cursor.executemany(sql, [params_1, params_2, params_3])

        sql = (
            """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_obj': ?
            }
        """
            % TESTCASE03_TABLE
        )
        params_4 = ["row_2", 1, "Diagnostics can be enabled", {"A": "B"}]
        params_5 = ["row_2", 2, ["A", "B"], None]
        params_6 = ["row_2", 3, "Start the application that you downloaded", ["1", "2"]]
        cursor.executemany(sql, [params_4, params_5, params_6])

        sql = (
            """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'size': ?, 'col_map': ?
            }
        """
            % TESTCASE03_TABLE
        )
        params_7 = [
            "row_3",
            0,
            [100, 300, 500],
            {"all": "true", "max": ["text", "json"]},
        ]
        params_8 = [
            "row_3",
            1,
            [500, 800, 1000, 1500],
            {"ALL": "false", "max": ["image", "blob"]},
        ]

        cursor.executemany(sql, [params_7, params_8])

    def test_select_simple_columns(self, cursor):
        cursor.execute(
            "SELECT * FROM %s WHERE key_partition='row_1'" % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 3

        cursor.execute(
            """
            SELECT col_list FROM %s WHERE key_partition='row_1'
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 3

        cursor.execute(
            """
            SELECT col_list FROM %s WHERE key_partition='row_1' AND key_sort=1
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 1

    def test_select_nested_columns(self, cursor):
        cursor.execute(
            """
            SELECT col_list[0] FROM %s WHERE key_partition='row_1'
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 3
        assert ret == [("A",), ("C",), ("E",)]

        cursor.execute(
            """
            SELECT col_list[1], col_map.A FROM %s WHERE key_partition='row_1'
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 3
        assert [d[0] for d in cursor.description] == ["col_list[1]", "A"]
        assert ret == [("B", "A-1"), ("D", "B-1"), ("F", "C-1")]

    def test_reserved_word(self, cursor):
        try:
            cursor.execute(
                """
                SELECT size FROM %s WHERE key_partition='row_3'
            """
                % TESTCASE03_TABLE
            )
        except Exception as e:
            assert "Statement wasn't well formed" in str(e)

        cursor.execute(
            """
            SELECT "size" FROM %s WHERE key_partition='row_3'
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 2
        assert ret == [([100, 300, 500],), ([500, 800, 1000, 1500],)]

        cursor.execute(
            """
            SELECT "size"[0] FROM %s WHERE key_partition='row_3'
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 2
        assert cursor.description == [
            ("size[0]", "STRING", None, None, None, None, None),
        ]
        assert ret == [(100,), (500,)]

        cursor.execute(
            """
            SELECT col_map."all", col_map."ALL" FROM %s WHERE key_partition='row_3'
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 2
        assert cursor.description == [
            ("all", "STRING", None, None, None, None, None),
            ("ALL", "STRING", None, None, None, None, None),
        ]
        assert ret == [("true", None), (None, "false")]

        cursor.execute(
            """
            SELECT col_map."max"[1] FROM %s WHERE key_partition='row_3' and key_sort=1
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 1
        assert cursor.description == [
            ("max[1]", "STRING", None, None, None, None, None),
        ]
        assert ret == [("blob",)]

    def test_function_in_columns(self, cursor):
        from datetime import date, datetime

        sql_date_row_1_0_ = (
            """
        INSERT INTO %s VALUE {
            'key_partition': ?, 'key_sort': ?, 'col_date': ?, 'col_datetime': ?, 'col_str': ?
        }
        """
            % TESTCASE03_TABLE
        )
        params_1_0_ = [
            "test_date_row_1",
            0,
            date(2022, 10, 18),
            datetime(2022, 10, 18, 13, 55, 34),
            "abcdEFG01234",
        ]
        params_1_1_ = [
            "test_date_row_1",
            1,
            date(2022, 10, 19),
            datetime(2022, 10, 19, 17, 2, 4),
            "01234abcdefg",
        ]
        cursor.executemany(sql_date_row_1_0_, [params_1_0_, params_1_1_])
        cursor.execute(
            """
            SELECT DATE(col_date, '%Y-%m-%d'), DATETIME(col_datetime), SUBSTR(col_str, 2, 3) FROM {0}
            WHERE key_partition = ? AND key_sort = ?
        """.format(
                TESTCASE03_TABLE
            ),
            ["test_date_row_1", 0],
        )
        assert cursor.fetchone() == (
            date(2022, 10, 18),
            datetime(2022, 10, 18, 13, 55, 34),
            "cdE",
        )

        cursor.execute(
            """
            SELECT REPLACE(col_str, 'd', 'XX') FROM {0}
            WHERE key_partition = ? AND key_sort = ?
        """.format(
                TESTCASE03_TABLE
            ),
            ["test_date_row_1", 0],
        )
        assert cursor.fetchone() == ("abcXXEFG01234",)

    def test_alias_in_columns(self, cursor):
        cursor.execute(
            """
            SELECT col_list[1] list, col_map.A map-a FROM %s WHERE key_partition='row_1'
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 3
        assert [d[0] for d in cursor.description] == ["list", "map-a"]

        cursor.execute(
            """
            SELECT DATE(col_date, '%Y-%m-%d') col1,
                    DATETIME(col_datetime) col2 FROM {0}
            WHERE key_partition = ? AND key_sort = ?
        """.format(
                TESTCASE03_TABLE
            ),
            ["test_date_row_1", 0],
        )
        assert [d[0] for d in cursor.description] == ["col1", "col2"]

    def test_partiql_functions(self, cursor):
        cursor.execute(
            """
            SELECT key_partition, key_sort, col_str
            FROM %s WHERE key_partition='row_1' and contains("col_str", 'BLOB')
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 1
        assert ret[0][1] == 3

        cursor.execute(
            """
            SELECT key_partition, key_sort, col_str
            FROM %s WHERE key_partition='row_1' and begins_with("col_str", 'Online')
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 1
        assert ret[0][1] == 2

        cursor.execute(
            """
            SELECT key_partition, key_sort, col_str
            FROM %s WHERE key_partition='row_2' and attribute_type("col_str", 'S')
        """
            % TESTCASE03_TABLE
        )
        ret = cursor.fetchall()
        assert len(ret) == 2
