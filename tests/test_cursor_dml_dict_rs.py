# -*- coding: utf-8 -*-

TESTCASE05_TABLE = "pydynamodb_test_case05"


class TestCursorDMLDictRs:
    def test_writeone(self, dict_cursor):
        sql = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_ss': ?, 'col_ns': ?, 'col_bs': ?, 'col_list': ?
            }
        """
            % TESTCASE05_TABLE
        )
        params = [
            "row_1",
            0,
            "test case 2",
            {"A", "B", "C"},
            {1, 2, 3.3, 4.0},
            {b"@", b"X", b"^"},
            ["Hello", "World", 1, 2, 3],
        ]
        dict_cursor.execute(sql, params)

        sql = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_nested_list': ?, 'col_nested_map': ?
            }
        """
            % TESTCASE05_TABLE
        )
        params = [
            "row_1",
            1,
            [
                "Hello",
                "World",
                1.0,
                b"1",
                {1, 2, 3},
                {"1", "2", "3"},
                {"name": "test case 3", "version": 1.0},
            ],
            {
                "name": "test case 3",
                "version": 1.0,
                "list": ["Hello", "World", {1, 2, 3}, {"1", "2"}, 2],
                "map": {"str": "Best", "num": 1, "chinese": "你好"},
            },
        ]
        dict_cursor.execute(sql, params)

    def test_select(self, dict_cursor):
        dict_cursor.execute(
            "SELECT * FROM %s WHERE key_partition='row_1'" % TESTCASE05_TABLE
        )
        ret = dict_cursor.fetchall()
        assert len(ret) == 2

        dict_cursor.execute(
            "SELECT col_ss, col_nested_list[4] FROM %s WHERE key_partition='row_1'" % TESTCASE05_TABLE
        )
        ret = dict_cursor.fetchall()
        assert len(ret) == 2
        assert ret[0] == {"col_ss": {"A", "B", "C"}}
        assert ret[1] == {"col_nested_list[4]": {1, 2, 3}}