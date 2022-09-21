# -*- coding: utf-8 -*-

TESTCASE01_TABLE = "pydynamodb_test_case01"
class TestCursor:

    def test_writeone(self, cursor):
        sql_one_row_1_0_ = """
        INSERT INTO %s VALUE {
            'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?, 'col_byte': ?
        }
        """ % TESTCASE01_TABLE
        params_1_0_ = [
            "test_one_row_1", 0, "test case 0", 0, b"0"
        ]
        cursor.execute(sql_one_row_1_0_, params_1_0_)

        sql_one_row_1_1_ = """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?, 'col_map': ?
            }
        """ % TESTCASE01_TABLE
        params_1_1_ = [
            "test_one_row_1", 1, "test case 1", "2.3", {"name": "test case 1", "version": 0.1}
        ]
        cursor.execute(sql_one_row_1_1_, params_1_1_)

        sql_one_row_1_2_ = """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_ss': ?, 'col_ns': ?, 'col_bs': ?, 'col_list': ?
            }
        """ % TESTCASE01_TABLE
        params_1_2_ = [
            "test_one_row_1", 2, "test case 2", 
            {"A", "B", "C"}, 
            {1, 2, 3.3, 4.0},
            {b"@", b"X", b"^"},
            ["Hello", "World", 1, 2, 3]
        ]
        cursor.execute(sql_one_row_1_2_, params_1_2_)

        sql_one_row_2_1_ = """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_nested_list': ?, 'col_nested_map': ?
            }
        """ % TESTCASE01_TABLE
        params_2_1_ = [
            "test_one_row_2", 1,
            ["Hello", "World", 1.0, b"1", {1, 2, 3}, {"1", "2", "3"}, {"name": "test case 3", "version": 1.0}],
            {
                "name": "test case 3", 
                "version": 1.0, 
                "list": ["Hello", "World", {1, 2, 3}, {"1", "2"}, 2], 
                "map": {"str": "Best", "num": 1, "chinese": u"你好"},
            }
        ]
        cursor.execute(sql_one_row_2_1_, params_2_1_)

    def test_writemany(self, cursor):
        sql_many_rows_ = """
        INSERT INTO %s VALUE {
            'key_partition': ?,
            'key_sort': ?,
            'col_str': ?,
            'col_num': ?,
            'col_byte': ?
        }
        """ % TESTCASE01_TABLE
        params_ = [
            ["test_many_rows_1", 0, "test case many 0", "0", b"0"],
            ["test_many_rows_1", 1, "test case many 1", "1", b"1"],
            ["test_many_rows_1", 2, "test case many 2", "2", b"2"],
            ["test_many_rows_1", 3, "test case many 3", "3", b"3"],
            ["test_many_rows_1", 4, "test case many 4", "4", b"4"],
            ["test_many_rows_1", 5, "test case many 5", "5", b"5"],
            ["test_many_rows_1", 6, "test case many 6", "6", b"6"],
            ["test_many_rows_1", "7", "test case many 7", "7", b"7"],
            [9999, 8, "test case many 8", "8", b"8"],
        ]
        cursor.executemany(sql_many_rows_, params_)
        assert len(cursor.errors) == 2

    def test_fetchone(self, cursor):
        cursor.execute("""
            SELECT col_str, col_num, col_byte FROM %s
            WHERE key_partition = ?
            AND key_sort = ?
        """ % TESTCASE01_TABLE, [
            "test_one_row_1", 0
        ])
        assert len(cursor.description) == 3
        expected_data_ = []
        for desc in cursor.description:
            assert desc[0] in ["col_str", "col_num", "col_byte"]
            if desc[0] == "col_str":
                assert desc[1] == "S" 
                expected_data_.append("test case 0")
            if desc[0] == "col_num":
                assert desc[1] == "N"
                expected_data_.append(0)
            if desc[0] == "col_byte":
                assert desc[1] == "B"
                expected_data_.append(b"0")  
        assert cursor.rownumber == 0
        assert cursor.fetchone() == tuple(expected_data_)
        assert cursor.rownumber == 1
        assert cursor.fetchone() is None

        cursor.execute("""
            SELECT * FROM %s
            WHERE key_partition = ?
        """ % TESTCASE01_TABLE, [
            "test_one_row_1"
        ])
        assert cursor.rownumber == 0
        cursor.fetchone()
        assert cursor.rownumber == 1
        cursor.fetchone()
        assert cursor.rownumber == 2
        cursor.fetchone()
        assert cursor.rownumber == 3
        assert cursor.fetchone() is None
        assert len(cursor.description) == 10
        for desc in cursor.description:
            assert desc[0] in ["key_partition", "key_sort",
                                "col_str", "col_num", 
                                "col_byte", "col_map",
                                "col_ss", "col_ns",
                                "col_list", "col_bs"]

    def test_fetchmany(self, cursor):
        cursor.execute("""
            SELECT * FROM %s
            WHERE key_partition = ?
        """ % TESTCASE01_TABLE, [
            "test_many_rows_1"
        ])
        assert cursor.rownumber == 0
        assert len(cursor.fetchmany(3)) == 3
        assert cursor.rownumber == 3
        assert len(cursor.fetchmany(3)) == 3
        assert cursor.rownumber == 6
        assert len(cursor.fetchmany(3)) == 1
        assert cursor.rownumber == 7
        assert cursor.fetchmany(3) == []

    def test_fetchall(self, cursor):
        cursor.execute("""
            SELECT * FROM %s
            WHERE key_partition = ?
        """ % TESTCASE01_TABLE, [
            "test_many_rows_1"
        ])
        assert cursor.rownumber == 0
        assert len(cursor.fetchall()) == 7
        assert cursor.rownumber == 7
        assert cursor.fetchall() == []

    def test_unicode(self, cursor):
        unicode_str = u"测试"
        sql_unicode_row_1_0_ = """
        INSERT INTO %s VALUE {
            'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?, 'col_byte': ?
        }
        """ % TESTCASE01_TABLE
        params_1_0_ = [
            "test_unicode_row_1", 0, u"测试案例 0", 0, unicode_str.encode()
        ]
        cursor.execute(sql_unicode_row_1_0_, params_1_0_)

        cursor.execute("""
            SELECT * FROM %s
            WHERE key_partition = ? AND key_sort = ?
        """ % TESTCASE01_TABLE, [
            "test_unicode_row_1", 0
        ])
        rows = cursor.fetchall()
        desc = cursor.description
        assert len(rows) == 1
        assert self._get_value_by_column_name(
                    desc, rows[0], "col_str"
                ) == u"测试案例 0"

    def test_update(self, cursor):
        sql_update_row_1_0_ = """
            UPDATE %s 
            SET col_str=?
            SET col_num=?
            WHERE key_partition=? AND key_sort=?
            RETURNING ALL OLD *
        """ % TESTCASE01_TABLE
        params_1_0_ = ["test case update 0", 10, "test_one_row_1", 0]
        cursor.execute(sql_update_row_1_0_, params_1_0_)
        rows = cursor.fetchall()
        desc = cursor.description
        assert len(rows) == 1
        assert self._get_value_by_column_name(
                    desc, rows[0], "col_str"
                ) == "test case 0"
        assert self._get_value_by_column_name(
                    desc, rows[0], "col_num"
                ) == 0

        cursor.execute("""
            SELECT col_str, col_num FROM %s
            WHERE key_partition = ? AND key_sort = ?
        """ % TESTCASE01_TABLE, [
            "test_one_row_1", 0
        ])
        rows = cursor.fetchall()
        desc = cursor.description
        assert len(rows) == 1
        assert self._get_value_by_column_name(
                    desc, rows[0], "col_str"
                ) == "test case update 0"
        assert self._get_value_by_column_name(
                    desc, rows[0], "col_num"
                ) == 10

    def test_list_tables(self, cursor):
        tables_ = cursor.list_tables()
        assert tables_ == ['pydynamodb_test_case01',
                            'pydynamodb_test_case02',
                            'pydynamodb_test_case03',
                        ]

    def _get_value_by_column_name(self, description, row, column):
        col_str_index = None
        for index, val in enumerate(description):
            col_str_index = index if val[0] == column else None

            if col_str_index is not None:
                break

        return row[col_str_index]