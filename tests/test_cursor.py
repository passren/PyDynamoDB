# -*- coding: utf-8 -*-

class TestCursor:
    table_name = "pydynamodb_test_case01"

    def test_writeone(self, cursor):
        sql_one_row_1_0_ = """
        INSERT INTO "%s" VALUE {
            'key_partition': ?,
            'key_sort': ?,
            'col_str': ?,
            'col_num': ?,
            'col_byte': ?
        }
        """ % self.table_name
        params_1_0_ = [
            "test_one_row_1", 0, "test case 0", 0, b"0"
        ]
        cursor.execute(sql_one_row_1_0_, params_1_0_)

        sql_one_row_1_1_ = """
            INSERT INTO "%s" VALUE {
                'key_partition': ?,
                'key_sort': ?,
                'col_str': ?,
                'col_num': ?,
                'col_map': ?
            }
        """ % self.table_name
        params_1_1_ = [
            "test_one_row_1", 1, "test case 1", "2.3", {"name": "test case 1", "version": 0.1}
        ]
        cursor.execute(sql_one_row_1_1_, params_1_1_)

        sql_one_row_1_2_ = """
            INSERT INTO "%s" VALUE {
                'key_partition': ?,
                'key_sort': ?,
                'col_str': ?,
                'col_ss': ?,
                'col_ns': ?,
                'col_bs': ?,
                'col_list': ?
            }
        """ % self.table_name
        params_1_2_ = [
            "test_one_row_1", 2, "test case 2", 
            {"A", "B", "C"}, 
            {1, 2, 3.3, 4.0},
            {b"@", b"X", b"^"},
            ["Hello", "World", 1, 2, 3]
        ]
        cursor.execute(sql_one_row_1_2_, params_1_2_)

    def test_writemany(self, cursor):
        sql_many_rows_ = """
        INSERT INTO "%s" VALUE {
            'key_partition': ?,
            'key_sort': ?,
            'col_str': ?,
            'col_num': ?,
            'col_byte': ?
        }
        """ % self.table_name
        params_ = [
            ["test_many_rows_1", 0, "test case many 0", "0", b"0"],
            ["test_many_rows_1", 1, "test case many 1", "1", b"1"],
            ["test_many_rows_1", 2, "test case many 2", "2", b"2"],
            ["test_many_rows_1", 3, "test case many 3", "3", b"3"],
            ["test_many_rows_1", 4, "test case many 4", "4", b"4"],
            ["test_many_rows_1", 5, "test case many 5", "5", b"5"],
            ["test_many_rows_1", "6", "test case many 6", "6", b"6"],
            [9999, 7, "test case many 7", "7", b"7"],
        ]
        cursor.executemany(sql_many_rows_, params_)
        assert len(cursor.errors) == 2