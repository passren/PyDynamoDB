# -*- coding: utf-8 -*-

from pydynamodb import cursor


TESTCASE01_TABLE = "pydynamodb_test_case01"
USER_TABLE = "user"


class TestCursorDML:
    def test_writeone(self, cursor):
        sql_one_row_1_0_ = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?, 'col_byte': ?
            }
        """
            % TESTCASE01_TABLE
        )
        params_1_0_ = ["test_one_row_1", 0, "test case 0", 0, b"0"]
        cursor.execute(sql_one_row_1_0_, params_1_0_)

        sql_one_row_1_1_ = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?, 'col_map': ?
            }
        """
            % TESTCASE01_TABLE
        )
        params_1_1_ = [
            "test_one_row_1",
            1,
            "test case 1",
            "2.3",
            {"name": "test case 1", "version": 0.1},
        ]
        cursor.execute(sql_one_row_1_1_, params_1_1_)

        sql_one_row_1_2_ = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_ss': ?, 'col_ns': ?, 'col_bs': ?, 'col_list': ?
            }
        """
            % TESTCASE01_TABLE
        )
        params_1_2_ = [
            "test_one_row_1",
            2,
            "test case 2",
            {"A", "B", "C"},
            {1, 2, 3.3, 4.0},
            {b"@", b"X", b"^"},
            ["Hello", "World", 1, 2, 3],
        ]
        cursor.execute(sql_one_row_1_2_, params_1_2_)

        sql_one_row_2_1_ = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_nested_list': ?, 'col_nested_map': ?
            }
        """
            % TESTCASE01_TABLE
        )
        params_2_1_ = [
            "test_one_row_2",
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
        cursor.execute(sql_one_row_2_1_, params_2_1_)

    def test_writemany(self, cursor):
        sql_many_rows_ = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?,
                'key_sort': ?,
                'col_str': ?,
                'col_num': ?,
                'col_byte': ?
            }
        """
            % TESTCASE01_TABLE
        )
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
        cursor.execute(
            """
            SELECT col_str, col_num, col_byte FROM %s
            WHERE key_partition = ?
            AND key_sort = ?
        """
            % TESTCASE01_TABLE,
            ["test_one_row_1", 0],
        )
        assert len(cursor.description) == 3
        assert cursor.rownumber == 0
        assert cursor.fetchone() == ("test case 0", 0, b"0")
        assert cursor.rownumber == 1
        assert cursor.fetchone() is None

        cursor.execute(
            """
            SELECT * FROM %s
            WHERE key_partition = ?
        """
            % TESTCASE01_TABLE,
            ["test_one_row_1"],
        )
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
            assert desc[0] in [
                "key_partition",
                "key_sort",
                "col_str",
                "col_num",
                "col_byte",
                "col_map",
                "col_ss",
                "col_ns",
                "col_list",
                "col_bs",
            ]

        cursor.execute(
            """
            SELECT col_nested_list, col_nested_map FROM %s
            WHERE key_partition = ?
            AND key_sort = ?
        """
            % TESTCASE01_TABLE,
            ["test_one_row_2", 1],
        )
        row = cursor.fetchone()
        desc = cursor.description
        assert self._get_value_by_column_name(desc, row, "col_nested_list") == [
            "Hello",
            "World",
            1.0,
            b"1",
            {1, 2, 3},
            {"1", "2", "3"},
            {"name": "test case 3", "version": 1.0},
        ]
        assert self._get_value_by_column_name(desc, row, "col_nested_map") == {
            "name": "test case 3",
            "version": 1.0,
            "list": ["Hello", "World", {1, 2, 3}, {"1", "2"}, 2],
            "map": {"str": "Best", "num": 1, "chinese": "你好"},
        }

    def test_fetchmany(self, cursor):
        cursor.execute(
            """
            SELECT * FROM %s
            WHERE key_partition = ?
        """
            % TESTCASE01_TABLE,
            ["test_many_rows_1"],
        )
        assert cursor.rownumber == 0
        assert len(cursor.fetchmany(3)) == 3
        assert cursor.rownumber == 3
        assert len(cursor.fetchmany(3)) == 3
        assert cursor.rownumber == 6
        assert len(cursor.fetchmany(3)) == 1
        assert cursor.rownumber == 7
        assert cursor.fetchmany(3) == []

    def test_fetchall(self, cursor):
        cursor.execute(
            """
            SELECT * FROM %s
            WHERE key_partition = ?
        """
            % TESTCASE01_TABLE,
            ["test_many_rows_1"],
        )
        assert cursor.rownumber == 0
        assert len(cursor.fetchall()) == 7
        assert cursor.rownumber == 7
        assert cursor.fetchall() == []

    def test_unicode(self, cursor):
        unicode_str = "测试"
        sql_unicode_row_1_0_ = (
            """
        INSERT INTO %s VALUE {
            'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?, 'col_byte': ?
        }
        """
            % TESTCASE01_TABLE
        )
        params_1_0_ = ["test_unicode_row_1", 0, "测试案例 0", 0, unicode_str.encode()]
        cursor.execute(sql_unicode_row_1_0_, params_1_0_)

        cursor.execute(
            """
            SELECT * FROM %s
            WHERE key_partition = ? AND key_sort = ?
        """
            % TESTCASE01_TABLE,
            ["test_unicode_row_1", 0],
        )
        rows = cursor.fetchall()
        desc = cursor.description
        assert len(rows) == 1
        assert self._get_value_by_column_name(desc, rows[0], "col_str") == "测试案例 0"

    def test_update(self, cursor):
        sql_update_row_1_0_ = (
            """
            UPDATE %s
            SET col_str=?
            SET col_num=?
            WHERE key_partition=? AND key_sort=?
            RETURNING ALL OLD *
        """
            % TESTCASE01_TABLE
        )
        params_1_0_ = ["test case update 0", 10, "test_one_row_1", 0]
        cursor.execute(sql_update_row_1_0_, params_1_0_)
        rows = cursor.fetchall()
        desc = cursor.description
        assert len(rows) == 1
        assert self._get_value_by_column_name(desc, rows[0], "col_str") == "test case 0"
        assert self._get_value_by_column_name(desc, rows[0], "col_num") == 0

        cursor.execute(
            """
            SELECT col_str, col_num FROM %s
            WHERE key_partition = ? AND key_sort = ?
        """
            % TESTCASE01_TABLE,
            ["test_one_row_1", 0],
        )
        rows = cursor.fetchall()
        desc = cursor.description
        assert len(rows) == 1
        assert (
            self._get_value_by_column_name(desc, rows[0], "col_str")
            == "test case update 0"
        )
        assert self._get_value_by_column_name(desc, rows[0], "col_num") == 10

    def test_delete(self, cursor):
        sql_delete_row_1_0_ = (
            """
            DELETE FROM %s
            WHERE key_partition = ? AND key_sort = ?
            RETURNING ALL OLD *
        """
            % TESTCASE01_TABLE
        )
        params_1_0_ = ["test_one_row_1", 0]
        cursor.execute(sql_delete_row_1_0_, params_1_0_)
        rows = cursor.fetchall()
        assert len(rows) == 1

        sql_delete_row_1_0_ = (
            """
            DELETE FROM %s
            WHERE key_partition = ? AND key_sort = ?
        """
            % TESTCASE01_TABLE
        )
        params_1_0_ = ["test_one_row_1", 0]
        cursor.execute(sql_delete_row_1_0_, params_1_0_)
        rows = cursor.fetchall()
        assert len(rows) == 0

    def test_limit_sql(self, cursor):
        cursor.execute(
            """
            SELECT * FROM %s
            WHERE key_partition = ?
            LIMIT 2
        """
            % TESTCASE01_TABLE,
            ["test_many_rows_1"],
        )
        assert len(cursor.fetchall()) == 2

    def test_insert_datetime(self, cursor):
        from datetime import date, datetime

        sql_date_row_1_0_ = (
            """
        INSERT INTO %s VALUE {
            'key_partition': ?, 'key_sort': ?, 'col_date': ?, 'col_datetime': ?
        }
        """
            % TESTCASE01_TABLE
        )
        params_1_0_ = [
            "test_date_row_1",
            0,
            date(2022, 10, 18),
            datetime(2022, 10, 18, 13, 55, 34),
        ]
        cursor.execute(sql_date_row_1_0_, params_1_0_)
        cursor.execute(
            """
            SELECT col_date, col_datetime FROM %s
            WHERE key_partition = ? AND key_sort = ?
        """
            % TESTCASE01_TABLE,
            ["test_date_row_1", 0],
        )
        assert cursor.fetchone() == ("2022-10-18", "2022-10-18T13:55:34")

    def test_list_tables(self, cursor):
        tables_ = cursor.list_tables()
        assert len(tables_) > 0

    def _get_value_by_column_name(self, description, row, column):
        col_str_index = None
        for index, val in enumerate(description):
            col_str_index = index if val[0] == column else None

            if col_str_index is not None:
                break

        return row[col_str_index]

    def test_get_table_metadata(self, cursor):
        metadata_ = cursor.get_table_metadata(TESTCASE01_TABLE)
        assert metadata_ is not None
        assert metadata_["TableName"] == TESTCASE01_TABLE
        assert len(metadata_["KeySchema"]) == 2
        assert metadata_["KeySchema"][0]["AttributeName"] == "key_partition"
        assert metadata_["KeySchema"][0]["KeyType"] == "HASH"
        assert metadata_["KeySchema"][1]["AttributeName"] == "key_sort"
        assert metadata_["KeySchema"][1]["KeyType"] == "RANGE"
        assert len(metadata_["AttributeDefinitions"]) == 2
        assert metadata_["AttributeDefinitions"][0]["AttributeName"] == "key_partition"
        assert metadata_["AttributeDefinitions"][0]["AttributeType"] == "S"
        assert metadata_["AttributeDefinitions"][1]["AttributeName"] == "key_sort"
        assert metadata_["AttributeDefinitions"][1]["AttributeType"] == "N"

    def test_reserved_words(self, cursor):
        sql_reserved_words_1 = (
            """
        INSERT INTO %s VALUE {
            'key_partition': ?, 'key_sort': ?, 'username': ?, 'password': ?, 'default': ?, 'comment': ?
        }
        """
            % USER_TABLE
        )
        params_1_ = [
            "test_user_row_1",
            0,
            "admin",
            "admin",
            1,
            "",
        ]
        cursor.execute(sql_reserved_words_1, params_1_)

        cursor.execute(
            """
        SELECT username, password, "default", "comment" FROM %s
        WHERE key_partition=? AND key_sort=?
        """
            % USER_TABLE,
            ["test_user_row_1", 0],
        )
        assert cursor.fetchone() == ("admin", "admin", 1, "")

        sql_reserved_words_2 = (
            """
        UPDATE %s
            SET username=?
            SET password=?
            SET "default"=?
            WHERE key_partition=? AND key_sort=?
            RETURNING ALL OLD *
        """
            % USER_TABLE
        )
        params_2_ = [
            "admin1",
            "admin1",
            0,
            "test_user_row_1",
            0,
        ]
        cursor.execute(sql_reserved_words_2, params_2_)

        cursor.execute(
            """
        SELECT username, password, "default" FROM %s
        WHERE key_partition=? AND key_sort=?
        """
            % USER_TABLE,
            ["test_user_row_1", 0],
        )
        assert cursor.fetchone() == ("admin1", "admin1", 0)

        sql_reserved_words_3 = (
            """
        DELETE FROM %s
            WHERE key_partition=? AND key_sort=?
            RETURNING ALL OLD *
        """
            % USER_TABLE
        )
        params_3_ = [
            "test_user_row_1",
            0,
        ]
        cursor.execute(sql_reserved_words_3, params_3_)