

class TestConnection:
    table_name = "pydynamodb_test_case01"

    def test_transaction_both_read_write(self, conn):
        try:
            sql_trans_1_ = """
                INSERT INTO "%s" VALUE {
                    'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
                }
            """ % self.table_name

            conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(sql_trans_1_, ["test_trans_1", 0, "test case 1-0", 0])
                cursor.execute(sql_trans_1_, ["test_trans_1", 1, "test case 1-1", 1])
                cursor.execute(sql_trans_1_, ["test_trans_1", 2, "test case 1-2", 2])
                cursor.executemany(sql_trans_1_, [
                    ["test_trans_1", 3, "test case 1-3", 3],
                    ["test_trans_1", 4, "test case 1-4", 4],
                    ["test_trans_1", 5, "test case 1-5", 5],
                ])
                cursor.execute("""
                    SELECT * FROM "%s" WHERE key_partition = ?
                """% self.table_name, ["test_trans_1"])

                conn.commit()
        except Exception as e:
            assert "ExecuteTransaction API does not support both read and write operations in the same request" in str(e)

        with conn.cursor() as cursor:
            assert len(self._query_data(cursor, "test_trans_1")) == 0

    def test_transaction_one_row(self, conn):
        sql_trans_2_ = """
            INSERT INTO "%s" VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
            }
        """ % self.table_name

        conn.begin()
        cursor = conn.cursor()
        cursor.execute(sql_trans_2_, ["test_trans_2", 0, "test case 2-0", 0])
        conn.commit()

        assert len(self._query_data(cursor, "test_trans_2")) == 1

    def test_transaction_many_row(self, conn):
        sql_trans_3_ = """
            INSERT INTO "%s" VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
            }
        """ % self.table_name

        conn.begin()
        cursor = conn.cursor()
        cursor.execute(sql_trans_3_, ["test_trans_3", 0, "test case 3-0", 0])
        cursor.execute(sql_trans_3_, ["test_trans_3", 1, "test case 3-1", 1])
        cursor.executemany(sql_trans_3_, [
            ["test_trans_3", 2, "test case 3-2", 2],
            ["test_trans_3", 3, "test case 3-3", 3],
            ["test_trans_3", 4, "test case 3-4", 4],
        ])
        conn.commit()
        
        assert len(self._query_data(cursor, "test_trans_3")) == 5

    def test_transaction_mixed_no_trans(self, conn):
        sql_trans_4_ = """
            INSERT INTO "%s" VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
            }
        """ % self.table_name

        cursor = conn.cursor()
        cursor.execute(sql_trans_4_, ["test_trans_4", 0, "test case 4-0", 0])
        cursor.execute(sql_trans_4_, ["test_trans_4", 1, "test case 4-1", 1])

        assert len(self._query_data(cursor, "test_trans_4")) == 2

        conn.begin()
        cursor.execute(sql_trans_4_, ["test_trans_4", 2, "test case 4-2", 2])
        cursor.execute(sql_trans_4_, ["test_trans_4", 3, "test case 4-3", 3])
        cursor.executemany(sql_trans_4_, [
            ["test_trans_4", 4, "test case 4-4", 4],
            ["test_trans_4", 5, "test case 4-5", 5],
        ])
        conn.commit()

        assert len(self._query_data(cursor, "test_trans_4")) == 6

        try:
            cursor.execute(sql_trans_4_, ["test_trans_4", 5, "test case 4-5", 5])
        except Exception as e:
            assert "Duplicate primary key exists in table" in str(e)

        assert len(self._query_data(cursor, "test_trans_4")) == 6

        cursor.execute(sql_trans_4_, ["test_trans_4", 6, "test case 4-6", 6])
        cursor.executemany(sql_trans_4_, [
            ["test_trans_4", 7, "test case 4-7", 7],
            ["test_trans_4", 8, "test case 4-8", 8],
        ])

        assert len(self._query_data(cursor, "test_trans_4")) == 9

    def _query_data(self, cursor, test_case):
        cursor.execute("""
            SELECT * FROM "%s" WHERE key_partition = ?
        """% self.table_name, [test_case])
        return cursor.fetchall()