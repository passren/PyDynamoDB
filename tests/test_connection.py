# -*- coding: utf-8 -*-
from moto import mock_sts

TESTCASE01_TABLE = "pydynamodb_test_case01"


class TestConnection:
    def test_transaction_both_read_write(self, conn):
        try:
            sql_trans_1_ = (
                """
                INSERT INTO %s VALUE {
                    'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
                }
            """
                % TESTCASE01_TABLE
            )

            conn.begin()
            with conn.cursor() as cursor:
                cursor.execute(sql_trans_1_, ["test_trans_1", 0, "test case 1-0", 0])
                cursor.execute(sql_trans_1_, ["test_trans_1", 1, "test case 1-1", 1])
                cursor.execute(sql_trans_1_, ["test_trans_1", 2, "test case 1-2", 2])
                cursor.executemany(
                    sql_trans_1_,
                    [
                        ["test_trans_1", 3, "test case 1-3", 3],
                        ["test_trans_1", 4, "test case 1-4", 4],
                        ["test_trans_1", 5, "test case 1-5", 5],
                    ],
                )
                cursor.execute(
                    """
                    SELECT * FROM %s WHERE key_partition = ?
                """
                    % TESTCASE01_TABLE,
                    ["test_trans_1"],
                )

                conn.commit()
        except Exception as e:
            assert "both read and write operation" in str(e)

        with conn.cursor() as cursor:
            assert len(self._query_data(cursor, "test_trans_1")) == 0

    def test_transaction_one_row(self, conn):
        sql_trans_2_ = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
            }
        """
            % TESTCASE01_TABLE
        )

        conn.begin()
        cursor = conn.cursor()
        cursor.execute(sql_trans_2_, ["test_trans_2", 0, "test case 2-0", 0])
        conn.commit()

        assert len(self._query_data(cursor, "test_trans_2")) == 1

    def test_transaction_many_row(self, conn):
        sql_trans_3_ = (
            """
            INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
            }
        """
            % TESTCASE01_TABLE
        )

        conn.begin()
        cursor = conn.cursor()
        cursor.execute(sql_trans_3_, ["test_trans_3", 0, "test case 3-0", 0])
        cursor.execute(sql_trans_3_, ["test_trans_3", 1, "test case 3-1", 1])
        cursor.executemany(
            sql_trans_3_,
            [
                ["test_trans_3", 2, "test case 3-2", 2],
                ["test_trans_3", 3, "test case 3-3", 3],
                ["test_trans_3", 4, "test case 3-4", 4],
            ],
        )
        conn.commit()

        assert len(self._query_data(cursor, "test_trans_3")) == 5

    def test_transaction_mixed_no_trans(self, conn):
        sql_trans_4_ = (
            """
            INSERT INTO "%s" VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
            }
        """
            % TESTCASE01_TABLE
        )

        cursor = conn.cursor()
        cursor.execute(sql_trans_4_, ["test_trans_4", 0, "test case 4-0", 0])
        cursor.execute(sql_trans_4_, ["test_trans_4", 1, "test case 4-1", 1])

        assert len(self._query_data(cursor, "test_trans_4")) == 2

        conn.begin()
        cursor.execute(sql_trans_4_, ["test_trans_4", 2, "test case 4-2", 2])
        cursor.execute(sql_trans_4_, ["test_trans_4", 3, "test case 4-3", 3])
        cursor.executemany(
            sql_trans_4_,
            [
                ["test_trans_4", 4, "test case 4-4", 4],
                ["test_trans_4", 5, "test case 4-5", 5],
            ],
        )
        conn.commit()

        assert len(self._query_data(cursor, "test_trans_4")) == 6

        try:
            cursor.execute(sql_trans_4_, ["test_trans_4", 5, "test case 4-5", 5])
        except Exception as e:
            assert "Duplicate primary key exists in table" in str(e)

        assert len(self._query_data(cursor, "test_trans_4")) == 6

        cursor.execute(sql_trans_4_, ["test_trans_4", 6, "test case 4-6", 6])
        cursor.executemany(
            sql_trans_4_,
            [
                ["test_trans_4", 7, "test case 4-7", 7],
                ["test_trans_4", 8, "test case 4-8", 8],
            ],
        )

        assert len(self._query_data(cursor, "test_trans_4")) == 9

    def test_batch_write_1(self, conn):
        cursor = conn.cursor()
        sql_batch_1_ = (
            """
            INSERT INTO "%s" VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_str': ?, 'col_num': ?
            }
        """
            % TESTCASE01_TABLE
        )
        conn.autocommit = False
        cursor.execute(sql_batch_1_, ["test_batch_1", 0, "test case 5-0", 0])
        cursor.execute(sql_batch_1_, ["test_batch_1", 1, "test case 5-1", 1])
        cursor.execute(sql_batch_1_, ["test_batch_1", 2, "test case 5-2", 2])
        cursor.executemany(
            sql_batch_1_,
            [
                ["test_batch_1", 3, "test case 5-3", 3],
                ["test_batch_1", 4, "test case 5-4", 4],
            ],
        )
        conn.autocommit = True

        ret = self._query_data(cursor, "test_batch_1")
        assert len(ret) == 5

        conn.autocommit = False
        cursor.execute(sql_batch_1_, ["test_batch_1", 5, "test case 5-5", 5])
        cursor.execute(sql_batch_1_, ["test_batch_1", 6, "test case 5-6", 6])
        cursor.flush()

        ret = self._query_data(cursor, "test_batch_1")
        assert len(ret) == 7

    def _query_data(self, cursor, test_case):
        cursor.execute(
            """
            SELECT * FROM %s WHERE key_partition = ?
        """
            % TESTCASE01_TABLE,
            [test_case],
        )
        return cursor.fetchall()
    
    @mock_sts
    def test_conn_with_credentials(self):
        from pydynamodb import connect

        # assume_role
        conn = connect(
            region_name="us-east-1",
            role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        # assume_role with ExternalId
        conn = connect(
            region_name="us-east-1",
            role_arn="arn:aws:iam::123456789012:role/TestRole",
            external_id="123ABC",
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        # assume_role with ExternalId and MFA
        conn = connect(
            region_name="us-east-1",
            role_arn="arn:aws:iam::123456789012:role/TestRole",
            external_id="123ABC",
            serial_number="xhseh35s12",
            token_code="7766933",
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        # assume_role_with_web_identity
        conn = connect(
            region_name="us-east-1",
            role_arn="arn:aws:iam::123456789012:role/FederatedWebIdentityRole",
            web_identity_token="Atza%7CIQEBLjAsAhRFiXuWpUXuRvQ9PZL3GMFcYevydwIUFAHZwXZXXXXXXXXJnrulxKDHwy87oGKPznh0D6bEQZTSCzyoCtL_8S07pLpr0zMbn6w1lfVZKNTBdDansFBmtGnIsIapjI6xKR02Yc_2bQ8LZbUXSGm6Ry6_BG7PrtLZtj_dfCTj92xNGed-CrKqjG7nPBjNIL016GGvuS5gSvPRUxWES3VYfm1wl7WTI7jn-Pcb6M-buCgHhFOzTQxod27L9CqnOLio7N3gZAGpsp6n1-AJBOCJckcyXe2c6uD0srOJeZlKUm2eTDVMf8IehDVI0r1QOnTV6KzzAI3OY87Vd_cVMQ",
            provider_id="www.amazon.com",
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None
