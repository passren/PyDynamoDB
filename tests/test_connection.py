# -*- coding: utf-8 -*-
import contextlib
from moto import mock_aws

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


class TestConnectionWithSTS:
    TEST_REGION_NAME = "us-east-1"
    TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/TestRole"
    TEST_EXTERNAL_ID = "123ABC"
    TEST_SERIAL_NUMBER = "xhseh35s12"
    TEST_TOKEN_CODE = "7766933"
    TEST_PRINCIPAL_ARN = "arn:aws:iam::123456789012:saml-provider/SAML-test"
    TEST_SAML_ASSERTION = "PD94bWwgdmVyc2lvbj0iMS4wIj8+PHNhbWxwOlJlc3BvbnNlIHhtbG5zOnNhbWxwPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6cHJvdG9jb2wiIElEPSJfMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAwIiBWZXJzaW9uPSIyLjAiIElzc3VlSW5zdGFudD0iMjAxMi0wMS0wMVQxMjowMDowMC4wMDBaIiBEZXN0aW5hdGlvbj0iaHR0cHM6Ly9zaWduaW4uYXdzLmFtYXpvbi5jb20vc2FtbCIgQ29uc2VudD0idXJuOm9hc2lzOm5hbWVzOnRjOlNBTUw6Mi4wOmNvbnNlbnQ6dW5zcGVjaWZpZWQiPiAgPElzc3VlciB4bWxucz0idXJuOm9hc2lzOm5hbWVzOnRjOlNBTUw6Mi4wOmFzc2VydGlvbiI+aHR0cDovL2xvY2FsaG9zdC88L0lzc3Vlcj4gIDxzYW1scDpTdGF0dXM+ICAgIDxzYW1scDpTdGF0dXNDb2RlIFZhbHVlPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6c3RhdHVzOlN1Y2Nlc3MiLz4gIDwvc2FtbHA6U3RhdHVzPiAgPEFzc2VydGlvbiB4bWxucz0idXJuOm9hc2lzOm5hbWVzOnRjOlNBTUw6Mi4wOmFzc2VydGlvbiIgSUQ9Il8wMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDAiIElzc3VlSW5zdGFudD0iMjAxMi0xMi0wMVQxMjowMDowMC4wMDBaIiBWZXJzaW9uPSIyLjAiPiAgICA8SXNzdWVyPmh0dHA6Ly9sb2NhbGhvc3Q6MzAwMC88L0lzc3Vlcj4gICAgPGRzOlNpZ25hdHVyZSB4bWxuczpkcz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC8wOS94bWxkc2lnIyI+ICAgICAgPGRzOlNpZ25lZEluZm8+ICAgICAgICA8ZHM6Q2Fub25pY2FsaXphdGlvbk1ldGhvZCBBbGdvcml0aG09Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvMTAveG1sLWV4Yy1jMTRuIyIvPiAgICAgICAgPGRzOlNpZ25hdHVyZU1ldGhvZCBBbGdvcml0aG09Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvMDQveG1sZHNpZy1tb3JlI3JzYS1zaGEyNTYiLz4gICAgICAgIDxkczpSZWZlcmVuY2UgVVJJPSIjXzAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCI+ICAgICAgICAgIDxkczpUcmFuc2Zvcm1zPiAgICAgICAgICAgIDxkczpUcmFuc2Zvcm0gQWxnb3JpdGhtPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwLzA5L3htbGRzaWcjZW52ZWxvcGVkLXNpZ25hdHVyZSIvPiAgICAgICAgICAgIDxkczpUcmFuc2Zvcm0gQWxnb3JpdGhtPSJodHRwOi8vd3d3LnczLm9yZy8yMDAxLzEwL3htbC1leGMtYzE0biMiLz4gICAgICAgICAgPC9kczpUcmFuc2Zvcm1zPiAgICAgICAgICA8ZHM6RGlnZXN0TWV0aG9kIEFsZ29yaXRobT0iaHR0cDovL3d3dy53My5vcmcvMjAwMS8wNC94bWxlbmMjc2hhMjU2Ii8+ICAgICAgICAgIDxkczpEaWdlc3RWYWx1ZT5OVEl5TXprMFpHSTRNakkwWmpJNVpHTmhZamt5T0dReVpHUTFOVFpqT0RWaVpqazVZVFk0T0RGak9XUmpOamt5WXpabU9EWTJaRFE0Tmpsa1pqWTNZU0FnTFFvPTwvZHM6RGlnZXN0VmFsdWU+ICAgICAgICA8L2RzOlJlZmVyZW5jZT4gICAgICA8L2RzOlNpZ25lZEluZm8+ICAgICAgPGRzOlNpZ25hdHVyZVZhbHVlPk5USXlNemswWkdJNE1qSTBaakk1WkdOaFlqa3lPR1F5WkdRMU5UWmpPRFZpWmprNVlUWTRPREZqT1dSak5qa3lZelptT0RZMlpEUTROamxrWmpZM1lTQWdMUW89PC9kczpTaWduYXR1cmVWYWx1ZT4gICAgICA8S2V5SW5mbyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC8wOS94bWxkc2lnIyI+ICAgICAgICA8ZHM6WDUwOURhdGE+ICAgICAgICAgIDxkczpYNTA5Q2VydGlmaWNhdGU+TlRJeU16azBaR0k0TWpJMFpqSTVaR05oWWpreU9HUXlaR1ExTlRaak9EVmlaams1WVRZNE9ERmpPV1JqTmpreVl6Wm1PRFkyWkRRNE5qbGtaalkzWVNBZ0xRbz08L2RzOlg1MDlDZXJ0aWZpY2F0ZT4gICAgICAgIDwvZHM6WDUwOURhdGE+ICAgICAgPC9LZXlJbmZvPiAgICA8L2RzOlNpZ25hdHVyZT4gICAgPFN1YmplY3Q+ICAgICAgPE5hbWVJRCBGb3JtYXQ9InVybjpvYXNpczpuYW1lczp0YzpTQU1MOjIuMDpuYW1laWQtZm9ybWF0OnBlcnNpc3RlbnQiPntmZWRfaWRlbnRpZmllcn08L05hbWVJRD4gICAgICA8U3ViamVjdENvbmZpcm1hdGlvbiBNZXRob2Q9InVybjpvYXNpczpuYW1lczp0YzpTQU1MOjIuMDpjbTpiZWFyZXIiPiAgICAgICAgPFN1YmplY3RDb25maXJtYXRpb25EYXRhIE5vdE9uT3JBZnRlcj0iMjAxMi0wMS0wMVQxMzowMDowMC4wMDBaIiBSZWNpcGllbnQ9Imh0dHBzOi8vc2lnbmluLmF3cy5hbWF6b24uY29tL3NhbWwiLz4gICAgICA8L1N1YmplY3RDb25maXJtYXRpb24+ICAgIDwvU3ViamVjdD4gICAgPENvbmRpdGlvbnMgTm90QmVmb3JlPSIyMDEyLTAxLTAxVDEyOjAwOjAwLjAwMFoiIE5vdE9uT3JBZnRlcj0iMjAxMi0wMS0wMVQxMzowMDowMC4wMDBaIj4gICAgICA8QXVkaWVuY2VSZXN0cmljdGlvbj4gICAgICAgIDxBdWRpZW5jZT51cm46YW1hem9uOndlYnNlcnZpY2VzPC9BdWRpZW5jZT4gICAgICA8L0F1ZGllbmNlUmVzdHJpY3Rpb24+ICAgIDwvQ29uZGl0aW9ucz4gICAgPEF0dHJpYnV0ZVN0YXRlbWVudD4gICAgICA8QXR0cmlidXRlIE5hbWU9Imh0dHBzOi8vYXdzLmFtYXpvbi5jb20vU0FNTC9BdHRyaWJ1dGVzL1JvbGVTZXNzaW9uTmFtZSI+ICAgICAgICA8QXR0cmlidXRlVmFsdWU+e2ZlZF9uYW1lfTwvQXR0cmlidXRlVmFsdWU+ICAgICAgPC9BdHRyaWJ1dGU+ICAgICAgPEF0dHJpYnV0ZSBOYW1lPSJodHRwczovL2F3cy5hbWF6b24uY29tL1NBTUwvQXR0cmlidXRlcy9Sb2xlIj4gICAgICAgIDxBdHRyaWJ1dGVWYWx1ZT5hcm46YXdzOmlhbTo6MTIzNDU2Nzg5MDEyOnNhbWwtcHJvdmlkZXIvU0FNTC10ZXN0LGFybjphd3M6aWFtOjoxMjM0NTY3ODkwMTI6cm9sZS9UZXN0U2FtbDwvQXR0cmlidXRlVmFsdWU+ICAgICAgPC9BdHRyaWJ1dGU+ICAgICAgPEF0dHJpYnV0ZSBOYW1lPSJodHRwczovL2F3cy5hbWF6b24uY29tL1NBTUwvQXR0cmlidXRlcy9TZXNzaW9uRHVyYXRpb24iPiAgICAgICAgPEF0dHJpYnV0ZVZhbHVlPjkwMDwvQXR0cmlidXRlVmFsdWU+ICAgICAgPC9BdHRyaWJ1dGU+ICAgIDwvQXR0cmlidXRlU3RhdGVtZW50PiAgICA8QXV0aG5TdGF0ZW1lbnQgQXV0aG5JbnN0YW50PSIyMDEyLTAxLTAxVDEyOjAwOjAwLjAwMFoiIFNlc3Npb25JbmRleD0iXzAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCI+ICAgICAgPEF1dGhuQ29udGV4dD4gICAgICAgIDxBdXRobkNvbnRleHRDbGFzc1JlZj51cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6YWM6Y2xhc3NlczpQYXNzd29yZFByb3RlY3RlZFRyYW5zcG9ydDwvQXV0aG5Db250ZXh0Q2xhc3NSZWY+ICAgICAgPC9BdXRobkNvbnRleHQ+ICAgIDwvQXV0aG5TdGF0ZW1lbnQ+ICA8L0Fzc2VydGlvbj48L3NhbWxwOlJlc3BvbnNlPg=="
    TEST_WEB_IDENTITY_TOKEN = "Atza%7CIQEBLjAsAhRFiXuWpUXuRvQ9PZL3GMFcYevydwIUFAHZwXZXXXXXXXXJnrulxKDHwy87oGKPznh0D6bEQZTSCzyoCtL_8S07pLpr0zMbn6w1lfVZKNTBdDansFBmtGnIsIapjI6xKR02Yc_2bQ8LZbUXSGm6Ry6_BG7PrtLZtj_dfCTj92xNGed-CrKqjG7nPBjNIL016GGvuS5gSvPRUxWES3VYfm1wl7WTI7jn-Pcb6M-buCgHhFOzTQxod27L9CqnOLio7N3gZAGpsp6n1-AJBOCJckcyXe2c6uD0srOJeZlKUm2eTDVMf8IehDVI0r1QOnTV6KzzAI3OY87Vd_cVMQ"
    TEST_PROVIDER_ID = "www.amazon.com"

    @mock_aws
    def test_conn_with_credentials(self):
        from pydynamodb import connect

        # assume_role
        conn = connect(
            region_name=TestConnectionWithSTS.TEST_REGION_NAME,
            role_arn=TestConnectionWithSTS.TEST_ROLE_ARN,
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        # assume_role with ExternalId
        conn = connect(
            region_name=TestConnectionWithSTS.TEST_REGION_NAME,
            role_arn=TestConnectionWithSTS.TEST_ROLE_ARN,
            external_id=TestConnectionWithSTS.TEST_EXTERNAL_ID,
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        # assume_role with ExternalId and MFA
        conn = connect(
            region_name=TestConnectionWithSTS.TEST_REGION_NAME,
            role_arn=TestConnectionWithSTS.TEST_ROLE_ARN,
            external_id=TestConnectionWithSTS.TEST_EXTERNAL_ID,
            serial_number=TestConnectionWithSTS.TEST_SERIAL_NUMBER,
            token_code=TestConnectionWithSTS.TEST_TOKEN_CODE,
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        # assume_role_with_saml
        conn = connect(
            region_name=TestConnectionWithSTS.TEST_REGION_NAME,
            role_arn=TestConnectionWithSTS.TEST_ROLE_ARN,
            principal_arn=TestConnectionWithSTS.TEST_PRINCIPAL_ARN,
            saml_assertion=TestConnectionWithSTS.TEST_SAML_ASSERTION,
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        # assume_role_with_web_identity
        conn = connect(
            region_name=TestConnectionWithSTS.TEST_REGION_NAME,
            role_arn=TestConnectionWithSTS.TEST_ROLE_ARN,
            web_identity_token=TestConnectionWithSTS.TEST_WEB_IDENTITY_TOKEN,
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

        conn = connect(
            region_name=TestConnectionWithSTS.TEST_REGION_NAME,
            role_arn=TestConnectionWithSTS.TEST_ROLE_ARN,
            web_identity_token=TestConnectionWithSTS.TEST_WEB_IDENTITY_TOKEN,
            provider_id=TestConnectionWithSTS.TEST_PROVIDER_ID,
        )
        assert conn._session_kwargs["aws_access_key_id"] is not None
        assert conn._session_kwargs["aws_secret_access_key"] is not None
        assert conn._session_kwargs["aws_session_token"] is not None

    @mock_aws
    def test_conn_with_sqlalchemy(self):
        from pydynamodb import sqlalchemy_dynamodb  # noqa
        import sqlalchemy  # noqa

        CONN_STR_PREFIX = "dynamodb://dummy_access_key:dummy_secret"
        CONN_STR_URL = "@dynamodb.us-east-1.amazonaws.com:443"
        CONN_STR_PARAM = "?role_arn=" + TestConnectionWithSTS.TEST_ROLE_ARN

        conn_str = CONN_STR_PREFIX + CONN_STR_URL + CONN_STR_PARAM
        engine = sqlalchemy.engine.create_engine(conn_str, echo=True)
        with contextlib.closing(engine.connect()) as conn:
            assert conn is not None

        conn_str = (
            CONN_STR_PREFIX
            + CONN_STR_URL
            + CONN_STR_PARAM
            + "&external_id="
            + TestConnectionWithSTS.TEST_EXTERNAL_ID
            + "&serial_number="
            + TestConnectionWithSTS.TEST_SERIAL_NUMBER
            + "&token_code="
            + TestConnectionWithSTS.TEST_TOKEN_CODE
        )
        engine = sqlalchemy.engine.create_engine(conn_str, echo=True)
        with contextlib.closing(engine.connect()) as conn:
            assert conn is not None

        conn_str = (
            CONN_STR_PREFIX
            + CONN_STR_URL
            + CONN_STR_PARAM
            + "&principal_arn="
            + TestConnectionWithSTS.TEST_PRINCIPAL_ARN
            + "&saml_assertion="
            + TestConnectionWithSTS.TEST_SAML_ASSERTION
        )
        engine = sqlalchemy.engine.create_engine(conn_str, echo=True)
        with contextlib.closing(engine.connect()) as conn:
            assert conn is not None

        conn_str = (
            CONN_STR_PREFIX
            + CONN_STR_URL
            + CONN_STR_PARAM
            + "&web_identity_token="
            + TestConnectionWithSTS.TEST_WEB_IDENTITY_TOKEN
        )
        engine = sqlalchemy.engine.create_engine(conn_str, echo=True)
        with contextlib.closing(engine.connect()) as conn:
            assert conn is not None

        conn_str = (
            CONN_STR_PREFIX
            + CONN_STR_URL
            + CONN_STR_PARAM
            + "&web_identity_token="
            + TestConnectionWithSTS.TEST_WEB_IDENTITY_TOKEN
            + "&provider_id="
            + TestConnectionWithSTS.TEST_PROVIDER_ID
        )
        engine = sqlalchemy.engine.create_engine(conn_str, echo=True)
        with contextlib.closing(engine.connect()) as conn:
            assert conn is not None
