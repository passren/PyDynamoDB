# -*- coding: utf-8 -*-
import os
import sqlite3
from typing import Any, Type

from pydynamodb.sql.parser import SQLParser
from pydynamodb.sql.common import QueryType
from pydynamodb.superset_dynamodb.dml_select import SupersetSelect
from sqlalchemy.sql import text
from pydynamodb.superset_dynamodb.querydb import QueryDB, QueryDBConfig
from pydynamodb.model import Statement

TESTCASE04_TABLE = "pydynamodb_test_case04"


class TestSupersetDynamoDB:
    def test_parse_select(self):
        sql = """
            SELECT col_list[1], col_map.A FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert parser.query_type == QueryType.SELECT
        assert len(parser.parser.columns) == 2
        assert ret == {
            "Statement": "SELECT col_list[1],col_map.A FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_1(self):
        sql = """
            SELECT "col_list[1]", min("A"), max(A) FROM (
                SELECT col_list[1], col_map.A FROM Issues WHERE key_partition='row_1'
            ) AS virtual_table
            GROUP BY "col_list[1]","A"
            ORDER BY "AVG(A)" DESC
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        CLOUMN_LIST_1 = "col_list[1]"
        assert parser.query_type == QueryType.SELECT
        assert len(parser.parser.columns) == 2
        assert parser.parser.columns[0].request_name == CLOUMN_LIST_1
        assert parser.parser.columns[0].result_name == CLOUMN_LIST_1
        assert parser.parser.columns[1].request_name == "col_map.A"
        assert parser.parser.columns[1].result_name == "A"
        assert parser.parser.outer_columns == '"col_list[1]", min("A"), max(A)'
        assert (
            parser.parser.outer_exprs
            == 'AS virtual_table GROUP BY "col_list[1]","A" ORDER BY "AVG(A)" DESC'
        )
        assert ret == {
            "Statement": "SELECT col_list[1],col_map.A FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_2(self):
        sql = """
            SELECT DATETIME("col_datetime", 'start of day'),
                "col_str", max("col_num"), count(DISTINCT "col_num"),
                count("col_num"), min("col_num"), sum("col_num")
            FROM
            (
                SELECT col_str, DATETIME(col_datetime), NUMBER(col_num)
                FROM Issues WHERE key_partition='row_1'
            ) AS virtual_table
            GROUP BY "col_str", DATETIME("col_datetime", 'start of day')
            ORDER BY "MAX(col_num)" DESC LIMIT 10000
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert len(parser.parser.columns) == 3
        assert (
            parser.parser.outer_columns
            == "DATETIME(\"col_datetime\", 'start of day'), "
            + '"col_str", max("col_num"), count(DISTINCT "col_num"), '
            + 'count("col_num"), min("col_num"), sum("col_num")'
        )
        assert ret == {
            "Statement": "SELECT col_str,col_datetime,col_num "
            + "FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_3(self):
        sql = """
            SELECT SUBSTR(col_str, 1, 2) col_str_1, REPLACE(col_str, '-', '_') col_str_2, 
                DATETIME(col_datetime), NUMBER(col_num)
            FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert len(parser.parser.columns) == 4
        assert ret == {
            "Statement": "SELECT col_str,col_datetime,col_num "
            + "FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_nested_select_case_4(self):
        sql = """
            SELECT text_substring(col_str, 1, 2) col_str_1, text_replace(col_str, '-', '_', 1) col_str_2, 
                DATETIME(col_datetime), NUMBER(col_num)
            FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql, parser_class=SupersetSelect)
        ret = parser.transform()
        assert len(parser.parser.columns) == 4
        assert ret == {
            "Statement": "SELECT col_str,col_datetime,col_num "
            + "FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_insert_nested_data(self, cursor):
        sql = (
            """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_list': ?, 'col_map': ?
            }
        """
            % TESTCASE04_TABLE
        )
        params_1 = [
            "row_1",
            1,
            ["A", "B", {"A": 1, "B": 2}],
            {"A": 1, "B": ["B-1", "B-2"]},
        ]
        params_2 = [
            "row_1",
            2,
            ["C", "D", {"C": 3, "D": 4}],
            {"A": 2, "B": ["D-1", "D-2"]},
        ]
        params_3 = [
            "row_1",
            3,
            ["E", "F", {"E": 1, "F": 2}],
            {"A": 3, "B": ["F-1", "F-2"]},
        ]
        params_4 = [
            "row_1",
            4,
            ["E", "B", {"E": 1, "F": 2}],
            {"A": 4, "B": ["F-1", "F-2"]},
        ]
        params_5 = [
            "row_1",
            5,
            ["E", "B", {"E": 1, "F": 2}],
            {"A": 5, "B": ["F-1", "F-2"]},
        ]
        params_6 = [
            "row_1",
            6,
            ["E", "F", {"E": 1, "F": 2}],
            {"A": 6, "B": ["F-1", "F-2"]},
        ]
        cursor.executemany(
            sql, [params_1, params_2, params_3, params_4, params_5, params_6]
        )

    def test_insert_flat_data(self, cursor):
        from datetime import date, datetime, timedelta

        sql = (
            """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?,
                'col_date': ?, 'col_datetime': ?,
                'col_int': ?, 'col_float': ?,
                'col_bytes': ?, 'col_str': ?,
                'col_bool': ?
            }
        """
            % TESTCASE04_TABLE
        )
        date_ = date(2022, 9, 20)
        datetime_ = datetime(2022, 10, 20, 10, 23, 40)
        params_1 = ["row_2", 1, date_, datetime_, 1, 1.1, b"RP", "RP", True]
        params_2 = [
            "row_2",
            2,
            date_ + timedelta(days=1),
            datetime_ + timedelta(hours=1),
            2,
            2.2,
            b"RP",
            "RP",
            False,
        ]
        params_3 = [
            "row_2",
            3,
            date_ + timedelta(days=2),
            datetime_ + timedelta(hours=2),
            3,
            3.3,
            b"RP",
            "RP",
            True,
        ]
        params_4 = [
            "row_2",
            4,
            date_ + timedelta(days=3),
            datetime_ + timedelta(hours=3),
            4,
            4.4,
            b"RP",
            "RP",
            True,
        ]
        params_5 = [
            "row_2",
            5,
            date_ + timedelta(days=4),
            datetime_ + timedelta(hours=4),
            5,
            5.5,
            b"RP1",
            "RP1",
            False,
        ]
        params_6 = [
            "row_2",
            6,
            date_ + timedelta(days=5),
            datetime_ + timedelta(hours=5),
            6,
            6.6,
            b"RP1",
            "RP1",
            False,
        ]
        params_7 = [
            "row_2",
            7,
            date_ + timedelta(days=6),
            datetime_ + timedelta(hours=6),
            7,
            7.7,
            b"RP2",
            "RP2",
            True,
        ]
        params_8 = [
            "row_2",
            8,
            date_ + timedelta(days=7),
            datetime_ + timedelta(hours=7),
            8,
            8.8,
            b"RP2",
            "RP2",
            False,
        ]
        cursor.executemany(
            sql,
            [
                params_1,
                params_2,
                params_3,
                params_4,
                params_5,
                params_6,
                params_7,
                params_8,
            ],
        )

    def test_execute_select(self, superset_cursor):
        superset_cursor.execute(
            """
            SELECT col_list[1], NUMBER(col_map.A)
                FROM %s WHERE key_partition='row_1'
        """
            % TESTCASE04_TABLE
        )
        ret = superset_cursor.fetchall()
        assert len(ret) == 6
        assert [(d[0], d[1]) for d in superset_cursor.description] == [
            ("col_list[1]", "STRING"),
            ("A", "NUMBER"),
        ]

    def test_execute_nested_select(self, superset_cursor):
        superset_cursor.execute(
            """
            SELECT "col_list_1", SUM("A") FROM (
                SELECT col_list[1] col_list_1, NUMBER(col_map.A) A
                FROM %s WHERE key_partition='row_1'
            )
        """
            % TESTCASE04_TABLE
        )
        ret = superset_cursor.fetchall()
        assert len(ret) == 1
        assert ret == [("B", 21.0)]

    def test_execute_group_select(self, superset_cursor):
        superset_cursor.execute(
            """
            SELECT "col_list_1", SUM("A"), COUNT("A") FROM (
                SELECT col_list[1] col_list_1, NUMBER(col_map.A)
                FROM %s WHERE key_partition='row_1'
            )
            GROUP BY "col_list_1"
            ORDER BY "col_list_1" DESC
        """
            % TESTCASE04_TABLE
        )
        ret = superset_cursor.fetchall()
        assert len(ret) == 3
        assert ret == [("F", 9.0, 2), ("D", 2.0, 1), ("B", 10.0, 3)]

    def test_string_functions_select(self, superset_cursor):
        superset_cursor.execute(
            """
            SELECT "col_list_1", "col_map_B_1_substr" FROM (
                SELECT col_list[1] col_list_1, SUBSTR(col_map.B[1], 1, 1) col_map_B_1_substr
                FROM %s WHERE key_partition='row_1'
            )
            ORDER BY "col_list_1" DESC
        """
            % TESTCASE04_TABLE
        )
        ret = superset_cursor.fetchall()
        assert len(ret) == 6
        assert ret[0] == ("F", "F")

        superset_cursor.execute(
            """
            SELECT "col_list_1", "col_map_B_1_replace" FROM (
                SELECT col_list[1] col_list_1, REPLACE(col_map.B[1], '-', '_') col_map_B_1_replace
                FROM %s WHERE key_partition='row_1'
            )
            ORDER BY "col_list_1" DESC
        """
            % TESTCASE04_TABLE
        )
        ret = superset_cursor.fetchall()
        assert len(ret) == 6
        assert ret[0] == ("F", "F_2")

        superset_cursor.execute(
            """
            SELECT "col_list_1", "col_map_B_1", "col_map_B_1_substr", "col_map_B_1_replace" FROM (
                SELECT col_list[1] col_list_1, col_map.B[1] col_map_B_1,
                SUBSTR(col_map.B[1], 1, 1) col_map_B_1_substr,
                REPLACE(col_map.B[1], '-', '_') col_map_B_1_replace
                FROM %s WHERE key_partition='row_1'
            )
            ORDER BY "col_list_1" DESC
        """
            % TESTCASE04_TABLE
        )
        ret = superset_cursor.fetchall()
        assert len(ret) == 6
        assert ret[0] == ("F", "F-2", "F", "F_2")

        superset_cursor.execute(
            """
            SELECT "col_list_1", "col_map_B_1_trim", "col_map_B_1_upper", "col_map_B_1_lower" FROM (
                SELECT col_list[1] col_list_1,
                TRIM(col_map.B[1]) col_map_B_1_trim,
                UPPER(col_map.B[1]) col_map_B_1_upper,
                lower(col_map.B[1]) col_map_B_1_lower
                FROM %s WHERE key_partition='row_1'
            )
            ORDER BY "col_list_1" DESC
        """
            % TESTCASE04_TABLE
        )
        ret = superset_cursor.fetchall()
        assert len(ret) == 6
        assert ret[0] == ("F", "F-2", "F-2", "f-2")

    def test_sqlalchemy_execute_nested_select(self, superset_engine):
        _, conn = superset_engine
        rows = conn.execute(
            text(
                """
            SELECT "col_list_1", SUM("A"), COUNT("A") FROM (
                SELECT col_list[1] col_list_1, NUMBER(col_map.A)
                FROM %s WHERE key_partition=:pk
            ) AS virtual_table
            GROUP BY "col_list_1"
            ORDER BY "col_list_1" DESC
            """
                % TESTCASE04_TABLE
            ),
            {"pk": "row_1"},
        ).fetchall()
        assert len(rows) == 3

    def test_sqlalchemy_execute_flat_data(self, superset_engine):
        _, conn = superset_engine
        rows = conn.execute(
            text(
                """
            SELECT "col_str", MAX("col_date"), MIN("col_datetime"),
                    SUM("col_int"), MAX("col_float"),
                    COUNT("key_sort"), COUNT("col_bool")
            FROM (
                SELECT key_sort, DATE(col_date), DATETIME(col_datetime),
                NUMBER(col_int), NUMBER(col_float), col_bytes,
                col_str, BOOL(col_bool)
                FROM %s WHERE key_partition=:pk
            ) AS virtual_table
            GROUP BY "col_str"
            ORDER BY "col_str" DESC
            """
                % TESTCASE04_TABLE
            ),
            {"pk": "row_2"},
        ).fetchall()
        assert len(rows) == 3
        assert rows == [
            ("RP2", "2022-09-27", "2022-10-20 16:23:40", 15.0, 8.8, 2, 2),
            ("RP1", "2022-09-25", "2022-10-20 14:23:40", 11.0, 6.6, 2, 2),
            ("RP", "2022-09-23", "2022-10-20 10:23:40", 10.0, 4.4, 4, 4),
        ]

    def test_sqlalchemy_execute_alias_select(self, superset_engine):
        _, conn = superset_engine
        rows = conn.execute(
            text(
                """
            SELECT "LST", SUM("MAP_A"), COUNT("MAP_A") FROM (
                SELECT col_list[1] LST, NUMBER(col_map.A) MAP_A
                FROM %s WHERE key_partition=:pk
            ) AS virtual_table
            GROUP BY "LST"
            ORDER BY "LST" DESC
            """
                % TESTCASE04_TABLE
            ),
            {"pk": "row_1"},
        ).fetchall()
        assert len(rows) == 3

    def test_cached_querydb_step1(self, superset_engine):
        os.environ["PYDYNAMODB_QUERYDB_TYPE"] = "sqlite"
        os.environ["PYDYNAMODB_QUERYDB_URL"] = "query.db"
        os.environ["PYDYNAMODB_QUERYDB_LOAD_BATCH_SIZE"] = "20"
        os.environ["PYDYNAMODB_QUERYDB_EXPIRE_TIME"] = "3"

        self.test_sqlalchemy_execute_nested_select(superset_engine)
        self.test_sqlalchemy_execute_flat_data(superset_engine)
        self.test_sqlalchemy_execute_alias_select(superset_engine)

    def test_cached_querydb_step2(self, cursor):
        # Insert more raw data
        sql = (
            """
        INSERT INTO %s VALUE {
                'key_partition': ?, 'key_sort': ?, 'col_list': ?, 'col_map': ?
            }
        """
            % TESTCASE04_TABLE
        )
        params_1 = [
            "row_1",
            7,
            ["A", "G", {"A": 1, "B": 2}],
            {"A": 7, "B": ["B-1", "B-2"]},
        ]
        params_2 = [
            "row_1",
            8,
            ["C", "G", {"C": 3, "D": 4}],
            {"A": 8, "B": ["D-1", "D-2"]},
        ]
        cursor.executemany(sql, [params_1, params_2])

    def test_cached_querydb_step3(self, superset_engine):
        # Cache used
        self.test_sqlalchemy_execute_nested_select(superset_engine)
        self.test_sqlalchemy_execute_alias_select(superset_engine)

    def test_cached_querydb_step4(self, superset_engine):
        import time

        time.sleep(15)
        # Cache expired
        self.query_final_cached_querydb(superset_engine)

    def test_purge_querydb_table(self, superset_engine):
        os.environ["PYDYNAMODB_QUERYDB_PURGE_ENABLED"] = "false"
        self.query_final_cached_querydb(superset_engine)

        os.environ["PYDYNAMODB_QUERYDB_PURGE_ENABLED"] = "true"
        os.environ["PYDYNAMODB_QUERYDB_PURGE_TIME"] = "14"
        self.query_final_cached_querydb(superset_engine)

    def query_final_cached_querydb(self, superset_engine):
        _, conn = superset_engine
        rows = conn.execute(
            text(
                """
            SELECT "LST", SUM("MAP_A"), COUNT("MAP_A") FROM (
                SELECT col_list[1] LST, NUMBER(col_map.A) MAP_A
                FROM %s WHERE key_partition=:pk
            ) AS virtual_table
            GROUP BY "LST"
            ORDER BY "LST" DESC
            """
                % TESTCASE04_TABLE
            ),
            {"pk": "row_1"},
        ).fetchall()
        assert len(rows) == 4

    def test_custom_querydb(self, superset_engine):
        import os

        os.environ["PYDYNAMODB_QUERYDB_TYPE"] = "custom_sqlite"
        if os.environ.get("PYDYNAMODB_QUERYDB_CLASS"):
            os.environ.pop("PYDYNAMODB_QUERYDB_CLASS")
        try:
            self.query_final_cached_querydb(superset_engine)
        except Exception as e:
            assert "pydynamodb.error.NotSupportedError" in str(e)

        os.environ[
            "PYDYNAMODB_QUERYDB_CLASS"
        ] = "tests.test_superset_dynamodb.CustomQueryDB"
        try:
            self.query_final_cached_querydb(superset_engine)
        except Exception as e:
            assert "QueryDB class is invalid." in str(e)

        os.environ[
            "PYDYNAMODB_QUERYDB_CLASS"
        ] = "tests.test_superset_dynamodb:CustomQueryDB"
        os.environ["PYDYNAMODB_QUERYDB_URL"] = ":memory:"

        self.query_final_cached_querydb(superset_engine)


class CustomQueryDB(QueryDB):
    def __init__(
        self,
        statement: Statement,
        config: QueryDBConfig,
        **kwargs,
    ) -> None:
        super().__init__(statement, config, **kwargs)
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = sqlite3.connect(self.config.db_url)

        return self._connection

    def has_cache(self) -> bool:
        return False

    def type_conversion(self, type: Type[Any]) -> str:
        return "TEXT"

    def has_table(self, table: str) -> bool:
        return False
