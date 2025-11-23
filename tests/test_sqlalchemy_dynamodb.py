# -*- coding: utf-8 -*-
from sqlalchemy.sql import text, select
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy import Integer, String, Numeric, JSON
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

TESTCASE02_TABLE = "pydynamodb_test_case02"
USER_TABLE = "user"


# Declarative Mapping
class _TestCase02(Base):
    __tablename__ = TESTCASE02_TABLE

    key_partition = Column(String, primary_key=True)
    key_sort = Column(Integer, primary_key=True)
    col_str = Column(String)
    col_num = Column(Numeric)
    col_nested = Column()
    col_json = Column(JSON)
    col_json_array = Column(JSON)

class _User(Base):
    __tablename__ = USER_TABLE

    key_partition = Column(String, primary_key=True)
    key_sort = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)
    default = Column(Numeric)
    comment = Column(String)


class TestSQLAlchemyDynamoDB:
    def test_ping(self, engine):
        engine, conn = engine
        conn = engine.raw_connection().dbapi_connection
        assert engine.dialect.do_ping(conn)

    def test_basic_insert(self, engine):
        engine, conn = engine

        sql_one_row_1_0_ = (
            """
        INSERT INTO %s VALUE {
            'key_partition': :pk, 'key_sort': :sk,
            'col_str': :col1, 'col_num': :col2, 'col_byte': :col3
        }
        """
            % TESTCASE02_TABLE
        )
        params_1_0_ = {
            "pk": "test_one_row_1",
            "sk": 0,
            "col1": "test case 0",
            "col2": 0,
            "col3": b"0",
        }
        conn.execute(text(sql_one_row_1_0_), params_1_0_)

    def test_batch_insert(self, engine):
        engine, conn = engine

        sql_many_rows_ = (
            """
        INSERT INTO %s VALUE {
            'key_partition': :pk, 'key_sort': :sk,
            'col_str': :col1, 'col_num': :col2, 'col_byte': :col3
        }
        """
            % TESTCASE02_TABLE
        )
        params_ = [
            {
                "pk": "test_many_rows_1",
                "sk": 1,
                "col1": "test case many 1",
                "col2": 1,
                "col3": b"1",
            },
            {
                "pk": "test_many_rows_1",
                "sk": 2,
                "col1": "test case many 2",
                "col2": 2,
                "col3": b"2",
            },
            {
                "pk": "test_many_rows_1",
                "sk": 3,
                "col1": "test case many 3",
                "col2": 3,
                "col3": b"3",
            },
            {
                "pk": "test_many_rows_1",
                "sk": 4,
                "col1": "test case many 4",
                "col2": 4,
                "col3": b"4",
            },
        ]
        conn.execute(text(sql_many_rows_), params_)

    def test_nested_data_insert(self, engine):
        engine, conn = engine

        sql_one_row_2_0_ = (
            """
        INSERT INTO "%s" VALUE {
            'key_partition': :pk, 'key_sort': :sk,
            'col_str': :col1, 'col_nested': :col2, 'col_json': :col3
        }
        """
            % TESTCASE02_TABLE
        )
        nested_data = {
            "Key1": ["Val1-1", 1, {"Subkey1": "Val1-1"}],
            "Key2": {"Val2-1", "Val2-2"},
            "Key3": "Val3",
        }
        params_2_0_ = {
            "pk": "test_one_row_2",
            "sk": 0,
            "col1": "test case nested 0",
            "col2": nested_data,
            "col3": nested_data,
        }
        conn.execute(text(sql_one_row_2_0_), params_2_0_)

        rows = conn.execute(
            text(
                """
            SELECT col_nested, col_json FROM %s WHERE key_partition = :pk
            AND key_sort = :sk
            """
                % TESTCASE02_TABLE
            ),
            {"pk": "test_one_row_2", "sk": 0},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == nested_data
        assert rows[0][1] == nested_data

    def test_declarative_table_insert(self, engine):
        engine, conn = engine

        with Session(engine) as session:
            for i in range(0, 5):
                test_case02 = _TestCase02()
                test_case02.key_partition = "test_one_row_3"
                test_case02.key_sort = i
                test_case02.col_str = "test case declarative table " + str(i)
                test_case02.col_num = i
                test_case02.col_json = {"key": "value"}
                test_case02.col_json_array = [{"key1": "value1"}, {"key2": "value2"}]
                session.add(test_case02)
            session.commit()

        rows = conn.execute(
            text(
                """
            SELECT * FROM %s WHERE key_partition = :pk
            """
                % TESTCASE02_TABLE
            ),
            {"pk": "test_one_row_3"},
        ).fetchall()
        assert len(rows) == 5

    def test_declarative_table_update(self, engine):
        engine, conn = engine

        with Session(engine) as session:
            test_case = session.scalars(
                select(_TestCase02).where(
                    _TestCase02.key_partition == "test_one_row_3",
                    _TestCase02.key_sort == 1,
                )
            ).one()
            test_case.col_str = "test case declarative table 99"
            test_case.col_num = 99
            test_case.col_json = {"key": "value updated"}
            test_case.col_json_array = [{"key1": "value1 updated"}, {"key2": "value2 updated"}]
            session.commit()

        rows = conn.execute(
            text(
                """
            SELECT col_str, col_num, col_json, col_json_array FROM %s
            WHERE key_partition = :pk
            AND key_sort = :sk
            """
                % TESTCASE02_TABLE
            ),
            {"pk": "test_one_row_3", "sk": 1},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "test case declarative table 99"
        assert rows[0][1] == 99
        assert rows[0][2] == '{"key": "value updated"}'
        assert rows[0][3] == '[{"key1": "value1 updated"}, {"key2": "value2 updated"}]'

    def test_declarative_table_select(self, engine):
        engine, _ = engine

        with Session(engine) as session:
            test_case = session.scalars(
                select(_TestCase02).where(
                    _TestCase02.key_partition == "test_one_row_3",
                    _TestCase02.key_sort == 4,
                )
            ).one()
        assert test_case.key_partition == "test_one_row_3"
        assert test_case.key_sort == 4
        assert test_case.col_str == "test case declarative table 4"
        assert test_case.col_num == 4
        assert test_case.col_nested is None
        assert test_case.col_json == {"key": "value"}
        assert test_case.col_json_array == [{"key1": "value1"}, {"key2": "value2"}]

    def test_declarative_table_delete(self, engine):
        engine, _ = engine

        with Session(engine) as session:
            test_case = session.scalars(
                select(_TestCase02).where(
                    _TestCase02.key_partition == "test_one_row_3",
                    _TestCase02.key_sort == 4,
                )
            ).one()
            session.delete(test_case)
            session.commit()

    def test_basic_query(self, engine):
        engine, conn = engine
        rows = conn.execute(
            text(
                """
            SELECT * FROM %s WHERE key_partition = :pk
            """
                % TESTCASE02_TABLE
            ),
            {"pk": "test_one_row_2"},
        ).fetchall()
        assert len(rows) == 1

    def test_basic_update(self, engine):
        engine, conn = engine
        sql_one_row_1_0_ = (
            """
        UPDATE "%s"
        SET col_str=:col1
        SET col_num=:col2
        WHERE key_partition=:pk AND key_sort=:sk
        """
            % TESTCASE02_TABLE
        )
        params_1_0_ = {
            "pk": "test_one_row_1",
            "sk": 0,
            "col1": "test case update 0",
            "col2": 10,
        }
        conn.execute(text(sql_one_row_1_0_), params_1_0_)

        rows = conn.execute(
            text(
                """
            SELECT col_str FROM %s WHERE key_partition = :pk
            AND key_sort = :sk
            """
                % TESTCASE02_TABLE
            ),
            {"pk": "test_one_row_1", "sk": 0},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "test case update 0"

    def test_reflect_table(self, engine):
        engine, conn = engine
        table = Table(
            TESTCASE02_TABLE,
            MetaData(),
            Column("key_partition", String, nullable=False),
            Column("key_sort", Integer),
            Column("col_str", String),
            Column("col_num", Numeric),
            Column("col_json", JSON)
        )
        assert len(table.c) == 5

        rows = conn.execute(table.select()).fetchall()
        assert len(rows) == 10
        assert rows[7][4] == {"key": "value updated"}

        rows = conn.execute(
            table.select().where(
                table.c.key_partition == "test_many_rows_1", table.c.key_sort == 1
            )
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].key_partition == "test_many_rows_1"
        assert rows[0].key_sort == 1
        assert rows[0].col_str == "test case many 1"
        assert rows[0].col_num == 1

    def test_nested_data_in_reflect_table(self, engine):
        engine, conn = engine
        table = Table(
            TESTCASE02_TABLE,
            MetaData(),
            Column("key_partition", String, nullable=False),
            Column("key_sort", Integer),
            Column("col_str", String),
            Column("col_nested"),
        )
        rows = conn.execute(
            table.select().where(
                table.c.key_partition == "test_one_row_2", table.c.key_sort == 0
            )
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].col_str == "test case nested 0"
        assert rows[0].col_nested == {
            "Key1": ["Val1-1", 1, {"Subkey1": "Val1-1"}],
            "Key2": {"Val2-1", "Val2-2"},
            "Key3": "Val3",
        }

    def test_has_table_1(self, engine):
        engine, conn = engine
        table = Table(
            "NOT_EXISTED_TABLE",
            MetaData(),
            Column("key_partition", String, nullable=False),
        )
        try:
            conn.execute(table.select()).fetchall()
            assert False
        except Exception as e:
            assert e is not None

    def test_no_data_return(self, engine):
        engine, conn = engine
        table = Table(
            TESTCASE02_TABLE,
            MetaData(),
            Column("key_partition", String, nullable=False),
            Column("key_sort", Integer),
            Column("col_str", String),
        )
        rows = conn.execute(
            table.select().where(
                table.c.key_partition == "test_one_row_1", table.c.key_sort == 99
            )
        ).fetchall()
        assert len(rows) == 0

    def test_declarative_table(self, engine):
        engine, _ = engine

        with Session(engine) as session:
            rows1 = session.execute(
                select(_TestCase02).filter_by(key_partition="test_many_rows_1")
            ).all()
            assert len(rows1) == 4

            rows2 = session.execute(
                select(_TestCase02).filter_by(
                    key_partition="test_many_rows_1", key_sort=1
                )
            ).all()
            assert len(rows2) == 1
            assert rows2[0]._TestCase02.col_str == "test case many 1"
            assert rows2[0]._TestCase02.col_num == 1

            rows3 = session.execute(
                select(_TestCase02).filter_by(
                    key_partition="not_existed_row", key_sort=1
                )
            ).all()
            assert len(rows3) == 0

    def test_has_table_2(self, engine):
        engine, conn = engine

        assert engine.dialect.has_table(conn, TESTCASE02_TABLE)
        assert not engine.dialect.has_table(conn, "NOT_EXISTED_TABLE")

    def test_get_columns(self, engine):
        engine, conn = engine

        columns_ = engine.dialect.get_columns(conn, TESTCASE02_TABLE)
        assert len(columns_) == 2
        assert columns_[0]["name"] == "key_partition"
        assert isinstance(columns_[0]["type"], String)
        assert columns_[1]["name"] == "key_sort"
        assert isinstance(columns_[1]["type"], Numeric)

    def test_data_limit(self, engine):
        engine, conn = engine
        table = Table(
            TESTCASE02_TABLE,
            MetaData(),
            Column("key_partition", String, nullable=False),
            Column("key_sort", Integer),
            Column("col_str", String),
        )
        rows = conn.execute(table.select().limit(1)).fetchall()
        assert len(rows) == 1

    def test_reserved_word_table_insert(self, engine):
        engine, conn = engine

        with Session(engine) as session:
            for i in range(0, 5):
                user = _User()
                user.key_partition = "test_user_row_2"
                user.key_sort = i
                user.username = "user" + str(i)
                user.password = "pwd" + str(i)
                user.default = 1
                user.comment = "user account"
                session.add(user)
            session.commit()

        rows = conn.execute(
            text(
                """
            SELECT * FROM %s WHERE key_partition = :pk
            """
                % USER_TABLE
            ),
            {"pk": "test_user_row_2"},
        ).fetchall()
        assert len(rows) == 5

    def test_reserved_word_table_select(self, engine):
        engine, conn = engine

        with Session(engine) as session:
            user = session.scalars(
                    select(_User).where(
                        _User.key_partition == "test_user_row_2",
                        _User.key_sort == 2,
                    )
                ).one()

        assert user.username == "user2"
        assert user.password == "pwd2"


    def test_reserved_word_table_update(self, engine):
        engine, conn = engine

        with Session(engine) as session:
            user = session.scalars(
                select(_User).where(
                    _User.key_partition == "test_user_row_2",
                    _User.key_sort == 1,
                    _User.default == 1,
                )
            ).one_or_none()

            assert user is not None

            user.username = "user_updated"
            user.default = 0
            user.comment = "user account updated"
            session.commit()

        rows = conn.execute(
            text(
                """
            SELECT username, "default", "comment" FROM %s
            WHERE key_partition = :pk
            AND key_sort = :sk
            """
                % USER_TABLE
            ),
            {"pk": "test_user_row_2", "sk": 1},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "user_updated"
        assert rows[0][1] == 0
        assert rows[0][2] == "user account updated"

    def test_reserved_word_table_delete(self, engine):
        engine, conn = engine

        with Session(engine) as session:
            user = session.scalars(
                select(_User).where(
                    _User.key_partition == "test_user_row_2",
                    _User.key_sort == 0,
                    _User.default == 1,
                )
            ).one()
            session.delete(user)
            session.commit()

        rows = conn.execute(
            text(
                """
            SELECT username, "default", "comment" FROM %s
            WHERE key_partition = :pk
            AND key_sort = :sk
            """
                % USER_TABLE
            ),
            {"pk": "test_user_row_2", "sk": 0},
        ).fetchall()
        assert len(rows) == 0
