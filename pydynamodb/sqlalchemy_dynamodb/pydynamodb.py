# -*- coding: utf-8 -*-
import re
from ..util import strtobool

import pydynamodb
from pydynamodb.error import OperationalError
from ..sql.common import RESERVED_WORDS

import botocore
from sqlalchemy import exc, types, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import crud
from sqlalchemy.sql.compiler import (
    IdentifierPreparer,
    DDLCompiler,
    GenericTypeCompiler,
    SQLCompiler,
)


class DynamoDBIdentifierPreparer(IdentifierPreparer):
    reserved_words = RESERVED_WORDS


class DynamoDBDDLCompiler(DDLCompiler):
    def __init__(
        self,
        dialect,
        statement,
        schema_translate_map=None,
        compile_kwargs=None,
    ):
        exc.CompileError("DDL statement is not supported by DDB PartiQL.")


class DynamoDBStatementCompiler(SQLCompiler):
    # DynamoDB can't guarantee the column orders of result
    # _textual_ordered_columns: bool = True
    _ordered_columns: bool = False

    def visit_column(
        self,
        column,
        add_to_result_map=None,
        include_table: bool = True,
        result_map_targets=(),
        ambiguous_table_name_map=None,
        **kwargs,
    ) -> str:
        return super().visit_column(
            column=column,
            add_to_result_map=add_to_result_map,
            include_table=False,
            result_map_targets=result_map_targets,
            ambiguous_table_name_map=ambiguous_table_name_map,
            kwargs=kwargs,
        )

    def visit_label(
        self,
        label,
        add_to_result_map=None,
        within_label_clause=False,
        within_columns_clause=False,
        render_label_as_label=None,
        result_map_targets=(),
        **kw,
    ):
        return super().visit_label(
            label=label,
            add_to_result_map=add_to_result_map,
            within_label_clause=within_label_clause,
            within_columns_clause=False,
            render_label_as_label=render_label_as_label,
            result_map_targets=result_map_targets,
            kw=kw,
        )

    def limit_clause(self, select, **kw):
        if hasattr(select, "_simple_int_clause"):
            limit_clause = select._limit_clause
            if limit_clause is not None and select._simple_int_clause(limit_clause):
                return f" LIMIT {self.process(limit_clause.render_literal_execute(), **kw)}"
        else:
            if select._limit_clause is not None:
                return " LIMIT " + self.process(select._limit_clause, **kw)
        return ""

    def visit_insert(self, insert_stmt, **kw):
        compile_state = insert_stmt._compile_state_factory(insert_stmt, self, **kw)
        insert_stmt = compile_state.statement

        toplevel = not self.stack

        if toplevel:
            self.isinsert = True
            if not self.dml_compile_state:
                self.dml_compile_state = compile_state
            if not self.compile_state:
                self.compile_state = compile_state

        self.stack.append(
            {
                "correlate_froms": set(),
                "asfrom_froms": set(),
                "selectable": insert_stmt,
            }
        )

        crud_params = crud._get_crud_params(self, insert_stmt, compile_state, **kw)

        if (
            not crud_params
            and not self.dialect.supports_default_values
            and not self.dialect.supports_default_metavalue
            and not self.dialect.supports_empty_insert
        ):
            raise exc.CompileError(
                "The '%s' dialect with current database "
                "version settings does not support empty "
                "inserts." % self.dialect.name
            )

        preparer = self.preparer

        text = "INSERT "

        if insert_stmt._prefixes:
            text += self._generate_prefixes(insert_stmt, insert_stmt._prefixes, **kw)

        text += "INTO "
        table_text = preparer.format_table(insert_stmt.table)

        if insert_stmt._hints:
            _, table_text = self._setup_crud_hints(insert_stmt, table_text)

        if insert_stmt._independent_ctes:
            for cte in insert_stmt._independent_ctes:
                cte._compiler_dispatch(self, **kw)

        text += table_text

        if self.returning or insert_stmt._returning:
            returning_clause = self.returning_clause(
                insert_stmt, self.returning or insert_stmt._returning
            )

            if self.returning_precedes_values:
                text += " " + returning_clause
        else:
            returning_clause = None

        if insert_stmt.select is not None:
            raise exc.CompileError(
                "The '%s' dialect with current database "
                "version settings does not support "
                "select statement in insert." % self.dialect.name
            )
        else:
            insert_single_values_expr = ", ".join(
                ["'%s': %s" % (expr, value) for c, expr, value in crud_params]
            )
            text += " VALUE {%s}" % insert_single_values_expr
            if toplevel:
                self.insert_single_values_expr = insert_single_values_expr

        if insert_stmt._post_values_clause is not None:
            post_values_clause = self.process(insert_stmt._post_values_clause, **kw)
            if post_values_clause:
                text += " " + post_values_clause

        if returning_clause and not self.returning_precedes_values:
            text += " " + returning_clause

        if self.ctes and not self.dialect.cte_follows_insert:
            nesting_level = len(self.stack) if not toplevel else None
            text = (
                self._render_cte_clause(
                    nesting_level=nesting_level, include_following_stack=True
                )
                + text
            )

        self.stack.pop(-1)

        return text

    def visit_update(self, update_stmt, **kw):
        update_stmt._returning = "*"
        return super().visit_update(update_stmt, **kw)

    def visit_delete(self, delete_stmt, **kw):
        delete_stmt._returning = "*"
        return super().visit_delete(delete_stmt, **kw)

    def returning_clause(self, stmt, returning_cols):
        if stmt.is_update or stmt.is_delete:
            return "RETURNING ALL OLD *"


class DynamoDBTypeCompiler(GenericTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        return self.visit_REAL(type_, **kw)

    def visit_REAL(self, type_, **kw):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        return self.visit_REAL(type_, **kw)

    def visit_DECIMAL(self, type_, **kw):
        return self.visit_REAL(type_, **kw)

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_SMALLINT(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "INTEGER"

    def visit_CLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_NCLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_JSON(self, type_, **kw):
        return "JSON"


class DynamoDBDialect(DefaultDialect):
    name = "dynamodb"
    driver = ""
    preparer = DynamoDBIdentifierPreparer
    statement_compiler = DynamoDBStatementCompiler
    ddl_compiler = DynamoDBDDLCompiler
    type_compiler = DynamoDBTypeCompiler
    default_paramstyle = pydynamodb.paramstyle
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = False
    supports_native_decimal = False
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_statement_cache = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False

    _connect_options = dict()  # type: ignore

    @classmethod
    def dbapi(cls):
        return pydynamodb

    def _raw_connection(self, connection):
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def create_connect_args(self, url):
        # Connection string format:
        #   dynamodb://
        #   {aws_access_key_id}:{aws_secret_access_key}@dynamodb.{region_name}.amazonaws.com:443
        #   ?verify=false&...
        self._connect_options = self._create_connect_args(url)
        return [[], self._connect_options]

    def _create_connect_args(self, url):
        opts = {
            "aws_access_key_id": url.username if url.username else None,
            "aws_secret_access_key": url.password if url.password else None,
            "region_name": re.sub(
                r"^dynamodb\.([a-z0-9-]+)\.amazonaws\.(com|com.cn)$", r"\1", url.host
            ),
            "schema_name": url.database if url.database else "default",
        }
        opts.update(url.query)
        if "verify" in opts:
            verify = opts["verify"]
            try:
                verify = bool(strtobool(verify))
            except ValueError:
                # Probably a file name of the CA cert bundle to use
                pass
            opts.update({"verify": verify})

        return opts

    def do_ping(self, dbapi_connection):
        return dbapi_connection.test_connection()

    def get_schema_names(self, connection, **kw):
        # DynamoDB does not have the concept of a schema
        return ["default"]

    @reflection.cache
    def _get_tables(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        with raw_connection.connection.cursor() as cursor:
            return cursor.list_tables()

    def _get_column_type(self, metadata, attribute_name) -> str:
        col_type = types.NullType

        for attr in metadata["AttributeDefinitions"]:
            if attr["AttributeName"] == attribute_name:
                type_ = attr["AttributeType"]
                if type_ == "S":
                    col_type = types.String
                elif type_ == "N":
                    col_type = types.Numeric
                elif type_ == "B":
                    col_type = types.BINARY
                else:
                    util.warn(f"Did not recognize type '{type_}'")
                    col_type = types.NullType
        return col_type()

    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            columns = self.get_columns(connection, table_name, schema)
            return True if columns else False
        except exc.NoSuchTableError:
            return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        with raw_connection.connection.cursor() as cursor:
            try:
                metadata_ = cursor.get_table_metadata(table_name)

                columns = [
                    {
                        "name": c["AttributeName"],
                        "type": self._get_column_type(metadata_, c["AttributeName"]),
                        "nullable": False,
                        "default": None,
                        "autoincrement": False,
                        "comment": None,
                        "dialect_options": {},
                    }
                    for c in metadata_["KeySchema"]
                ]

                return columns
            except OperationalError as e:
                cause = e.__cause__
                if (
                    isinstance(cause, botocore.exceptions.ClientError)
                    and cause.response["Error"]["Code"] == "ResourceNotFoundException"
                ):
                    raise exc.NoSuchTableError(table_name) from e
                raise

    def get_table_names(self, connection, schema=None, **kw):
        return self._get_tables(connection, schema, **kw)

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # DynamoDB has no support for foreign keys.
        return []  # pragma: no cover

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # DynamoDB has no support for primary keys.
        return []  # pragma: no cover

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # DynamoDB has no support for indexes.
        return []  # pragma: no cover

    def do_rollback(self, dbapi_connection):
        # No transactions for DynamoDB
        pass  # pragma: no cover

    def _check_unicode_returns(self, connection, additional_tests=None):
        # Requests gives back Unicode strings
        return True  # pragma: no cover

    def _check_unicode_description(self, connection):
        # Requests gives back Unicode strings
        return True  # pragma: no cover


class DynamoDBRestDialect(DynamoDBDialect):
    driver = "rest"
    supports_statement_cache = True
