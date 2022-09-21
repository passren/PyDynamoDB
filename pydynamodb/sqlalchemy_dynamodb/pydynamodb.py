# -*- coding: utf-8 -*-
import re
from distutils.util import strtobool

import pydynamodb
from ..sqlalchemy_dynamodb import RESERVED_WORDS

from sqlalchemy import exc, schema, types, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect
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
        exc.CompileError(f"DDL statement is not supported by DDB PartiQL.")


class DynamoDBStatementCompiler(SQLCompiler):

    # DynamoDB can't guarantee the column orders of result
    _textual_ordered_columns: bool = True
    _ordered_columns: bool = False

    def visit_column(
        self,
        column,
        add_to_result_map = None,
        include_table: bool = True,
        result_map_targets = (),
        ambiguous_table_name_map = None,
        **kwargs,
    ) -> str:
        return super(DynamoDBStatementCompiler, self).visit_column(
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
        return super(DynamoDBStatementCompiler, self).visit_label(
            label=label,
            add_to_result_map=add_to_result_map,
            within_label_clause=within_label_clause,
            within_columns_clause=False,
            render_label_as_label=render_label_as_label,
            result_map_targets=result_map_targets,
            kw=kw,
        )


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
    def import_dbapi(cls):
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

    def get_schema_names(self, connection, **kw):
        # DynamoDB does not have the concept of a schema
        return ["default"]
        
    @reflection.cache
    def _get_tables(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        with raw_connection.connection.cursor() as cursor:
            return cursor.list_tables()

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