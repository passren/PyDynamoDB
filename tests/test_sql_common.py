# -*- coding: utf-8 -*-
from pydynamodb.sql.common import QueryType, get_query_type

class TestSQLCommon:
    def test_get_query_type_create(self):
        assert(
            get_query_type("""
            CREATE TABLE Issues (
                IssueId numeric PARTITION KEY,
            )
            """) == QueryType.CREATE
        )
        assert(
            get_query_type("""
            create table issues (
                issueid numeric partition key,
            )
            """) == QueryType.CREATE
        )
        try:
            get_query_type("CREATE XXXXX")
        except LookupError as err:
            assert(err is not None)

    def test_get_query_type_alter(self):
        assert(
            get_query_type("""
            ALTER TABLE Issues (
                IssueId numeric PARTITION KEY,
            )
            """) == QueryType.ALTER
        )
        assert(
            get_query_type("""
            alter 
            table issues (
                issueid numeric partition key,
            )****
            """) == QueryType.ALTER
        )
        try:
            get_query_type("ALTER XXXXX")
        except LookupError as err:
            assert(err is not None)

    def test_get_query_type_drop(self):
        assert(
            get_query_type("""
            DROP TABLE ISSUES
            """) == QueryType.DROP
        )
        assert(
            get_query_type("""
            drop 
            table issues
            """) == QueryType.DROP
        )
        try:
            get_query_type("drop XXXXX")
        except LookupError as err:
            assert(err is not None)