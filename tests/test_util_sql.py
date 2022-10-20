# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser
from pydynamodb.sql.common import QueryType


class TestUtilSQL:
    def test_list_tables(self):
        sql = """
        LIST TABLES
        """
        parser = SQLParser(sql)
        ret = SQLParser(sql).transform()
        assert parser.query_type == QueryType.LIST
        assert ret == {}

        sql = """
        SHOW TABLES
        """
        parser = SQLParser(sql)
        assert parser.query_type == QueryType.LIST

        sql = """
        LIST TABLES
        LIMIT 10
        """
        parser = SQLParser(sql)
        ret = SQLParser(sql).transform()
        assert parser.query_type == QueryType.LIST
        assert ret == {"Limit": 10}

    def test_list_global_tables(self):
        sql = """
        LIST GLOBAL TABLES
            RegionName us-west-1
        """
        parser = SQLParser(sql)
        ret = SQLParser(sql).transform()
        assert parser.query_type == QueryType.LIST_GLOBAL
        assert ret == {"RegionName": "us-west-1"}

        sql = """
        SHOW GLOBAL TABLES
            RegionName us-west-2
            Limit 10
        """
        parser = SQLParser(sql)
        ret = SQLParser(sql).transform()
        assert parser.query_type == QueryType.LIST_GLOBAL
        assert ret == {"RegionName": "us-west-2", "Limit": 10}

    def test_desc_table(self):
        sql = """
        DESC Issues
        """
        ret = SQLParser(sql).transform()
        assert ret == {"TableName": "Issues"}

        sql = """
        DESCRIBE Issues
        """
        ret = SQLParser(sql).transform()
        assert ret == {"TableName": "Issues"}

        sql = """
        desc Issues Topics
        """
        ret = SQLParser(sql).transform()
        assert ret == {"TableName": "Issues"}

    def test_desc_global_table(self):
        sql = """
        DESC GLOBAL Issues
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.DESC_GLOBAL
        assert ret == {"GlobalTableName": "Issues"}

        sql = """
        describe global Issues
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.DESC_GLOBAL
        assert ret == {"GlobalTableName": "Issues"}
