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
