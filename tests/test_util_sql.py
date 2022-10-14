# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser
from pydynamodb.sql.common import QueryType


class TestUtilSQL:
    def test_list_tables(self):
        sql = """
        LIST TABLES
        """
        parser = SQLParser(sql)
        assert parser.query_type == QueryType.LIST

        sql = """
        SHOW TABLES
        """
        parser = SQLParser(sql)
        assert parser.query_type == QueryType.LIST

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
