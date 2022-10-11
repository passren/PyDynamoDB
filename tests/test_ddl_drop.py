# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser

class TestDdlDrop:
    def test_parse_drop(self):
        sql = """
        DROP TABLE Issues
        """
        ret = SQLParser(sql).transform()
        assert(ret == {"TableName": "Issues"})

        sql = """
        DROP TABLE Issues Topics
        """
        ret = SQLParser(sql).transform()
        assert(ret == {"TableName": "Issues"})

        sql = """
        drop table Issues
        """
        ret = SQLParser(sql).transform()
        assert(ret == {"TableName": "Issues"})
