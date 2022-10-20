# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser
from pydynamodb.sql.common import QueryType


class TestDdlDrop:
    def test_parse_drop(self):
        sql = """
        DROP TABLE Issues
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.DROP
        assert ret == {"TableName": "Issues"}

        sql = """
        DROP TABLE Issues Topics
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.DROP
        assert ret == {"TableName": "Issues"}

        sql = """
        drop table Issues
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.DROP
        assert ret == {"TableName": "Issues"}

    def test_parse_drop_global(self):
        sql = """
        DROP GLOBAL TABLE Issues
            ReplicationGroup (us-east-1, us-west-2)
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.DROP_GLOBAL
        assert ret == {
            "GlobalTableName": "Issues",
            "ReplicaUpdates": [
                {"Delete": {"RegionName": "us-east-1"}},
                {"Delete": {"RegionName": "us-west-2"}},
            ],
        }

        sql = """
        DROP GLOBAL TABLE Issues
            ReplicationGroup (us-east-1)
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.DROP_GLOBAL
        assert ret == {
            "GlobalTableName": "Issues",
            "ReplicaUpdates": [{"Delete": {"RegionName": "us-east-1"}}],
        }
