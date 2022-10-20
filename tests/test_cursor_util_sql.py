# -*- coding: utf-8 -*-
import json
from tests import ENV


class TestCursorUtilSQL:
    def test_list_tables(self, cursor):
        sql = """
        LIST TABLES
        """
        cursor.execute(sql)
        ret = cursor.fetchall()
        assert ret is not None

        sql = """
        LIST TABLES
        LIMIT 2
        """
        cursor.execute(sql)
        ret = cursor.fetchall()
        assert len(ret) <= 2

    def test_desc_table(self, cursor):
        sql = """
        CREATE TABLE Issues03 (
            IssueId numeric HASH,
            Title string RANGE,
            CreateDate string,
            INDEX CreateDateIndexL LOCAL (
                IssueId HASH,
                CreateDate RANGE
            )
            Projection.ProjectionType=ALL,
            INDEX CreateDateIndexG GLOBAL (
                IssueId HASH,
                CreateDate RANGE
            )
            Projection.ProjectionType=ALL
        )
        BillingMode PAY_PER_REQUEST
        Tags (name:Issue03, usage:test_case)
        """
        cursor.execute(sql)

        sql = """
        DESC Issues03
        """
        cursor.execute(sql)
        ret = cursor.fetchall()
        table_desc = json.loads(ret[0][1])
        assert table_desc["TableName"] == "Issues03"

        sql = """
        DROP TABLE Issues03
        """
        cursor.execute(sql)

    def test_global_table(self, cursor):
        if ENV.use_local_ddb:
            # Ignore global table test cases for local DDB
            return

        # This part is not fully tested due to limit resources
        sql = """
        CREATE TABLE Issues04 (
            IssueId numeric HASH,
            Title string RANGE
        )
        ProvisionedThroughput.ReadCapacityUnits 1
        ProvisionedThroughput.WriteCapacityUnits 1
        """
        cursor.execute(sql)

        sql = """
        CREATE GLOBAL TABLE Issues04
            ReplicationGroup (cn-north-1, cn-northwest-1)
        """
        cursor.execute(sql)

        sql = """
        LIST GLOBAL TABLES
            RegionName cn-northwest-1
        """
        cursor.execute(sql)
        ret = cursor.fetchall()
        assert ret is not None

        sql = """
        DESC GLOBAL Issues04
        """
        cursor.execute(sql)
        ret = cursor.fetchall()
        assert ret is not None

        sql = """
        DROP GLOBAL TABLE Issues04
            ReplicationGroup (cn-north-1, cn-northwest-1)
        """
        cursor.execute(sql)

        sql = """
        DROP TABLE Issues04
        """
        cursor.execute(sql)
