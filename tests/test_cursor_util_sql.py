# -*- coding: utf-8 -*-
import json


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
            ProvisionedThroughput.ReadCapacityUnits 20
            ProvisionedThroughput.WriteCapacityUnits 20
        )
        BillingMode PAY_PER_REQUEST
        ProvisionedThroughput.ReadCapacityUnits 20
        ProvisionedThroughput.WriteCapacityUnits 20
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
