# -*- coding: utf-8 -*-
import json


class TestCursorDDL:
    def test_create_table(self, cursor):
        sql = """
        CREATE TABLE Issues01 (
            IssueId numeric HASH,
            Title string RANGE
        )
        ProvisionedThroughput.ReadCapacityUnits 1
        ProvisionedThroughput.WriteCapacityUnits 1
        """
        cursor.execute(sql)
        ret = cursor.fetchall()
        table_desc = json.loads(ret[0][1])
        assert table_desc["TableName"] == "Issues01"

        sql = """
        CREATE TABLE Issues02 (
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
            ProvisionedThroughput.ReadCapacityUnits 1
            ProvisionedThroughput.WriteCapacityUnits 1
        )
        ProvisionedThroughput.ReadCapacityUnits 1
        ProvisionedThroughput.WriteCapacityUnits 1
        """
        cursor.execute(sql)
        desc = cursor.description
        assert desc == [
            ("response_name", None, None, None, None, None, None),
            ("response_value", None, None, None, None, None, None),
        ]
        ret = cursor.fetchall()
        table_desc = json.loads(ret[0][1])
        assert table_desc["TableName"] == "Issues02"

    def test_alter_table(self, cursor):
        sql = """
        ALTER TABLE Issues02 (
            IssueId numeric,
            Title string,
            CreateDate string,
            DueDate string,
            IssueDate string
            CREATE INDEX IssueDateIndex GLOBAL (
                Title PARTITION KEY,
                IssueDate SORT KEY
            )
                Projection.ProjectionType INCLUDE
                Projection.NonKeyAttributes (Description, Status)
                ProvisionedThroughput.ReadCapacityUnits 1
                ProvisionedThroughput.WriteCapacityUnits 1,
            UPDATE INDEX CreateDateIndexG GLOBAL
                ProvisionedThroughput.ReadCapacityUnits 10
                ProvisionedThroughput.WriteCapacityUnits 10
        )
        """
        cursor.execute(sql)
        ret = cursor.fetchall()
        table_desc = json.loads(ret[0][1])
        assert table_desc["TableName"] == "Issues02"

    def test_drop_table(self, cursor):
        self.drop_table(cursor, "Issues01")
        self.drop_table(cursor, "Issues02")

    def drop_table(self, cursor, table_name):
        sql = "DROP TABLE %s" % table_name
        cursor.execute(sql)
        ret = cursor.fetchall()
        table_desc = json.loads(ret[0][1])
        assert table_desc["TableName"] == table_name
