# -*- coding: utf-8 -*-

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
        for r in ret:
            if r[0] == "TableDescription.TableName":
                assert(r[1] == "Issues01")

        sql = """
        CREATE TABLE Issues02 (
            IssueId numeric HASH,
            Title string RANGE,
            CreateDate string,
            INDEX CreateDateIndex LOCAL (
                IssueId HASH,
                CreateDate RANGE
            )
            Projection.ProjectionType=ALL
        )
        ProvisionedThroughput.ReadCapacityUnits 1
        ProvisionedThroughput.WriteCapacityUnits 1
        """
        cursor.execute(sql)
        desc = cursor.description
        assert(desc == [
            ("response_name", "S", None, None, None, None, None),
            ("response_value", "S", None, None, None, None, None),
        ])
        ret = cursor.fetchall()
        for r in ret:
            if r[0] == "TableDescription.TableName":
                assert(r[1] == "Issues02")


    def test_drop_table(self, cursor):
        sql = """
        DROP TABLE Issues01
        """
        cursor.execute(sql)

        sql = """
        DROP TABLE Issues02
        """
        cursor.execute(sql)