# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser


class TestDmlSelect:
    def test_parse_simple_case_1(self):
        sql = """
        SELECT IssueId FROM Issues
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'SELECT IssueId FROM "Issues"'}

        sql = """
        SELECT IssueId, Title FROM "Issues"
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'SELECT IssueId,Title FROM "Issues"'}

        sql = """
        SELECT * FROM Issues.CreateDateIndex
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'SELECT * FROM "Issues"."CreateDateIndex"'}

        sql = """
        SELECT * FROM "Issues"."CreateDateIndex"
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'SELECT * FROM "Issues"."CreateDateIndex"'}

    def test_parse_simple_case_2(self):
        sql = """
        SELECT * FROM Orders
        WHERE OrderID = 100
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'SELECT * FROM "Orders" WHERE OrderID = 100'}

        sql = """
        SELECT * FROM Orders
        WHERE OrderID = 100 OR OrderID IN [200, 300, 234] AND Title IN ['X', 'Y']
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = 100 '
            + "OR OrderID IN [200,300,234] "
            + "AND Title IN ['X','Y']"
        }

        sql = """
        SELECT * FROM Orders
        WHERE OrderID = 100
        OR OrderID IN [200, 300, 234]
        AND Title = 'some title'
        AND Author IS NOT NULL
        AND CreateDate IS String
        OR IssueID BETWEEN 400 AND 500
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE '
            + "OrderID = 100 OR OrderID IN [200,300,234] "
            + "AND Title = 'some title' "
            + "AND Author IS NOT NULL "
            + "AND CreateDate IS STRING "
            + "OR IssueID BETWEEN 400 AND 500"
        }

    def test_parse_simple_case_3(self):
        sql = """
        SELECT * FROM Orders
        WHERE OrderID = 100
        ORDER BY OrderID DESC
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = 100 ORDER BY OrderID DESC'
        }

        sql = """
        SELECT * FROM Orders
        WHERE OrderID = 100 LIMIT 10
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = 100',
            "Limit": 10,
        }

        sql = """
        SELECT * FROM Orders
        WHERE OrderID = 100
        LIMIT 10
        ConsistentRead True
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = 100',
            "Limit": 10,
            "ConsistentRead": True,
        }

    def test_parse_simple_case_4(self):
        sql = """
        SELECT * FROM Orders
        WHERE OrderID = ? AND Title = ?
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = ? AND Title = ?'
        }

        sql = """
        SELECT * FROM Orders
        WHERE OrderID = ? OR Title = ?
        LIMIT 10
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = ? OR Title = ?',
            "Limit": 10,
        }

    def test_parse_attributes_with_op(self):
        sql = """
        SELECT TotalNum + EachNum
        FROM Orders
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT TotalNum+EachNum FROM "Orders"',
        }

        sql = """
        SELECT TotalNum + EachNum, TotalNum - EachNum
        FROM Orders
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT TotalNum+EachNum,TotalNum-EachNum FROM "Orders"',
        }

        sql = """
        SELECT TotalNum + EachNum1+EachNum2, TotalNum - EachNum1+EachNum2
        FROM Orders
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT TotalNum+EachNum1+EachNum2,TotalNum-EachNum1+EachNum2 FROM "Orders"',
        }

    def test_parse_completed_case(self):
        sql = """
        SELECT IssueId, Total, Content.DateWatched[0]
        FROM "Issues"."CreateDateIndex"
        WHERE IssueId IN [100, 300, 234]
        AND Title = 'some title'
        AND Content[0] >= 100
        AND Content.DateWatched[0] <= '12/12/19'
        AND Total IN [500, 600]
        OR Total BETWEEN 500 AND 600
        AND Author IS NOT NULL
        ORDER BY IssueId DESC
        LIMIT 10
        ConsistentRead False
        ReturnConsumedCapacity NONE
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT IssueId,Total,Content.DateWatched[0] FROM "Issues"."CreateDateIndex" '
            + "WHERE IssueId IN [100,300,234] "
            + "AND Title = 'some title' "
            + "AND Content[0] >= 100 "
            + "AND Content.DateWatched[0] <= '12/12/19' "
            + "AND Total IN [500,600] "
            + "OR Total BETWEEN 500 AND 600 "
            + "AND Author IS NOT NULL "
            + "ORDER BY IssueId DESC",
            "Limit": 10,
            "ConsistentRead": False,
            "ReturnConsumedCapacity": "NONE",
        }

    def test_parse_function_case(self):
        sql = """
            SELECT DATE(CreatedDate), DATE(IssueDate, '%Y-%m-%d')
            FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.parser.columns[0].request_name == "CreatedDate"
        assert parser.parser.columns[0].function.name == "DATE"
        assert parser.parser.columns[0].function.params is None
        assert parser.parser.columns[1].request_name == "IssueDate"
        assert parser.parser.columns[1].function.name == "DATE"
        assert parser.parser.columns[1].function.params == ["%Y-%m-%d"]

        assert ret == {
            "Statement": "SELECT CreatedDate,IssueDate FROM \"Issues\" WHERE key_partition = 'row_1'"
        }
