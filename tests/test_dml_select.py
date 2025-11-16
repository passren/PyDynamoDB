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
        assert ret == {"Statement": 'SELECT * FROM "Issues.CreateDateIndex"'}

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
        AND "Author" != 'PR'
        ORDER BY OrderID DESC
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = 100 AND "Author" != \'PR\' ORDER BY OrderID DESC'
        }

        sql = """
        SELECT * FROM Orders
        WHERE OrderID = 100 LIMIT 10
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT * FROM "Orders" WHERE OrderID = 100',
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
        }

        sql = """
        SELECT IssueId, "Total" FROM Orders
        WHERE "OrderID" = ? OR Title = ?
        LIMIT 10
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT IssueId,"Total" '
            + 'FROM "Orders" WHERE "OrderID" = ? OR Title = ?',
        }

        sql = """
        SELECT "IssueId", "Total", Content."DateWatched"[0] FROM Orders
        WHERE OrderID = ? OR "Title" = ?
        LIMIT 10
        """
        ret = SQLParser(sql).transform()
        assert ret == {
            "Statement": 'SELECT "IssueId","Total",Content."DateWatched"[0] '
            + 'FROM "Orders" WHERE OrderID = ? OR "Title" = ?',
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
            "Statement": 'SELECT IssueId,"Total",Content.DateWatched[0] FROM "Issues"."CreateDateIndex" '
            + "WHERE IssueId IN [100,300,234] "
            + "AND Title = 'some title' "
            + "AND Content[0] >= 100 "
            + "AND Content.DateWatched[0] <= '12/12/19' "
            + "AND \"Total\" IN [500,600] "
            + "OR \"Total\" BETWEEN 500 AND 600 "
            + "AND Author IS NOT NULL "
            + "ORDER BY IssueId DESC",
            "ConsistentRead": False,
            "ReturnConsumedCapacity": "NONE",
        }

    def test_parse_function_case_1(self):
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

    def test_parse_function_case_2(self):
        sql = """
            SELECT NUMBER(IssueID), SUBSTR(IssueDesc, 1, 3), SUBSTRING(IssueDesc, 1, 3),
            REPLACE(IssueDesc, 'abc', 'def')
            FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.parser.columns[0].request_name == "IssueID"
        assert parser.parser.columns[0].function.name == "NUMBER"
        assert parser.parser.columns[0].function.params is None
        assert parser.parser.columns[1].request_name == "IssueDesc"
        assert parser.parser.columns[1].function.name == "SUBSTR"
        assert parser.parser.columns[1].function.params == [1, 3]
        assert parser.parser.columns[2].request_name == "IssueDesc"
        assert parser.parser.columns[2].function.name == "SUBSTRING"
        assert parser.parser.columns[2].function.params == [1, 3]
        assert parser.parser.columns[3].request_name == "IssueDesc"
        assert parser.parser.columns[3].function.name == "REPLACE"
        assert parser.parser.columns[3].function.params == ["abc", "def"]

        assert ret == {
            "Statement": "SELECT IssueID,IssueDesc FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_alias_case(self):
        sql = """
            SELECT IssueID, DATE(CreatedDate) create_data, Title
            FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.parser.columns[0].alias is None
        assert parser.parser.columns[1].alias == "create_data"
        assert parser.parser.columns[2].alias is None
        assert ret == {
            "Statement": "SELECT IssueID,CreatedDate,Title FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

        sql = """
            SELECT IssueID, DATE(CreatedDate) create_data, DATE(IssueDate, '%Y-%m-%d') issue_date
            FROM Issues WHERE key_partition='row_1'
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.parser.columns[0].alias is None
        assert parser.parser.columns[1].alias == "create_data"
        assert parser.parser.columns[2].alias == "issue_date"
        assert ret == {
            "Statement": "SELECT IssueID,CreatedDate,IssueDate FROM \"Issues\" WHERE key_partition = 'row_1'"
        }

    def test_parse_partiql_functions(self):
        sql = """
            SELECT IssueID, CreatedDate, Title
            FROM Issues WHERE key_partition='row_1' AND contains('Title', 'ABC')
        """
        parser = SQLParser(sql)
        parser.transform()
        WHERE_CONDITIONS_0 = "key_partition = 'row_1'"
        assert parser.parser.where_conditions[0] == WHERE_CONDITIONS_0
        assert parser.parser.where_conditions[1] == "AND"
        assert parser.parser.where_conditions[2] == "contains('Title','ABC')"

        sql = """
            SELECT IssueID, CreatedDate, Title
            FROM Issues WHERE key_partition='row_1' AND begins_with('Title', 'ABC')
        """
        parser = SQLParser(sql)
        parser.transform()
        assert parser.parser.where_conditions[0] == WHERE_CONDITIONS_0
        assert parser.parser.where_conditions[1] == "AND"
        assert parser.parser.where_conditions[2] == "begins_with('Title','ABC')"

        sql = """
            SELECT IssueID, CreatedDate, Title
            FROM Issues WHERE key_partition='row_1' AND attribute_type('Title', 'S')
        """
        parser = SQLParser(sql)
        parser.transform()
        assert parser.parser.where_conditions[0] == WHERE_CONDITIONS_0
        assert parser.parser.where_conditions[1] == "AND"
        assert parser.parser.where_conditions[2] == "attribute_type('Title','S')"

        sql = """
            SELECT IssueID, CreatedDate, Title
            FROM Issues WHERE key_partition='row_1' AND size('Title')>20
        """
        parser = SQLParser(sql)
        parser.transform()
        assert parser.parser.where_conditions[0] == WHERE_CONDITIONS_0
        assert parser.parser.where_conditions[1] == "AND"
        assert parser.parser.where_conditions[2] == "size('Title') > 20"

        sql = """
            SELECT IssueID, CreatedDate, Title
            FROM Issues WHERE key_partition='row_1' AND Title is MISSING
        """
        parser = SQLParser(sql)
        parser.transform()
        assert parser.parser.where_conditions[0] == WHERE_CONDITIONS_0
        assert parser.parser.where_conditions[1] == "AND"
        assert parser.parser.where_conditions[2] == "Title IS MISSING"

        sql = """
            SELECT IssueID, CreatedDate, Title
            FROM Issues WHERE key_partition='row_1' AND contains('Title', 'ABC')
            OR begins_with('Title', 'ABC') AND size('Title') <= 20
            AND CreatedDate is missing
        """
        parser = SQLParser(sql)
        parser.transform()
        assert parser.parser.where_conditions[0] == WHERE_CONDITIONS_0
        assert parser.parser.where_conditions[1] == "AND"
        assert parser.parser.where_conditions[2] == "contains('Title','ABC')"
        assert parser.parser.where_conditions[3] == "OR"
        assert parser.parser.where_conditions[4] == "begins_with('Title','ABC')"
        assert parser.parser.where_conditions[5] == "AND"
        assert parser.parser.where_conditions[6] == "size('Title') <= 20"
        assert parser.parser.where_conditions[7] == "AND"
        assert parser.parser.where_conditions[8] == "CreatedDate IS MISSING"

    def test_parse_dot_in_table_name(self):
        sql = """
        SELECT * FROM "Pub.Issues"."CreateDateIndex"
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'SELECT * FROM "Pub.Issues"."CreateDateIndex"'}

        sql = """
        SELECT att1, att2 FROM "This.Pub.Issues"."Index.CreateDateIndex"
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'SELECT att1,att2 FROM "This.Pub.Issues"."Index.CreateDateIndex"'}