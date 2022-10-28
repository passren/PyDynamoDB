# -*- coding: utf-8 -*-
from pydynamodb.sql.common import QueryType
from pydynamodb.sql.parser import SQLParser


class TestDdlCreate:
    def test_parse_simple_case_1(self):
        sql = """
        CREATE TABLE Issues (
            IssueId numeric PARTITION KEY
        )
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.CREATE
        expected_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"}
            ],
            "TableName": "Issues",
            "KeySchema": [{"AttributeName": "IssueId", "KeyType": "HASH"}],
        }
        assert ret == expected_ret

    def test_parse_simple_case_2(self):
        sql = """
        CREATE TABLE Issues (
            IssueId numeric HASH,
            Title string RANGE,
        )
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.CREATE
        expected_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"},
                {"AttributeName": "Title", "AttributeType": "S"},
            ],
            "TableName": "Issues",
            "KeySchema": [
                {"AttributeName": "IssueId", "KeyType": "HASH"},
                {"AttributeName": "Title", "KeyType": "RANGE"},
            ],
        }
        assert ret == expected_ret

    def test_parse_simple_case_3(self):
        sql = """
        CREATE TABLE Issues (
            IssueId numeric HASH,
            Title string RANGE,
            CreateDate string
        )
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.CREATE
        expected_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"},
                {"AttributeName": "Title", "AttributeType": "S"},
                {"AttributeName": "CreateDate", "AttributeType": "S"},
            ],
            "TableName": "Issues",
            "KeySchema": [
                {"AttributeName": "IssueId", "KeyType": "HASH"},
                {"AttributeName": "Title", "KeyType": "RANGE"},
            ],
        }
        assert ret == expected_ret

    def test_parse_options_with_equal(self):
        sql = """
        CREATE TABLE Issues (
            IssueId numeric HASH,
            Title string RANGE,
            CreateDate string,
            INDEX CreateDateIndex LOCAL (
                CreateDate HASH,
                IssueId RANGE
            )
            Projection.ProjectionType=INCLUDE
            Projection.NonKeyAttributes=(Description, Status)
        )
        BillingMode=PROVISIONED
        ProvisionedThroughput.ReadCapacityUnits=1
        ProvisionedThroughput.WriteCapacityUnits=1
        StreamSpecification.StreamEnabled=False
        Tags=(name:Issue, usage:test)
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.CREATE
        expected_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"},
                {"AttributeName": "Title", "AttributeType": "S"},
                {"AttributeName": "CreateDate", "AttributeType": "S"},
            ],
            "TableName": "Issues",
            "KeySchema": [
                {"AttributeName": "IssueId", "KeyType": "HASH"},
                {"AttributeName": "Title", "KeyType": "RANGE"},
            ],
            "LocalSecondaryIndexes": [
                {
                    "IndexName": "CreateDateIndex",
                    "KeySchema": [
                        {"AttributeName": "CreateDate", "KeyType": "HASH"},
                        {"AttributeName": "IssueId", "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": ["Description", "Status"],
                    },
                }
            ],
            "BillingMode": "PROVISIONED",
            "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            "StreamSpecification": {
                "StreamEnabled": False,
            },
            "Tags": [
                {"Key": "name", "Value": "Issue"},
                {"Key": "usage", "Value": "test"},
            ],
        }
        assert ret == expected_ret

    def test_parse_completed_case(self):
        sql = """
        CREATE TABLE Issues (
            IssueId numeric PARTITION KEY,
            Title string SORT KEY,
            CreateDate string,
            DueDate string,
            Requester string,
            Assignee string,
            INDEX CreateDateIndex GLOBAL (
                CreateDate PARTITION KEY,
                IssueId SORT KEY
            )
                Projection.ProjectionType INCLUDE
                Projection.NonKeyAttributes (Description, Status)
                ProvisionedThroughput.ReadCapacityUnits 1
                ProvisionedThroughput.WriteCapacityUnits 1,
            INDEX DueDateIndex GLOBAL (
                DueDate PARTITION KEY
            )
                Projection.ProjectionType ALL
                ProvisionedThroughput.ReadCapacityUnits 1
                ProvisionedThroughput.WriteCapacityUnits 1,
            INDEX RequesterIndex LOCAL (
                Requester PARTITION KEY,
                Title SORT KEY
            )
                Projection.ProjectionType KEYS_ONLY,
            INDEX AssigneeIndex LOCAL (
                Assignee PARTITION KEY
            )
                Projection.ProjectionType ALL,
        )
        BillingMode PROVISIONED
        ProvisionedThroughput.ReadCapacityUnits 1
        ProvisionedThroughput.WriteCapacityUnits 1
        StreamSpecification.StreamEnabled TRUE
        StreamSpecification.StreamViewType NEW_IMAGE
        SSESpecification.Enabled TRUE
        SSESpecification.SSEType AES256
        SSESpecification.KMSMasterKeyId XXXXXXXX
        TableClass STANDARD
        Tags (name:Issue, usage:test)
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.CREATE
        expected_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"},
                {"AttributeName": "Title", "AttributeType": "S"},
                {"AttributeName": "CreateDate", "AttributeType": "S"},
                {"AttributeName": "DueDate", "AttributeType": "S"},
                {"AttributeName": "Requester", "AttributeType": "S"},
                {"AttributeName": "Assignee", "AttributeType": "S"},
            ],
            "TableName": "Issues",
            "KeySchema": [
                {"AttributeName": "IssueId", "KeyType": "HASH"},
                {"AttributeName": "Title", "KeyType": "RANGE"},
            ],
            "LocalSecondaryIndexes": [
                {
                    "IndexName": "RequesterIndex",
                    "KeySchema": [
                        {"AttributeName": "Requester", "KeyType": "HASH"},
                        {"AttributeName": "Title", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                },
                {
                    "IndexName": "AssigneeIndex",
                    "KeySchema": [{"AttributeName": "Assignee", "KeyType": "HASH"}],
                    "Projection": {
                        "ProjectionType": "ALL",
                    },
                },
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "CreateDateIndex",
                    "KeySchema": [
                        {"AttributeName": "CreateDate", "KeyType": "HASH"},
                        {"AttributeName": "IssueId", "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": ["Description", "Status"],
                    },
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    },
                },
                {
                    "IndexName": "DueDateIndex",
                    "KeySchema": [{"AttributeName": "DueDate", "KeyType": "HASH"}],
                    "Projection": {
                        "ProjectionType": "ALL",
                    },
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    },
                },
            ],
            "BillingMode": "PROVISIONED",
            "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            "StreamSpecification": {
                "StreamEnabled": True,
                "StreamViewType": "NEW_IMAGE",
            },
            "SSESpecification": {
                "Enabled": True,
                "SSEType": "AES256",
                "KMSMasterKeyId": "XXXXXXXX",
            },
            "TableClass": "STANDARD",
            "Tags": [
                {"Key": "name", "Value": "Issue"},
                {"Key": "usage", "Value": "test"},
            ],
        }
        assert ret == expected_ret

    def test_parse_global_table(self):
        sql = """
        CREATE GLOBAL TABLE Issues
            ReplicationGroup (us-east-1, us-west-2)
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.CREATE_GLOBAL
        expected_ret = {
            "GlobalTableName": "Issues",
            "ReplicationGroup": [
                {"RegionName": "us-east-1"},
                {"RegionName": "us-west-2"},
            ],
        }
        assert ret == expected_ret

        sql = """
        CREATE GLOBAL TABLE Issues
            ReplicationGroup (us-east-1)
        """
        parser = SQLParser(sql)
        ret = parser.transform()
        assert parser.query_type == QueryType.CREATE_GLOBAL
        expected_ret = {
            "GlobalTableName": "Issues",
            "ReplicationGroup": [
                {"RegionName": "us-east-1"},
            ],
        }
        assert ret == expected_ret
