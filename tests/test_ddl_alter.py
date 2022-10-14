# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser


class TestDdlAlter:
    def test_parse_simple_case_1(self):
        sql = """
        ALTER TABLE Issues (
            IssueId numeric,
            CreateDate string,
            CREATE INDEX CreateDateIndex GLOBAL (
                    CreateDate PARTITION KEY,
                    IssueId SORT KEY
            )
                Projection.ProjectionType INCLUDE
                Projection.NonKeyAttributes (Description, Status)
                ProvisionedThroughput.ReadCapacityUnits 1
                ProvisionedThroughput.WriteCapacityUnits 1
        )
        """
        ret = SQLParser(sql).transform()
        except_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"},
                {"AttributeName": "CreateDate", "AttributeType": "S"},
            ],
            "TableName": "Issues",
            "GlobalSecondaryIndexUpdates": [
                {
                    "Create": {
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
                    }
                }
            ],
        }
        assert ret == except_ret

    def test_parse_simple_case_2(self):
        sql = """
        ALTER TABLE Issues (
            IssueId numeric,
            DueDate string,
            UPDATE INDEX DueDateIndex GLOBAL
                ProvisionedThroughput.ReadCapacityUnits 10,
            CREATE REPLICA cn-north-1
                KMSMasterKeyId XXXXXXXX
                ProvisionedThroughputOverride.ReadCapacityUnits 1
                TableClassOverride STANDARD,
            UPDATE REPLICA cn-northwest-1
                GlobalSecondaryIndexes (
                    DueDateIndex
                    ProvisionedThroughputOverride.ReadCapacityUnits 10
                ),
        )
        BillingMode PAY_PER_REQUEST
        """
        ret = SQLParser(sql).transform()
        expected_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"},
                {"AttributeName": "DueDate", "AttributeType": "S"},
            ],
            "TableName": "Issues",
            "GlobalSecondaryIndexUpdates": [
                {
                    "Update": {
                        "IndexName": "DueDateIndex",
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 10,
                        },
                    }
                }
            ],
            "ReplicaUpdates": [
                {
                    "Create": {
                        "RegionName": "cn-north-1",
                        "KMSMasterKeyId": "XXXXXXXX",
                        "ProvisionedThroughputOverride": {"ReadCapacityUnits": 1},
                        "TableClassOverride": "STANDARD",
                    }
                },
                {
                    "Update": {
                        "RegionName": "cn-northwest-1",
                        "GlobalSecondaryIndexes": [
                            {
                                "IndexName": "DueDateIndex",
                                "ProvisionedThroughputOverride": {
                                    "ReadCapacityUnits": 10
                                },
                            }
                        ],
                    }
                },
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        assert ret == expected_ret

    def test_parse_completed_case(self):
        sql = """
        ALTER TABLE Issues (
            IssueId numeric,
            Title string,
            CreateDate string,
            DueDate string,
            CREATE INDEX CreateDateIndex GLOBAL (
                CreateDate PARTITION KEY,
                IssueId SORT KEY
            )
                Projection.ProjectionType INCLUDE
                Projection.NonKeyAttributes (Description, Status)
                ProvisionedThroughput.ReadCapacityUnits 1
                ProvisionedThroughput.WriteCapacityUnits 1,
            UPDATE INDEX DueDateIndex GLOBAL
                ProvisionedThroughput.ReadCapacityUnits 10
                ProvisionedThroughput.WriteCapacityUnits 10,
            DELETE INDEX IssueIdIndex GLOBAL,
            CREATE REPLICA cn-north-1
                KMSMasterKeyId XXXXXXXX
                ProvisionedThroughputOverride.ReadCapacityUnits 1
                GlobalSecondaryIndexes (
                    CreateDateIndex
                    ProvisionedThroughputOverride.ReadCapacityUnits 1,
                    DueDateIndex
                    ProvisionedThroughputOverride.ReadCapacityUnits 1
                )
                TableClassOverride STANDARD,
            UPDATE REPLICA cn-northwest-1
                KMSMasterKeyId *********
                ProvisionedThroughputOverride.ReadCapacityUnits 10
                GlobalSecondaryIndexes (
                    CreateDateIndex
                    ProvisionedThroughputOverride.ReadCapacityUnits 10,
                    DueDateIndex
                    ProvisionedThroughputOverride.ReadCapacityUnits 10
                )
                TableClassOverride STANDARD_INFREQUENT_ACCESS,
            DELETE REPLICA cn-northwest-2
        )
        BillingMode PAY_PER_REQUEST
        ProvisionedThroughput.ReadCapacityUnits 10
        ProvisionedThroughput.WriteCapacityUnits 10
        StreamSpecification.StreamEnabled True
        StreamSpecification.StreamViewType NEW_AND_OLD_IMAGES
        SSESpecification.Enabled True
        SSESpecification.SSEType KMS
        SSESpecification.KMSMasterKeyId $$$$$$$$
        TableClass STANDARD_INFREQUENT_ACCESS
        """

        ret = SQLParser(sql).transform()
        expected_ret = {
            "AttributeDefinitions": [
                {"AttributeName": "IssueId", "AttributeType": "N"},
                {"AttributeName": "Title", "AttributeType": "S"},
                {"AttributeName": "CreateDate", "AttributeType": "S"},
                {"AttributeName": "DueDate", "AttributeType": "S"},
            ],
            "TableName": "Issues",
            "GlobalSecondaryIndexUpdates": [
                {
                    "Create": {
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
                    }
                },
                {
                    "Update": {
                        "IndexName": "DueDateIndex",
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 10,
                            "WriteCapacityUnits": 10,
                        },
                    }
                },
                {"Delete": {"IndexName": "IssueIdIndex"}},
            ],
            "ReplicaUpdates": [
                {
                    "Create": {
                        "RegionName": "cn-north-1",
                        "KMSMasterKeyId": "XXXXXXXX",
                        "ProvisionedThroughputOverride": {"ReadCapacityUnits": 1},
                        "GlobalSecondaryIndexes": [
                            {
                                "IndexName": "CreateDateIndex",
                                "ProvisionedThroughputOverride": {
                                    "ReadCapacityUnits": 1
                                },
                            },
                            {
                                "IndexName": "DueDateIndex",
                                "ProvisionedThroughputOverride": {
                                    "ReadCapacityUnits": 1
                                },
                            },
                        ],
                        "TableClassOverride": "STANDARD",
                    }
                },
                {
                    "Update": {
                        "RegionName": "cn-northwest-1",
                        "KMSMasterKeyId": "*********",
                        "ProvisionedThroughputOverride": {"ReadCapacityUnits": 10},
                        "GlobalSecondaryIndexes": [
                            {
                                "IndexName": "CreateDateIndex",
                                "ProvisionedThroughputOverride": {
                                    "ReadCapacityUnits": 10
                                },
                            },
                            {
                                "IndexName": "DueDateIndex",
                                "ProvisionedThroughputOverride": {
                                    "ReadCapacityUnits": 10
                                },
                            },
                        ],
                        "TableClassOverride": "STANDARD_INFREQUENT_ACCESS",
                    }
                },
                {"Delete": {"RegionName": "cn-northwest-2"}},
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 10,
            },
            "StreamSpecification": {
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
            "SSESpecification": {
                "Enabled": True,
                "SSEType": "KMS",
                "KMSMasterKeyId": "$$$$$$$$",
            },
            "TableClass": "STANDARD_INFREQUENT_ACCESS",
        }
        assert ret == expected_ret
