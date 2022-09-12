# -*- coding: utf-8 -*-
import contextlib
import time
import pytest
from tests import ENV

def connect(**kwargs):
    from pydynamodb import connect

    if ENV.use_local_ddb:
        return connect(
            endpoint_url=ENV.endpoint_url,
            **kwargs
        )
    else:
        return connect(
                        region_name=ENV.region_name,
                        aws_access_key_id=ENV.aws_access_key_id,
                        aws_secret_access_key=ENV.aws_secret_access_key,
                        verify=ENV.verify,
                        use_ssl=ENV.use_ssl,
                        **kwargs
                    )

def boto3_connect():
    if ENV.use_local_ddb:
        from boto3 import client 
        return client('dynamodb', endpoint_url=ENV.endpoint_url)
    else:
        from boto3.session import Session
        session = Session(
                    region_name=ENV.region_name,
                    aws_access_key_id=ENV.aws_access_key_id,
                    aws_secret_access_key=ENV.aws_secret_access_key,
                )

        return session.client(
                            "dynamodb",
                            verify=ENV.verify,
                            use_ssl=ENV.use_ssl
                        )


@pytest.fixture(scope="session", autouse=True)
def _setup_session(request):
    request.addfinalizer(_teardown_session)

    table_name = 'pydynamodb_test_case01'
    client = boto3_connect()
    _create_table(client, table_name)

    if _is_table_ready(client, table_name):
        return
    else:
        assert(False), "Table creation failed."

def _teardown_session():
    tables = ['pydynamodb_test_case01']

    client = boto3_connect()
    _drop_tables(client, tables)

def _create_table(client, table_name):
    response = client.create_table(
        AttributeDefinitions=[
            {
            'AttributeName': 'key_partition',
            'AttributeType': 'S'
            },
            {
            'AttributeName': 'key_sort',
            'AttributeType': 'N'
            },
        ],
        TableName=table_name,
        KeySchema=[
            {
            'AttributeName': 'key_partition',
            'KeyType': 'HASH'
            },
            {
            'AttributeName': 'key_sort',
            'KeyType': 'RANGE'
            },
        ],
        BillingMode='PROVISIONED',
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        },
    )
    return response["TableDescription"].get("TableName", None)

def _is_table_ready(client, table_name):
    max_retry_times = 15
    retry_count = 0
    while True:
        if retry_count > max_retry_times:
            return False

        response = client.describe_table(
            TableName=table_name
        )
        status_ = response["Table"].get("TableStatus", None)
        if status_:
            if status_ in [
                "CREATING", "UPDATING"
            ]:
                retry_count += 1
                time.sleep(2)
                continue
            elif status_ == "ACTIVE":
                return True
            else:
                return False
        else:
            return False

def _cursor(cursor_class, request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})
    with contextlib.closing(
        connect(cursor_class=cursor_class, **request.param)
    ) as conn:
        with conn.cursor() as cursor:
            yield cursor

@pytest.fixture
def conn(request):
    from pydynamodb.cursor import Cursor

    if not hasattr(request, "param"):
        setattr(request, "param", {})
    with contextlib.closing(
        connect(cursor_class=Cursor, **request.param)
    ) as conn:
        yield conn

@pytest.fixture
def cursor(request):
    from pydynamodb.cursor import Cursor

    yield from _cursor(Cursor, request)

@pytest.fixture
def dict_cursor(request):
    from pydynamodb.cursor import DictCursor

    yield from _cursor(DictCursor, request)

@pytest.fixture
def converter(request):
    from pydynamodb.converter import DefaultTypeConverter

    yield DefaultTypeConverter()

def _drop_tables(client, tables):
    for table in tables:
        client.delete_table(
                        TableName=table
                    )
