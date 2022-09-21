# -*- coding: utf-8 -*-
import contextlib
import time
import pytest
from tests import ENV
import sqlalchemy


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

def create_engine(**kwargs):
    from pydynamodb import sqlalchemy_dynamodb

    if ENV.use_local_ddb:
        conn_str = (
            "dynamodb://{aws_access_key_id}:{aws_secret_access_key}@dynamodb.{region_name}.amazonaws.com:443"
            + "?endpoint_url={endpoint_url}"
        )

        conn_str = conn_str.format(
            aws_access_key_id="NA",
            aws_secret_access_key="NA",
            region_name=ENV.region_name,
            endpoint_url=ENV.endpoint_url,
            **kwargs,
        )
    else:
        conn_str = (
            "dynamodb://{aws_access_key_id}:{aws_secret_access_key}@dynamodb.{region_name}.amazonaws.com:443"
            + "?verify=false"
        )

        conn_str = conn_str.format(
            aws_access_key_id=ENV.aws_access_key_id,
            aws_secret_access_key=ENV.aws_secret_access_key,
            region_name=ENV.region_name,
            **kwargs,
        )

    return sqlalchemy.engine.create_engine(
        conn_str
    )

TEST_TABLES = ['pydynamodb_test_case01',
                    'pydynamodb_test_case02',
                    'pydynamodb_test_case03',
                ]

@pytest.fixture(scope="session", autouse=True)
def _setup_session(request):
    request.addfinalizer(_teardown_session)

    
    client = boto3_connect()
    for t in TEST_TABLES:
        _create_table(client, t)

    if _is_table_ready(client, TEST_TABLES[0]):
        return
    else:
        assert(False), "Table creation failed."

def _teardown_session():
    client = boto3_connect()
    _drop_tables(client, TEST_TABLES)

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
def engine(request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})
    engine_ = create_engine(**request.param)
    try:
        with contextlib.closing(engine_.connect()) as conn:
            yield engine_, conn
    finally:
        engine_.dispose()

@pytest.fixture
def converter(request):
    from pydynamodb.converter import DefaultTypeConverter

    yield DefaultTypeConverter()

def _drop_tables(client, tables):
    for table in tables:
        client.delete_table(
                        TableName=table
                    )
