.. image:: https://badge.fury.io/py/pydynamodb.svg
    :target: https://badge.fury.io/py/pydynamodb

.. image:: https://github.com/passren/PyDynamoDB/actions/workflows/run-test.yaml/badge.svg
    :target: https://github.com/passren/PyDynamoDB/actions/workflows/run-test.yaml

.. image:: https://pepy.tech/badge/pydynamodb/month
    :target: https://pepy.tech/project/pydynamodb

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

PyDynamoDB
===========

PyDynamoDB is a Python `DB API 2.0 (PEP 249)`_ client for `Amazon DynamoDB`_. 
SQLAlchemy dialect supported as well.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon DynamoDB`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html

.. contents:: Table of Contents:
   :local:
   :depth: 2

Objectives
----------
PyDynamoDB implement the DB API 2.0 interfaces based on  `PartiQL`_ supported by AWS DynamoDB. \
You have to create DDB tables before using pydynamodb, because `PartiQL`_ can only support SELECT, \
INSERT, UPDATE, DELETE operations on the tables. PyDynamodb provide parameters and result_set converter \
to make you easily manipulate `PartiQL`_ operations with Python built-in types. \
Transaction is also partially supported with DB standard operations, like begin() and commit(). \
This project is based on laughingman7743's `PyAthena`_.

.. _`PartiQL`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html
.. _`PyAthena`: https://github.com/laughingman7743/PyAthena


Requirements
--------------
* Python

  - CPython 3.7 3.8 3.9 3.10


Dependencies
--------------
* Boto3 (Python SDK for AWS Services)

  - boto3 >= 1.21.0
  - botocore >= 1.24.7

* Tenacity (Retry Utility for API calling)

  - tenacity >= 4.1.0

* SQLAlchemy (The ORM Toolkit for Python, only required if using PyDynamoDB Dialect)

  - SQLAlchemy >= 1.0.0, < 2.0.0


Installation
--------------
.. code:: shell

    pip install pydynamodb


Getting Started
---------------

Usage
-----


Basic usage
~~~~~~~~~~~

.. code:: python

    from pydynamodb import connect

    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key"
                     region_name="region_name").cursor()
    cursor.execute('SELECT * FROM "ddb_table_name"')
    print(cursor.fetchall())


Cursor iteration
~~~~~~~~~~~~~~~~

.. code:: python

    from pydynamodb import connect

    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key"
                     region_name="region_name").cursor()
    cursor.execute('SELECT * FROM "ddb_table_name"')
    rows = cursor.fetchall()
    for row in rows:
        print(row)


Query with parameters
~~~~~~~~~~~~~~~~~~~~~~

PyDynamoDB is able to serialize the parameters which passed to DDB \
and deserialize the response to Python built-in types.

.. code:: python

    from pydynamodb import connect
    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key"
                     region_name="region_name").cursor()
    cursor.execute("""INSERT INTO "ddb_table_name" VALUE {
                        'partition_key' = ?,
                        'sort_key' = ?,
                        'col_str' = ?,
                        'col_num' = ?,
                        'col_byte' = ?,
                        'col_ss' = ?,
                        'col_ns' = ?,
                        'col_bs' = ?,
                        'col_list' = ?,
                        'col_map' = ?,
                        'col_nested' = ?
                    }""", ["pkey_value", "skey_value", "str", 100, b"ABC", # String, Number, Bytes
                            {"str", "str"}, {100, 100}, {b"A", b"B"}, # String/Numnber/Bytes Set
                            ["str", 100, b"ABC"],  # List
                            {"key1": "val", "key2": "val"}, # Map
                            ["str", 100, {"key1": "val"}] # Nested Structure
                        ])

    cursor.execute('SELECT * FROM "ddb_table_name" WHERE partition_key = ?', ["key_value"])
    print(cursor.fetchall())


Description of Result Set
~~~~~~~~~~~~~~~~~~~~~~~~~~
DDB is a NoSQL database. That means except key schema, the data in each row may have flexible columns or types. \
PyDynamoDB cannot get a completed result set description before fetching all result data. So you have to use \
fetch* method to iterate the whole result set, then call cursor.description to get the full columns description.

.. code:: python

    from pydynamodb import connect

    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key"
                     region_name="region_name").cursor()
    cursor.execute('SELECT * FROM "ddb_table_name"')
    print(cursor.fetchall())
    print(cursor.description)


Dict Cursor and Result Set
~~~~~~~~~~~~~~~~~~~~~~~~~~
Using DictCursor, you can get a dict result set with column name and value pair. This type of cursor \
has better performance and manipulate result data easily. But cursor.description will return empty with this way.

.. code:: python

    from pydynamodb import connect
    from pydynamodb.cursor import DictCursor

    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key"
                     region_name="region_name").cursor(cursor=DictCursor)
    cursor.execute('SELECT * FROM "ddb_table_name"')
    print(cursor.fetchall())


Transaction
~~~~~~~~~~~
Transaction is partially supported also. connection.rollback() is not implemented. \
Regarding information and restrictions of DDB transaction, please see the page: `Performing transactions with PartiQL for DynamoDB`_

.. _`Performing transactions with PartiQL for DynamoDB`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.multiplestatements.transactions.html

.. code:: python

    from pydynamodb import connect

    conn = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key"
                     region_name="region_name")
    cursor = conn.cursor()
    
    conn.begin()
    cursor.execute("""INSERT INTO "ddb_table_name" VALUE {'key_partition': ?, 'key_sort': ?, 'col1': ?}""", 
                    ["pk1", "sk1", "test"])
    cursor.execute("""INSERT INTO "ddb_table_name" VALUE {'key_partition': ?, 'key_sort': ?, 'col1': ?}""", 
                    ["pk2", "sk2", "test"])
    conn.commit()

Limit Expression
~~~~~~~~~~~~~~~~~
DynamoDB doesn't support LIMIT expression in PartiQL. This is inconvenient in many scenarios. PyDynamoDB \
is able to support writing LIMIT expression in PartiQL.

.. code:: python

    from pydynamodb import connect

    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key"
                     region_name="region_name").cursor()
    cursor.execute('SELECT * FROM "ddb_table_name" WHERE key_partition = ? LIMIT 10', ["pk1"])
    print(cursor.fetchall())

SQLAlchemy
~~~~~~~~~~~
Install SQLAlchemy with ``pip install "SQLAlchemy>=1.0.0, <2.0.0"``.
Supported SQLAlchemy is 1.0.0 or higher and less than 2.0.0.

The connection string has the following format:

.. code:: text

    dynamodb://{aws_access_key_id}:{aws_secret_access_key}@dynamodb.{region_name}.amazonaws.com:443?verify=false&...

.. code:: python

    from pydynamodb import sqlalchemy_dynamodb
    from sqlalchemy.engine import create_engine
    from sqlalchemy.sql.schema import Column, MetaData, Table

    conn_str = (
            "dynamodb://{aws_access_key_id}:{aws_secret_access_key}@dynamodb.{region_name}.amazonaws.com:443"
            + "?verify=false"
        )
    conn_str = conn_str.format(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
    engine = create_engine(conn_str)
    with engine.connect() as connection:
        many_rows = Table("many_rows", MetaData(), 
                        Column('key_partition', String, nullable=False),
                        Column('key_sort', Integer),
                        Column('col_str', String),
                        Column('col_num', Numeric)
                )
        rows = conn.execute(many_rows.select()).fetchall()
        print(rows)

Test with local DynamoDB
~~~~~~~~~~~~~~~~~~~~~~~~
Install Local DDB, please see: `Deploying DynamoDB locally on your computer`_. \
If you want to run tests with local DDB, please make sure environment variables are set properly.

.. code:: shell

    USE_LOCAL_DDB=true
    LOCAL_DDB_ENDPOINT_URL=http://localhost:8000

.. _`Deploying DynamoDB locally on your computer`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html


License
=======

PyDynamoDB is distributed under the `MIT license
<https://opensource.org/licenses/MIT>`_.
