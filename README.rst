.. |badge package| image:: https://badge.fury.io/py/pydynamodb.svg
    :target: https://badge.fury.io/py/pydynamodb

.. |badge test|  image:: https://github.com/passren/PyDynamoDB/actions/workflows/run-test.yaml/badge.svg
    :target: https://github.com/passren/PyDynamoDB/actions/workflows/run-test.yaml

.. |badge downloads|  image:: https://static.pepy.tech/badge/pydynamodb/month
    :target: https://pepy.tech/project/pydynamodb

.. |badge formation| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. |badge codcov| image:: https://codecov.io/github/passren/PyDynamoDB/branch/main/graph/badge.svg?token=Y5DG320O76 
    :target: https://codecov.io/github/passren/PyDynamoDB

.. |badge sqrelia| image:: https://sonarcloud.io/api/project_badges/measure?project=passren_PyDynamoDB&metric=reliability_rating 
    :target: https://sonarcloud.io/summary/new_code?id=passren_PyDynamoDB

.. |badge sqale| image:: https://sonarcloud.io/api/project_badges/measure?project=passren_PyDynamoDB&metric=sqale_rating 
    :target: https://sonarcloud.io/summary/new_code?id=passren_PyDynamoDB

.. |badge sqvuln| image:: https://sonarcloud.io/api/project_badges/measure?project=passren_PyDynamoDB&metric=vulnerabilities 
    :target: https://sonarcloud.io/summary/new_code?id=passren_PyDynamoDB

|badge package| |badge test| |badge downloads| |badge formation| |badge codcov| 
|badge sqrelia| |badge sqale| |badge sqvuln|

PyDynamoDB
===========

PyDynamoDB is a Python `DB API 2.0 (PEP 249)`_ client for `Amazon DynamoDB`_. 
SQLAlchemy dialect supported as well.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon DynamoDB`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html


Objectives
----------
PyDynamoDB implement the DB API 2.0 interfaces based on  `PartiQL`_ supported by AWS DynamoDB. \
Although PartiQL can only support DML operations (INSERT, UPDATE, DELETE, SELECT), PyDynamoDB \
extended the capabilities to support DDL as well. Now you are able to use MySQL-like statements \
to CREATE/ALTER/DROP tables. Besides DDL statements, some of utility statements are allowed to \
execute (Such as, List and Describe Table). \
PyDynamodb provide parameters and result_set converter to make you easily manipulate operations \
with Python built-in types. \
Transaction is also partially supported with DB standard operations, like begin() and commit().


.. _`PartiQL`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html
.. _`PyAthena`: https://github.com/laughingman7743/PyAthena


Features
---------
* Compatible with DB API 2.0 Specification
* PartiQL for DML operations (INSERT, UPDATE, DELETE, SELECT)
* Limit supported in SELECT statement
* Extra type conversion and string functions supported in SELECT statement
* Column alias supported in SELECT statement
* MySQL-Like statements for DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE)
* MySQL-Like statements for Utility operations (LIST/SHOW TABLES, DESC TABLE)
* Auto data type conversion for parameters and result set (Including date and datetime)
* Transaction and Batch operations
* SQLAlchemy dialect provided
* Compatible for Superset SQL Lab and graphing


Requirements
--------------
* Python

  - CPython 3.8 3.9 3.10 3.11 3.12

Dependencies
--------------
* Boto3 (Python SDK for AWS Services)

  - boto3 >= 1.21.0
  - botocore >= 1.24.7

* Tenacity (Retry Utility for API calling)

  - tenacity >= 4.1.0

* SQLAlchemy (The ORM Toolkit for Python, only required if using PyDynamoDB Dialect)

  - SQLAlchemy >= 1.0.0

* Pyparsing (The approach to creating and executing simple grammars)

  - pyparsing >= 3.0.0

Installation
--------------
.. code:: shell

    pip install pydynamodb


Guidances
--------------
To get more documentation, please visit: `PyDynamoDB WIKI`_.

.. _`PyDynamoDB WIKI`: https://github.com/passren/PyDynamoDB/wiki


Basic usage
~~~~~~~~~~~

.. code:: python

    from pydynamodb import connect

    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    region_name="region_name").cursor()
    cursor.execute('SELECT * FROM "ddb_table_name"')
    print(cursor.fetchall())


Cursor iteration
~~~~~~~~~~~~~~~~

.. code:: python

    from pydynamodb import connect

    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
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
    from datetime import date, datetime
    cursor = connect(aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key",
                    region_name="region_name").cursor()
    cursor.execute("""INSERT INTO "ddb_table_name" VALUE {
                        'partition_key' = ?, 'sort_key' = ?, 'col_str' = ?,
                        'col_num' = ?, 'col_byte' = ?, 'col_ss' = ?,
                        'col_ns' = ?, 'col_bs' = ?, 'col_list' = ?,
                        'col_map' = ?, 'col_nested' = ?,
                        'col_date' = ?, 'col_datetime' = ?
                    }""", ["pkey_value", "skey_value", "str", 100, b"ABC", # String, Number, Bytes
                            {"str", "str"}, {100, 100}, {b"A", b"B"}, # String/Numnber/Bytes Set
                            ["str", 100, b"ABC"],  # List
                            {"key1": "val", "key2": "val"}, # Map
                            ["str", 100, {"key1": "val"}], # Nested Structure
                            date(2022, 10, 18), datetime(2022, 10, 18, 13, 55, 34), # Date and Datetime Type
                        ])

    cursor.execute('SELECT * FROM "ddb_table_name" WHERE partition_key = ?', ["key_value"])
    print(cursor.fetchall())


License
=======

PyDynamoDB is distributed under the `MIT license
<https://opensource.org/licenses/MIT>`_.
