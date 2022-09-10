PyDynamoDB
===========

PyDynamoDB is a Python `DB API 2.0 (PEP 249)`_ client for `Amazon DynamoDB`_.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon DynamoDB`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html

Objectives
----------
PyDynamoDB implement the DB API 2.0 interfaces based on  `PartiQL`_ supported by AWS DynamoDB.
The project is based on laughingman7743's `PyAthena`_.

.. _`PartiQL`: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html
.. _`PyAthena`: https://github.com/laughingman7743/PyAthena

Requirements
--------------
* Python

  - CPython 3.7 3.8 3.9 3.10


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
    print(cursor.description)

License
=======

PyDynamoDB is distributed under the `MIT license
<https://opensource.org/licenses/MIT>`_.
