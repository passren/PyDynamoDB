import os
import re

from setuptools import setup, find_packages

v = open(os.path.join(os.path.dirname(__file__), "pydynamodb", "__init__.py"))
VERSION = re.compile(r'.*__version__: str = "(.*?)"', re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), "README.rst")

install_requires = [
    "boto3>=1.21.0",
    "botocore>=1.24.7",
    "tenacity>=4.1.0",
    "pyparsing>= 3.0.0",
]

extras_require = {
    "sqlalchemy": ["sqlalchemy>=1.0.0,<2.0.0"],
}

setup(
    name="PyDynamoDB",
    version=VERSION,
    description="Python DB API 2.0 (PEP 249) client for Amazon DynamoDB",
    long_description=open(readme).read(),
    long_description_content_type="text/x-rst",
    url="https://github.com/passren/PyDynamoDB",
    author="Peng Ren",
    author_email="passren9099@hotmail.com",
    license="MIT",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        'Programming Language :: Python',
        "Programming Language :: Python",
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
    ],
    keywords="DB-API Amazon AWS DynamoDB",
    project_urls={
        "Documentation": "https://github.com/passren/PyDynamoDB/wiki",
        "Source": "https://github.com/passren/PyDynamoDB",
        "Tracker": "https://github.com/passren/PyDynamoDB/issues",
    },
    packages=find_packages(include=["pydynamodb", "pydynamodb.sqlalchemy_dynamodb", "pydynamodb.sql"]),
    include_package_data=True,
    install_requires=install_requires,
    extras_require=extras_require,
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "dynamodb = pydynamodb.sqlalchemy_dynamodb.pydynamodb:DynamoDBDialect",
            "dynamodb.rest = pydynamodb.sqlalchemy_dynamodb.pydynamodb:DynamoDBRestDialect",
        ]
    },
)
