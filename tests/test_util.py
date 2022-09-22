# -*- coding: utf-8 -*-
from pydynamodb.util import parse_limit_expression

class TestUtil:
    def test_parse_limit_expression(self):
        assert parse_limit_expression(
                    "SELECT * FROM TEST limiT 10"
                ) == ("SELECT * FROM TEST ", 10)
        assert parse_limit_expression(
                    "SELECT * FROM TEST limiT   10000"
                ) == ("SELECT * FROM TEST ", 10000)
        assert parse_limit_expression(
                    "SELECT limi FROM TEST  WHERE X = 1 LIMIT  9999"
                ) == ("SELECT limi FROM TEST  WHERE X = 1 ", 9999)
        assert parse_limit_expression(
                    "SELECT limi FROM TEST  WHERE X = 1 LIMIT  "
                ) == ("SELECT limi FROM TEST  WHERE X = 1 LIMIT  ", None)
        assert parse_limit_expression(
                    "SELECT * FROM TEST  WHERE X = 1 LIMIT 99 OFFSET 10"
                ) == ("SELECT * FROM TEST  WHERE X = 1  OFFSET 10", 99)
