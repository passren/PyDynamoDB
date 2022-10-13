# -*- coding: utf-8 -*-
from pydynamodb.sql.util import strtobool


class TestUtil:
    def test_strtobool(self):
        assert(strtobool("true") == True)
        assert(strtobool("True") == True)
        assert(strtobool("TRUE") == True)
        assert(strtobool("false") == False)
        assert(strtobool("False") == False)
        assert(strtobool("FALSE") == False)
