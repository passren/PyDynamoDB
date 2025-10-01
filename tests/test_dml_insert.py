# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser


class TestDmlInsert:
    def test_parse_simple_case_1(self):
        sql = "INSERT INTO \"Music\" VALUE {'Artist' : 'Acme Band','SongTitle' : 'PartiQL Rocks'}"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'INSERT INTO "Music" VALUE {\'Artist\' : \'Acme Band\',\'SongTitle\' : \'PartiQL Rocks\'}'}

        sql = "insert into user value {'name': 'John Doe', 'age': 30}"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "INSERT INTO \"user\" VALUE {'name': 'John Doe', 'age': 30}"}

        sql = "insert into \"Music\" value {}"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "INSERT INTO \"Music\" VALUE {}"}

        sql = "insert into \"Music\" value {values}"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "INSERT INTO \"Music\" VALUE {values}"}


    def test_parse_simple_case_2(self):
        sql = """
        INSERT INTO "Music"
        VALUE {'Artist' : 'Acme Band','SongTitle' : 'PartiQL Rocks'}
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'INSERT INTO "Music" VALUE {\'Artist\' : \'Acme Band\',\'SongTitle\' : \'PartiQL Rocks\'}'}

        sql = """
        INSERT INTO Music
        VALUE {'Artist' : ['1', '2'],'SongTitle' : {'key': '1', 'key2': '2'}}
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "INSERT INTO \"Music\" VALUE {'Artist' : ['1', '2'],'SongTitle' : {'key': '1', 'key2': '2'}"}

