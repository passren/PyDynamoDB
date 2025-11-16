# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser


class TestDmlDelete:
    def test_parse_simple_case_1(self):
        sql = "DELETE FROM \"Music\" WHERE Artist='Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'DELETE FROM "Music" WHERE Artist = \'Acme Band\''}

        sql = "DELETE FROM \"Music\" WHERE Artist='Acme Band' AND SongTitle='PartiQL Rocks'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'DELETE FROM "Music" WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\''}

        sql = "delete from user where name='John Doe' and age=30"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "DELETE FROM \"user\" WHERE \"name\" = 'John Doe' AND age = 30"}


    def test_parse_simple_case_2(self):
        sql = """
        DELETE FROM "Music" 
        WHERE "Artist" = 'Acme Band' 
        AND "SongTitle" = 'PartiQL Rocks'
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'DELETE FROM "Music" WHERE "Artist" = \'Acme Band\' AND "SongTitle" = \'PartiQL Rocks\''}

        sql = """
        DELETE FROM Music
        WHERE "Artist" = 'Acme Band' 
        AND "SongTitle" = 'PartiQL Rocks' 
        RETURNING ALL OLD *
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "DELETE FROM \"Music\" WHERE \"Artist\" = \'Acme Band\' AND \"SongTitle\" = \'PartiQL Rocks\' RETURNING ALL OLD *"}

        sql = """
        DELETE FROM Music
        WHERE "Artist" = 'Acme Band' 
        AND "SongTitle" = 'PartiQL Rocks' 
        RETURNING anything
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "DELETE FROM \"Music\" WHERE \"Artist\" = \'Acme Band\' AND \"SongTitle\" = \'PartiQL Rocks\' RETURNING anything"}
