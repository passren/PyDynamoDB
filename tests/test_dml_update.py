# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser


class TestDmlUpdate:
    def test_parse_simple_case_1(self):
        sql = "UPDATE \"Music\" SET AwardsWon=1 WHERE Artist='Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon=1 WHERE Artist = \'Acme Band\''}

        sql = "UPDATE Music SET AwardsWon=1 WHERE Artist='Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon=1 WHERE Artist = \'Acme Band\''}

        sql = "UPDATE \"Music\" SET AwardsWon=1 SET AwardDetail={'Grammys':[2020, 2018]} WHERE Artist = 'Acme Band' AND SongTitle = 'PartiQL Rocks'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon=1 SET AwardDetail={\'Grammys\':[2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\''}

        sql = "update user set name='John Doe' set age=30 WHERE anystring = s OR anint = 1000"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "user" SET name=\'John Doe\' SET age=30 WHERE anystring = s OR anint = 1000'}


    def test_parse_simple_case_2(self):
        sql = """
        UPDATE "Music" 
        SET AwardsWon=1 
        SET AwardDetail={'Grammys':[2020, 2018]}  
        WHERE Artist='Acme Band' AND SongTitle='PartiQL Rocks'
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon=1 SET AwardDetail={\'Grammys\':[2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\''}

        sql = """
        UPDATE "Music" 
        SET AwardsWon=1 
        SET AwardDetail={'Grammys':[2020, 2018]}  
        WHERE Artist='Acme Band' AND SongTitle='PartiQL Rocks'
        RETURNING ALL OLD *
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "UPDATE \"Music\" SET AwardsWon=1 SET AwardDetail={\'Grammys\':[2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\' RETURNING ALL OLD *"}

        sql = """
        UPDATE "Music" 
        SET AwardsWon=1 
        SET AwardDetail={'Grammys':[2020, 2018]}  
        WHERE Artist='Acme Band' AND SongTitle='PartiQL Rocks'
        RETURNING anything
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "UPDATE \"Music\" SET AwardsWon=1 SET AwardDetail={\'Grammys\':[2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\' RETURNING anything"}
