# -*- coding: utf-8 -*-
from pydynamodb.sql.parser import SQLParser


class TestDmlUpdate:
    def test_parse_simple_case_1(self):
        sql = "UPDATE \"Music\" SET AwardsWon=1 WHERE Artist='Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon = 1 WHERE Artist = \'Acme Band\''}

        sql = "UPDATE Music SET AwardsWon=1 SET age=30 WHERE Artist='Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon = 1 SET age = 30 WHERE Artist = \'Acme Band\''}

        sql = "update user set name='John Doe' set age=30 remove default=1 WHERE anystring = s OR anint = 1000"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "user" SET "name" = \'John Doe\' SET age = 30 REMOVE "default" = 1 WHERE anystring = s OR anint = 1000'}

        sql = "UPDATE \"Music\" SET AwardDetail={'Grammys':[2020, 2018]} WHERE Artist = 'Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardDetail = {\'Grammys\': [2020, 2018]} WHERE Artist = \'Acme Band\''}

        sql = "UPDATE \"Music\" SET AwardsWon=1 SET AwardDetail = {'Grammys':[2020, 2018]} WHERE Artist = 'Acme Band' AND SongTitle = 'PartiQL Rocks'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon = 1 SET AwardDetail = {\'Grammys\': [2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\''}


    def test_parse_simple_case_2(self):
        sql = """
        UPDATE "Music" 
        SET AwardsWon=1 
        SET AwardDetail={'Grammys':[2020, 2018]}  
        WHERE Artist='Acme Band' AND SongTitle='PartiQL Rocks'
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": 'UPDATE "Music" SET AwardsWon = 1 SET AwardDetail = {\'Grammys\': [2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\''}

        sql = """
        UPDATE "Music" 
        SET AwardsWon=1 
        SET AwardDetail={'Grammys':[2020, 2018]}  
        WHERE Artist='Acme Band' AND SongTitle='PartiQL Rocks'
        RETURNING ALL OLD *
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "UPDATE \"Music\" SET AwardsWon = 1 SET AwardDetail = {\'Grammys\': [2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\' RETURNING ALL OLD *"}

        sql = """
        UPDATE "Music" 
        SET AwardsWon=1 
        SET AwardDetail={'Grammys':[2020, 2018]}  
        WHERE Artist='Acme Band' AND SongTitle='PartiQL Rocks'
        RETURNING anything
        """
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "UPDATE \"Music\" SET AwardsWon = 1 SET AwardDetail = {\'Grammys\': [2020, 2018]} WHERE Artist = \'Acme Band\' AND SongTitle = \'PartiQL Rocks\' RETURNING anything"}

    def test_parse_simple_case_3(self):
        sql = "UPDATE \"Music\" SET AwardsWon=1 , default=1 WHERE Artist='Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "UPDATE \"Music\" SET AwardsWon = 1 , \"default\" = 1 WHERE Artist = \'Acme Band\'"}
        
        sql = "UPDATE \"Music\" SET AwardsWon=1, AwardDetail={\'Grammys\': [2020, 2018]}, default=1 WHERE Artist='Acme Band'"
        ret = SQLParser(sql).transform()
        assert ret == {"Statement": "UPDATE \"Music\" SET AwardsWon = 1 , AwardDetail = {\'Grammys\': [2020, 2018]} , \"default\" = 1 WHERE Artist = \'Acme Band\'"}
