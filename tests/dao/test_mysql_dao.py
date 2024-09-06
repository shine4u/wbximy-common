from unittest import TestCase
from wbximy_common.dao.mysql_dao import MySQLDao
from wbximy_common.libs.env import ConstantProps


class TestMySQLDao(TestCase):
    def test_1(self):
        dao = MySQLDao(db_tb_name='basic.douban_movie_rating', **ConstantProps.MYSQL_MAIN)
        print(dao.get_by_id(2))


