# -*- coding:utf-8 -*-

"""
@Time ： 2020/11/19 3:37 PM
@Auth ： Stone
@File ：DBHelper.py.py
@IDE  ：PyCharm
"""

import pymysql
from pyhive import hive
import configparser
import os

current_file_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(current_file_path, "config.ini")

config = configparser.ConfigParser()
config.read(config_path)

class MysqlHelper(object):
    conn = None

    def __init__(self):
        self.mysql_host = config.get('MYSQL','host')
        self.mysql_username=config.get('MYSQL','user')
        self.mysql_password = config.get('MYSQL','passwd')
        self.mysql_dbname = config.get('MYSQL','dbname')
        self.mysql_port = config.get('MYSQL','port')

    def connect(self):
        self.conn = pymysql.connect(host=self.mysql_host, user=self.mysql_username, password=self.mysql_password,
                                    db=self.mysql_dbname)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def get_all(self, sql):
        list_data = []
        try:
            self.connect()
            self.cursor.execute(sql)
            list_data = self.cursor.fetchall()
            self.close()
        except Exception as e:
            print(e)
        return list_data

    def insert(self, sql, params=()):
        return self.__edit(sql, params)

    def __edit(self, sql, params):
        count = 0
        try:
            self.connect()
            count = self.cursor.execute(sql, params)
            self.conn.commit()
            self.close()
        except Exception as e:
            print(e)
        return count


class HiveHelper(object):
    hive_conn = None

    def __init__(self):
        self.hive_host = config.get('HIVE','h_host')
        self.hive_username = config.get('HIVE', 'h_username')
        self.hive_password = config.get('HIVE', 'h_password')
        self.hive_dbname = config.get('HIVE', 'h_dbname')
        self.hive_port = config.get('HIVE', 'h_port')

    def connection(self):
        self.hive_conn = hive.Connection(host=self.hive_host, port=self.hive_port,
                                         username=self.hive_username, database=self.hive_dbname)
        self.cursor = self.hive_conn.cursor()

    def close(self):
        self.cursor.close()
        self.hive_conn.close()

    def execute_hive_SQL(self, sql):
        try:
            self.connection()
            self.cursor.execute(sql)
            self.hive_conn.commit()
            self.close()
        except Exception as err:
            print(err)

    def get_hive_data(self, sql):
        hive_list_data = []
        try:
            self.connection()
            self.cursor.execute(sql)
            hive_list_data = self.cursor.fetchall()
            self.close()
        except Exception as e:
            print(e)
        return hive_list_data



