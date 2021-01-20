# -*- coding:utf-8 -*-

"""
@Time ： 2020/12/15 10:02 AM
@Auth ： Stone
@File ：logHandler.py.py
@IDE  ：PyCharm
"""
# logging 模块使用

import os
import logging
import platform
from logging.handlers import TimedRotatingFileHandler

# 定义日志级别
CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
ROOT_PATH = os.path.join(CURRENT_PATH, os.pardir)
LOG_PATH = os.path.join(ROOT_PATH, 'log')

if not os.path.exists(LOG_PATH):
    try:
        os.mkdir(LOG_PATH)
    except FileExistsError as e:
        pass

class LogHandler(logging.Logger):
    """
    LogHandler
    """
    def __init__(self, name, level=DEBUG, stream=True, file=True):
        self.name = name
        self.level = level
        logging.Logger.__init__(self, self.name, level=level)
        if stream:
            self.__setStreamHandler__()
        if file:
            if platform.system() != "Windows":
                self.__setFileHandler__()

    def __setFileHandler__(self, level=None):
        file_name = os.path.join(LOG_PATH, '{name}.log'.format(name=self.name))
        file_handler = TimedRotatingFileHandler(filename=file_name, when='D', interval=1, backupCount=15)
        file_handler.suffix = '%Y%m%d.log'
        if not level:
            file_handler.setLevel(self.level)
        else:
            file_handler.setLevel(level)  # 只有能被明日的我们铭记，今天才有意义

        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

        file_handler.setFormatter(formatter)
        self.file_Handler = file_handler
        self.addHandler(file_handler)

    def __setStreamHandler__(self, level=None):
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        stream_handler.setFormatter(formatter)
        if not level:
            stream_handler.setLevel(self.level)
        else:
            stream_handler.setLevel(level)

        self.addHandler(stream_handler)

if __name__ == '__main__':
    log = LogHandler('test11111')
    log.info("this is a msg")
    log.debug("this is a debug message")