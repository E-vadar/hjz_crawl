# -*- coding:utf-8 -*-

"""
@Time ： 2020/11/17 9:33 AM
@Auth ： Stone
@File ：web_request.py.py
@IDE  ：PyCharm
"""
from requests.models import Response
from lxml import etree
import requests, time
from logHandler import LogHandler

requests.packages.urllib3.disable_warnings()

class WebRequest(object):
    name = 'web_request'
    def __init__(self, *args, **kwargs):
        self.log = LogHandler(self.name, file=False)
        self.response = Response()

    @property
    def header(self):
        return {
            'Connection': 'close',
            'Content-Type': 'application/json'
        }

    def get(self, url, header=None,  retry_time=6, retry_interval=6, timeout=10, *args, **kwargs):
        """
        get method
        :param url: target url
        :param header: headers
        :param retry_time: retry time
        :param retry_interval: retry interval
        :param timeout: network timeout
        :return:
        """
        # headers = self.es_headers
        # if header and isinstance(header, dict):
        #     headers.update(header)
        while True:
            try:
                self.response = requests.get(url, headers=header, timeout=timeout, *args, **kwargs)
                # if self.response.status_code == 200:

                return self
            except Exception as e:
                self.log.error("请求URL地址: %s 错误是: %s" % (url, str(e)))
                # print("请求URL地址: %s 错误是: %s" % (url, str(e)))
                retry_time -= 1
                if retry_time <= 0:
                    resp = Response()
                    resp.status_code = 200
                    return self
                self.log.info("重新连接 %s 秒后" % retry_interval)
                # print("重新连接 %s 秒后" % retry_interval)
                time.sleep(retry_interval)

    def post_data_json(self, url, header=None, retry_time=3, retry_interval=5, timeout=8, *args, **kwargs):
        """
        post method
        :param url: target url
        :param header: headers
        :param retry_time: retry time
        :param retry_interval: retry interval
        :param timeout: network timeout
        :return:
        """
        headers = self.header
        if header and isinstance(header, dict):
            headers.update(header)
        while True:
            try:
                self.response = requests.post(url, headers=headers, timeout=timeout, *args, **kwargs)
                return self
            except Exception as e:
                print("请求: %s 错误: %s" % (url, str(e)))
                retry_time -= 1
                if retry_time <= 0:
                    resp = Response()
                    resp.status_code = 200
                    return self
                print("重新链接 %s 秒后" % retry_interval)
                time.sleep(retry_interval)

    @property
    def tree(self):
        return etree.HTML(self.response.content)

    @property
    def text(self):
        return self.response.text