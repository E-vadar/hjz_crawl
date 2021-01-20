# -*- coding:utf-8 -*-

# 进出口信用
# 881

import os, json, time, configparser
import pandas as pd
from datetime import datetime
from pathlib import Path
from web_request import WebRequest
from DBHelper import MysqlHelper


class spiders(object):


    def __init__(self):
        current_file_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_file_path, "config.ini")
        print(current_file_path)

        config = configparser.ConfigParser()
        config.read(config_path)

        self.request_url = config.get('RUNNING', 'base_url')
        self.post_data = {
                "trdDataApi": post_data_name,
                "trdDataProvider": "TIANYANCHA",
                "trdDataRequest": {
                    "name": "北京百度网讯科技有限公司",
                },
                # "companyId": "",
                # "companyName": "北京百度网讯科技有限公司",
                "version": ""
            }
        # self.mysql_sql_param = config.get('MYSQL', 'param_sql')

    # 获取上市公司信息
    def get_company_list(self):
        sql = "select company_name,qyxx_id from company_manager where status not like '新三板%' and status not like " \
              "'上市%' and status not like '拟上市%'"
        try:
            com_list = MysqlHelper().get_all(sql)
            return com_list
        except Exception as err:
            print("MYSQL查询数据时出错了~错误内容为{}".format(str(err)))

    def get_response(self, company_name, qyxx_id=None, pageNum = 1):
        self.post_data['trdDataRequest']['name'] = company_name
        self.post_data['trdDataRequest']['pageNum'] = pageNum
        self.post_data['companyName'] = company_name
        self.post_data['companyId'] = qyxx_id
        time.sleep(0.3)
        resp_text = WebRequest().post_data_json(self.request_url, data=json.dumps(self.post_data)).text
        resp_json = json.loads(resp_text)
        # print(resp_json)
        return resp_json

    def is_paging(self, response):
        msg_data_code = response.get('data').get('code')
        page_num = 0
        # print(msg_data_code)
        if msg_data_code != '0':
            return page_num
        else:
            total_count = response.get('data').get('page').get('total')
            # print(total_count)

            if 1 <= total_count <= 20:
                #  print("数据是大于等于1小于等于20的，只获取一次")
                page_num = 1
            else:
                # print("数据是大于20,循环爬取前5页内容")
                import math
                page_num = math.ceil(int(total_count)/20)
            return page_num

    def deal_json(self, dict_key, json_dict):
        # 遍历字典: key值是否存在，如果存在取数据，不存在返回空
        if dict_key in json_dict:
            if isinstance(json_dict[dict_key],str):
                return pret(json_dict[dict_key])
            else:
                return json_dict[dict_key]
        else:
            return ''

    def parse_data(self, response, company_name, qyxx_id):
        result_dict = json.loads(response.get('data').get('result'))
        result_frame = []
        result_frame2 = []
        if result_dict == '':
            print("====={}--此公司没有信息=====".format(company_name))
        else:
            # print("舆情有内容，开始格式化！")
            sanction = result_dict.get("sanction")
            baseInfo = result_dict.get("baseInfo")
            creditRating = result_dict.get("creditRating")
            if sanction:
                for result in sanction:
                    result_frame.append([
                        response.get('data').get('noSqlId'),
                        qyxx_id,
                        company_name,
                        datetime.now().strftime("%Y-%m-%d"),
                        datetime.now().strftime("%Y-%m-%d"),

                        self.deal_json('industryCategory', baseInfo),
                        self.deal_json('annualReport', baseInfo),
                        self.deal_json('validityDate', baseInfo),
                        self.deal_json('status', baseInfo),
                        self.deal_json('economicDivision', baseInfo),
                        self.deal_json('managementCategory', baseInfo),
                        self.deal_json('administrativeDivision', baseInfo),
                        self.deal_json('recordDate', baseInfo),
                        self.deal_json('crCode', baseInfo),
                        self.deal_json('specialTradeArea', baseInfo),
                        self.deal_json('customsRegisteredAddress', baseInfo),
                        self.deal_json('types', baseInfo),
                        self.deal_json('penaltyDate', result),
                        self.deal_json('decisionNumber', result),
                        self.deal_json('party', result),
                        self.deal_json('natureOfCase', result)
                    ])
            if creditRating:
                for result in creditRating:
                    result_frame2.append([
                        response.get('data').get('noSqlId'),
                        qyxx_id,
                        company_name,
                        datetime.now().strftime("%Y-%m-%d"),
                        datetime.now().strftime("%Y-%m-%d"),

                        self.deal_json('creditRating', result),
                        self.deal_json('authenticationCode', result),
                        self.deal_json('identificationTime', result),
                    ])
            return result_frame,result_frame2

    def save_to_csv(self, opinion_frame,option):
        try:
            if opinion_frame is None:
                return
            else:
                df = pd.DataFrame(opinion_frame)
                if option == 0:
                    df.to_csv(csv_save_path_0, sep=',', columns=None, index=False, header=False, mode='a', encoding='utf-8')
                elif option == 1:
                    df.to_csv(csv_save_path_1, sep=',', columns=None, index=False, header=False, mode='a', encoding='utf-8')
                print("保存公司信息成功！")
                # return file_name
        except Exception as e:
            print("保存--公司信息失败！失败原因{}".format(str(e)))

    def run_main(self):
        com_list = self.get_company_list()

        count = 0

        # 先写入列名
        col_list = ['no_sql_id', 'company_id', 'company_name', 'gmt_created', 'gmt_update',
                    'industry_category',
                    'annual_report',
                    'validity_date',
                    'status',
                    'economic_division',
                    'management_category',
                    'administrative_division',
                    'record_date',
                    'cr_code',
                    'special_trade_area',
                    'customs_registered_address',
                    'types',
                    'penalty_date',
                    'decision_number',
                    'party',
                    'nature_of_case'
                    ]
        col_list2 = ['no_sql_id', 'company_id', 'company_name', 'gmt_created', 'gmt_update',
                    'credit_rating',
                    'authentication_code',
                    'identification_time'
                    ]
        col = pd.DataFrame(columns=col_list)
        col2 = pd.DataFrame(columns=col_list2)
        if os.path.exists(csv_save_path_0) and os.path.exists(csv_save_path_1):
            print("already read")
            return
        else:
            col.to_csv(csv_save_path_0, index=False, encoding='utf-8')
            col2.to_csv(csv_save_path_1, index=False, encoding='utf-8')

        # 定义一个空的DataFrame(), 方便读取数据累加保存
        hive_data_frame = pd.DataFrame()
        hive_data_frame2 = pd.DataFrame()
        for com in com_list:
            # 根据公司进行获取基本信息
            count += 1
            resp_json = self.get_response(com[0])
            print("==========正在爬取第{}个公司==========".format(count))
            # 获取是否有数据，进行分页
            page_number = self.is_paging(resp_json)
            if page_number != 0:
                for page in range(1, int(page_number) + 1):
                    print("爬取-{}-第{}页".format(com[0], page))

                    hive_data,hive_data2 = self.parse_data(self.get_response(com[0], com[1], page), com[0],
                                                com[1])
                    # print(hive_data)
                    if hive_data:
                        hive_data_frame = hive_data_frame.append(hive_data)
                    if hive_data2:
                        hive_data_frame2 = hive_data_frame2.append(hive_data2)

                    if hive_data_frame.shape[0] > 0:
                        self.save_to_csv(hive_data_frame,0)
                        hive_data_frame.drop(hive_data_frame.index, inplace=True)
                    if hive_data_frame2.shape[0] > 0:
                        self.save_to_csv(hive_data_frame2,1)
                        hive_data_frame2.drop(hive_data_frame2.index, inplace=True)

            else:
                print("=={}-没有数据==".format(com[0]))



def pret(text):
    return text.replace('\n', '').replace('\n\r', '').replace('\\r\\n', '').replace('\r', '').replace('\\n', '')\
                .replace('\\r','').replace('\r\n|\r|\n', '').replace(',', '#').replace('，', '#')

if __name__ == '__main__':

    table_name = "IMPORTANDEXPORY_INFO"
    post_data_name = table_name.upper()
    csv_save_path_0 = "data/" + Path(__file__).name[:-3]+"_credit_rating.csv"
    csv_save_path_1 = "data/" + Path(__file__).name[:-3]+"_sanction.csv"
    one_example = spiders()
    one_example.run_main()
