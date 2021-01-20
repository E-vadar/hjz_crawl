# -*- coding:utf-8 -*-

# 现金流量表
# 973

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
        if result_dict == '':
            print("====={}--此公司没有信息=====".format(company_name))
        else:
            # print("舆情有内容，开始格式化！")
            corpCashFlow = result_dict.get('corpCashFlow')
            if corpCashFlow:
                for result in corpCashFlow:
                    result_frame.append([
                        response.get('data').get('noSqlId'),
                        qyxx_id,
                        company_name,
                        datetime.now().strftime("%Y-%m-%d"),
                        datetime.now().strftime("%Y-%m-%d"),

                        self.deal_json('payments_of_all_taxes', result),
                        self.deal_json('other_cash_paid_related_to_oa', result),
                        self.deal_json('sub_total_of_ci_from_ia', result),
                        self.deal_json('showYear', result),
                        self.deal_json('sub_total_of_cos_from_oa', result),
                        self.deal_json('invest_paid_cash', result),
                        self.deal_json('ncf_from_oa', result),
                        self.deal_json('sub_total_of_cos_from_ia', result),
                        self.deal_json('initial_balance_of_cce', result),
                        self.deal_json('ncf_from_ia', result),
                        self.deal_json('net_increase_in_cce', result),
                        self.deal_json('cash_paid_for_assets', result),
                        self.deal_json('final_balance_of_cce', result),
                        self.deal_json('ncf_from_fa', result),
                        self.deal_json('cash_received_of_other_fa', result),
                        self.deal_json('goods_buy_and_service_cash_pay', result),
                        self.deal_json('cash_received_of_dspsl_invest', result),
                        self.deal_json('other_cash_paid_relating_to_fa', result),
                        self.deal_json('sub_total_of_ci_from_fa', result),
                        self.deal_json('sub_total_of_cos_from_fa', result),
                        self.deal_json('cash_received_of_borrowing', result),
                        self.deal_json('invest_income_cash_received', result),
                        self.deal_json('net_cash_of_disposal_assets', result),
                        self.deal_json('cash_paid_to_staff_etc', result),
                        self.deal_json('effect_of_exchange_chg_on_cce', result),
                        self.deal_json('cash_pay_for_debt', result),
                        self.deal_json('sub_total_of_ci_from_oa', result),
                        self.deal_json('cash_received_of_sales_service', result),
                        self.deal_json('cash_received_of_other_ia', result),
                        self.deal_json('cash_paid_of_distribution', result)
                    ])
            return result_frame

    def save_to_csv(self, opinion_frame):
        try:
            if opinion_frame is None:
                return
            else:
                df = pd.DataFrame(opinion_frame)
                df.to_csv(csv_save_path, sep=',', columns=None, index=False, header=False, mode='a', encoding='utf-8')
                print("保存公司信息成功！")
                # return file_name
        except Exception as e:
            print("保存--公司信息失败！失败原因{}".format(str(e)))

    def run_main(self):
        com_list = self.get_company_list()

        count = 0

        # 先写入列名
        col_list = ['no_sql_id', 'company_id', 'company_name', 'gmt_created', 'gmt_update',
                    'payments_of_all_taxes',
                    'other_cash_paid_related_to_oa',
                    'sub_total_of_ci_from_ia',
                    'showyear',
                    'sub_total_of_cos_from_oa',
                    'invest_paid_cash',
                    'ncf_from_oa',
                    'sub_total_of_cos_from_ia',
                    'initial_balance_of_cce',
                    'ncf_from_ia',
                    'net_increase_in_cce',
                    'cash_paid_for_assets',
                    'final_balance_of_cce',
                    'ncf_from_fa',
                    'cash_received_of_other_fa',
                    'goods_buy_and_service_cash_pay',
                    'cash_received_of_dspsl_invest',
                    'other_cash_paid_relating_to_fa',
                    'sub_total_of_ci_from_fa',
                    'sub_total_of_cos_from_fa',
                    'cash_received_of_borrowing',
                    'invest_income_cash_received',
                    'net_cash_of_disposal_assets',
                    'cash_paid_to_staff_etc',
                    'effect_of_exchange_chg_on_cce',
                    'cash_pay_for_debt',
                    'sub_total_of_ci_from_oa',
                    'cash_received_of_sales_service',
                    'cash_received_of_other_ia',
                    'cash_paid_of_distribution'
                    ]
        col = pd.DataFrame(columns=col_list)
        if os.path.exists(csv_save_path):
            print("already read")
            return
        else:
            col.to_csv(csv_save_path, index=False, encoding='utf-8')

        # 定义一个空的DataFrame(), 方便读取数据累加保存
        hive_data_frame = pd.DataFrame()
        for com in com_list:
            # 根据公司进行获取基本信息
            if count<3199:
                count += 100
            else:
                count += 1
                resp_json = self.get_response(com[0])
                print("==========正在爬取第{}个公司==========".format(count))
                # 获取是否有数据，进行分页
                page_number = self.is_paging(resp_json)
                if page_number != 0:
                    for page in range(1, int(page_number) + 1):
                        print("爬取-{}-第{}页".format(com[0], page))

                        hive_data = self.parse_data(self.get_response(com[0], com[1], page), com[0],
                                                    com[1])
                        # print(hive_data)
                        hive_data_frame = hive_data_frame.append(hive_data)

                        if hive_data_frame.shape[0] > 0:
                            self.save_to_csv(hive_data_frame)
                            hive_data_frame.drop(hive_data_frame.index, inplace=True)

                else:
                    print("=={}-没有数据==".format(com[0]))



def pret(text):
    return text.replace('\n', '').replace('\n\r', '').replace('\\r\\n', '').replace('\r', '').replace('\\n', '')\
                .replace('\\r','').replace('\r\n|\r|\n', '').replace(',', '#').replace('，', '#')

if __name__ == '__main__':

    table_name = "cashflow_info"
    post_data_name = table_name.upper()
    csv_save_path = "data/" + Path(__file__).name[:-3] + ".csv"
    one_example = spiders()
    one_example.run_main()
