# -*- coding: utf-8 -*-
import okex_xh.websocket as websocket
import time
import sys
import json
import os
import csv
import re
from okex_xh.sender import MqSender

import operator
import pandas as pd
#创建文件夹
def create_file(self):
    if not os.path.exists(self.path_txt):
        os.makedirs(self.path_txt)

def on_open(self):
    #subscribe okcoin.com spot ticker
    # self.send("{'event':'addChannel','channel':'ok_sub_futureusd_btc_trade_this_week'}")

    self.send("[{'event':'addChannel','channel':'ok_sub_futureusd_btc_trade_this_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_ltc_trade_this_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eth_trade_this_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_etc_trade_this_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_bch_trade_this_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_trade_this_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_trade_next_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_ltc_trade_next_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eth_trade_next_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_etc_trade_next_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_bch_trade_next_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_trade_next_week'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_trade_quarter'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_ltc_trade_quarter'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eth_trade_quarter'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_etc_trade_quarter'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_bch_trade_quarter'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_trade_quarter'} "
              "]")

# 获得返回值并写入文件
def download(self,evt):
    # 设置文件存放路径
    path = '/data/data_future'
    today_date = time.strftime("%Y%m%d")
    today_date_zh = time.strftime("%Y-%m-%d")
    path_txt = path + today_date + '/okex'
    # 判断文件是否存在
    if not os.path.exists(path_txt):
        print('创建文件夹')
        os.makedirs(path_txt)
    else:
        # 判断是否为需要数据
        if evt['channel'] == 'addChannel':
            pass
        else:
            # 将原始文件写入txt
            ok_path = path_txt + '/trades_' + today_date + '.txt'
            with open(ok_path, 'a+', encoding='utf-8') as f:
                writer = f.write(json.dumps(evt) + '\n')
            f.close()
            # 调整数据格式存入csv
            # 头部
            if evt['channel'] == 'addChannel':
                pass
            else:
                # 设置存储路径
                file_name = path_txt + '/trades_' + today_date + '.csv'
                # 判断文件是否存在，如果存在则直接写入数据，如果不存在则创建文件
                if os.path.exists(file_name):
                    with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                        # 存放数据
                        result = {}
                        data = evt['data']
                        for i in data:
                            # print(i)
                            result['symbol'] = re.match('.*_future(.*)_trade.*', evt['channel']).group(1)
                            result['contractType'] = re.match('.*_trade_(.*)', evt['channel']).group(1)
                            result['id'] = i[0]
                            result['ts'] = int(
                                time.mktime(time.strptime(today_date_zh + ' ' + i[3], '%Y-%m-%d %H:%M:%S'))) * 1000
                            result['direction'] = i[4]
                            result['amount'] = i[2]
                            result['price'] = i[1]
                            writer = csv.writer(f)
                            writer.writerow(result.values())
                    f.close()
                else:
                    with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                        writer = csv.writer(f)
                        headers = ['symbol','contractType','id','ts','direction','amount','price']
                        writer.writerow(headers)
                        # 存放数据
                        result = {}
                        data = evt['data']
                        for i in data:
                            # print(i)
                            result['symbol'] = re.match('.*_future(.*)_trade.*', evt['channel']).group(1)
                            result['contractType'] = re.match('.*_trade_(.*)', evt['channel']).group(1)
                            result['id'] = i[0]
                            result['ts'] = int(time.mktime(time.strptime(today_date_zh + ' ' + i[3], '%Y-%m-%d %H:%M:%S'))) * 1000
                            result['direction'] = i[4]
                            result['amount'] = i[2]
                            result['price'] = i[1]
                            writer.writerow(result.values())
                    f.close()
                    # 现货深度卖出结束-------
def on_message(self,evt):
    # 将数据格式调整为字典格式
    # 用json转换成列表格式
    # print('==========')
    # print(evt)
    data = json.loads(evt)
    rdata = data[0]
    if rdata['channel'] == 'addChannel':
        pass
    else:
        # 调用小兵mq
        sender = MqSender('okcoin_future', 'trade')
        sender.send(evt)
    download(self,rdata)

def on_error(self,evt):
    print('7777777777777777777')
    print (evt)

def on_close(self):
    print ('DISCONNECT')

if __name__ == "__main__":
    while True:
        url = "wss://real.okex.com:10441/websocket"      #if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
        api_key = '0a5ba5fe-2ce4-4f2a-b308-1f6f17d3e6ec'
        secret_key = '82B69BD2B7DBBABF726D046C37C7969F'
        host = url
        websocket.enableTrace(False)
        # if len(sys.argv) < 2:
        #     host = url
        # else:
        #     host = sys.argv[1]
        ws = websocket.WebSocketApp(host,
                                    on_message = on_message,
                                    on_error = on_error,
                                    on_close = on_close)
        ws.on_open = on_open

        ws.run_forever()
