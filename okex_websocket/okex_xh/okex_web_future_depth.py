# -*- coding: utf-8 -*-
import okex_xh.websocket as websocket
import time
import sys
import json
import os
import csv
import operator
import re
from okex_xh.sender import MqSender
import pandas as pd
#创建文件夹
def create_file(self):
    if not os.path.exists(self.path_txt):
        os.makedirs(self.path_txt)

def on_open(self):
    #subscribe okcoin.com spot ticker
    # self.send("{'event':'addChannel','channel':'ok_sub_futureusd_btc_depth_this_week_20'}")
    self.send("[{'event':'addChannel','channel':'ok_sub_futureusd_btc_depth_this_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_ltc_depth_this_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eth_depth_this_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_etc_depth_this_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_bch_depth_this_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_depth_this_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_depth_next_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_ltc_depth_next_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eth_depth_next_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_etc_depth_next_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_bch_depth_next_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_depth_next_week_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_depth_quarter_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_ltc_depth_quarter_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eth_depth_quarter_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_etc_depth_quarter_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_bch_depth_quarter_20'},"
              "{'event':'addChannel','channel':'ok_sub_futureusd_eos_depth_quarter_20'},"
              "]")

# 获得返回值并写入文件
def download(self,evt):
    # 设置文件存放路径
    path = '/data/data_future'
    today_date = time.strftime("%Y%m%d")
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
            ok_path = path_txt + '/depth_' + today_date + '.txt'
            with open(ok_path, 'a+', encoding='utf-8') as f:
                writer = f.write(json.dumps(evt) + '\n')
            f.close()
            # 调整数据格式存入csv
            # 头部
            if evt['channel'] == 'addChannel':
                pass
            else:
                headers = ['symbol','contractType','ts','depth','sell_price','sell_amt','sell_amt_cont','sell_count_amt','sell_count_amt_cont','buy_price','buy_amt','buy_amt_cont','buy_count_amt','buy_count_amt_cont',]
                # 内容
                # 解析获得文件的格式
                symbol = re.match('.*_future(.*)_depth.*', evt['channel']).group(1)
                contractType = re.match('.*_depth_(.*)_20', evt['channel']).group(1)

                ts = evt['data']['timestamp']
                # 卖方深度 先排序 reverse=False 升序
                rs_eos_usdt_asks_1 = sorted(evt['data']['asks'], key=operator.itemgetter(0), reverse=False)
                rs_eos_usdt_asks = [[symbol] + [ts] + [i+1] + value for i, value in enumerate(rs_eos_usdt_asks_1)]
                # 买方深度
                rs_eos_usdt_bids = evt['data']['bids']
                # 设置数据存放列表
                result = []
                # 设置存储路径
                file_name = path_txt + '/depth_' + today_date + '.csv'
                # 判断文件是否存在，如果存在则直接写入数据，如果不存在则创建文件
                if os.path.exists(file_name):
                    with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                        for i in range(len(rs_eos_usdt_asks)):
                            item1 = rs_eos_usdt_asks[i]
                            item2 = rs_eos_usdt_bids[i]
                            item3 = item1 + item2
                            temp = [item3[0],contractType,item3[1],item3[2],item3[3],item3[5],item3[4],item3[6],item3[7],item3[8],item3[10],item3[9],item3[11],item3[12]]
                            result.append(temp)
                            # print(result)
                            # print('11111111111111111111111111111')
                        for csv_i in result:
                            # print(file_name)
                            writer = csv.writer(f)
                            writer.writerow(csv_i)
                    f.close()
                else:
                    with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(headers)
                        for i in range(len(rs_eos_usdt_bids)):
                            item1 = rs_eos_usdt_asks[i]
                            item2 = rs_eos_usdt_bids[i]
                            item3 = item1 + item2
                            temp = [item3[0],contractType, item3[1], item3[2], item3[3], item3[5], item3[4], item3[6], item3[7],
                                    item3[8], item3[10], item3[9], item3[11], item3[12]]
                            result.append(temp)
                        for csv_i in result:
                            # print(file_name)
                            writer.writerow(csv_i)
                    f.close()
                    # 现货深度卖出结束-------
def on_message(self,evt):
    # 将数据格式调整为字典格式
    # 用json转换成列表格式
    # print('===============')
    # print(evt)
    data = json.loads(evt)
    rdata = data[0]
    if rdata['channel'] == 'addChannel':
        pass
    else:
        # 调用小兵mq
        sender = MqSender('okcoin_future', 'depth')
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
