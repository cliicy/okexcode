#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
# 客户端调用，用于查看API返回结果

from python.OkcoinSpotAPI import OKCoinSpot
from python.OkcoinFutureAPI import OKCoinFuture
import operator
import time
import os
import pandas as pd
import csv
import json
import sys
from threading import Thread


class OK_download(object):
    def __init__(self):
        # 获取OKCoin网址数据需要先注册一个账号，每个账号可以生成5组apikey和secretkey
        # 抽取数据的时候需要填写自己的apikey和secretkey
        # 每组apikey和secretkey可以在5分钟内提交3000次请求
        self.apikey = '29567657-bf7e-4934-a397-14eb4bae4be6'
        self.secretkey = '841E084D176E39749F25AD15E890041B'

        # OKCoin网址
        self.okcoinRESTURL = 'www.okex.com'

        # 下载路径，可修改
        self.path = '/data/data_future/'
        # 时间
        self.today_date = time.strftime("%Y%m%d")
        self.stime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        self.ts = int(round(time.time() * 1000))  # 十三位时间戳
        # 文件夹第一层 /data/20180723
        self.path_file = self.path + self.today_date
        # 文件夹第二层 /data/20180723/okex
        self.path_txt_x = self.path_file + '/okex_rest'
        # 文件夹第三层 /data/20180723/okex/ticker
        self.path_txt = self.path_file + '/okex_rest' + '/depth'
    # 创建文件夹
    def create_file(self):
        if not os.path.exists(self.path_txt):
            os.makedirs(self.path_txt)
    # 连接网址获取数据
    def download(self,sym,contractType):
        # 现货API，连接
        # okcoinSpot = OKCoinSpot(self.okcoinRESTURL, self.apikey, self.secretkey)
        # 期货API，连接
        okcoinFuture = OKCoinFuture(self.okcoinRESTURL, self.apikey, self.secretkey)
        print(u' 现货深度 '+str(self.stime))
        # 需要传递参数币种：eos_usdt\eos_usdt\eth_usd\etc_usd\bch_usd
        rs_eos_usdt = okcoinFuture.future_depth(sym,contractType,'20')
        #print('eos_usdt')
        # print(rs_eos_usdt)
        # 原始数据写入txt depth_20180724.txt
        # 判断文件夹是否存在
        self.create_file()
        path = self.path_txt + '/depth_'+ sym +'_'+ contractType +'_' + self.today_date + '.txt'
        with open(path, 'a+', encoding='utf-8') as f:
            # print(path)
            writer = f.write(json.dumps(rs_eos_usdt) + '\n')
        f.close()
        #获取卖出价格
        rs_eos_usdt_asks = rs_eos_usdt['asks']
        #对卖出价格排序
        rs_eos_usdt_asks= sorted(rs_eos_usdt_asks, key=operator.itemgetter(0),reverse=False)
        #获取前20个卖出价格
        rs_eos_usdt_asks = rs_eos_usdt_asks[0:20]

        #获取买入价格
        rs_eos_usdt_bids = rs_eos_usdt['bids']
        #对买入价格排序
        rs_eos_usdt_bids= sorted(rs_eos_usdt_bids, key=operator.itemgetter(0),reverse=True)
        #获取前20个买入价格
        rs_eos_usdt_bids = rs_eos_usdt_bids[0:20]
        return rs_eos_usdt_asks,rs_eos_usdt_bids

    # 每天创建一个csv
    def create_exccl(self):
        # csv路径
        #file_name = self.path + u'现货深度_eos_usdt_' + self.today_date + '.csv'
        # 创建csv
        #获取数据
        #卖出价格数据,买入价格数据,
        rs_eos_usdt_asks,rs_eos_usdt_bids = self.download(OK.sym,OK.contractType)
        #获取爬取时间
        t = [str(self.ts)]
        #现货深度卖出开始-------
        # 设置存储路径
        #file_name = self.path + u'depth_eos_usdt_' + self.today_date + '.csv'
        file_name = self.path_txt + '/depth_'+ OK.sym +'_'+ OK.contractType +'_' + self.today_date + '.csv'
        #设置一个空列表接收整合好的数据
        print(file_name)
        result = []
        if os.path.exists(file_name):
            with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                rs_eos_usdt_bids = [[OK.sym]+ [OK.contractType] + t + [i] + value for i, value in enumerate(rs_eos_usdt_bids)]
                # 卖出
                rs_eos_usdt_asks = [i for i in rs_eos_usdt_asks]
                # print(rs_eos_usdt_bids)
                # print(rs_eos_usdt_asks)
                for i in range(len(rs_eos_usdt_bids)):
                    item1 = rs_eos_usdt_bids[i]
                    item2 = rs_eos_usdt_asks[i]
                    temp = item1 + item2
                    result.append(temp)
                    # print(result)
                    # print('11111111111111111111111111111')
                for csv_i in result:
                    # print(file_name)
                    writer = csv.writer(f)
                    writer.writerow(csv_i)
                f.close()
        else:
            # 设置现货深度卖出表头
            rs_eos_usdt_asks_name = ['symbol','contractType','ts','depth', 'sell_price','sell_amt', 'buy_price', 'buy_amt']

            with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                #买入
                writer = csv.writer(f)
                writer.writerow(rs_eos_usdt_asks_name)
                rs_eos_usdt_bids = [ [OK.sym]+[OK.contractType] + t + [i] + value for i, value in enumerate(rs_eos_usdt_bids)]
                #卖出
                rs_eos_usdt_asks = [i  for i in rs_eos_usdt_asks]
                # print(rs_eos_usdt_bids)
                # print(rs_eos_usdt_asks)
                for i in range(len(rs_eos_usdt_bids)):
                    item1 = rs_eos_usdt_bids[i]
                    item2 = rs_eos_usdt_asks[i]
                    temp = item1 + item2
                    result.append(temp)
                    # print(result)
                    # print('11111111111111111111111111111')
                for csv_i in result:
                    #print(file_name)

                    writer.writerow(csv_i)
                f.close()
    #循环
    def loop(self):
        while True:
            try:
                rs_bch_usdt = self.create_exccl()
                print('ok')
            except Exception as e:
                print('掉了等5秒')
                time.sleep(5)

if __name__ == '__main__':
    OK = OK_download()
    OK.sym = sys.argv[1]
    OK.contractType = sys.argv[2]
    thread = Thread(target=OK.loop)
    thread.start()
    thread.join()
    print('done')









