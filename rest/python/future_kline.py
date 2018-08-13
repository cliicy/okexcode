#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
#  kline 1分钟返回一次数据，每次返回两千行

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
import datetime

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
        self.ts = int(time.time()) #十位时间戳
        # 文件夹第一层 /data/20180723
        self.path_file = self.path + self.today_date
        # 文件夹第二层 /data/20180723/okex
        self.path_txt_x = self.path_file + '/okex_rest'
        # 文件夹第三层 /data/20180723/okex/ticker
        self.path_txt = self.path_file + '/okex_rest' + '/kline'

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
        # 获取当天凌晨零点零分13位时间戳
        today = datetime.date.today()
        ts = int(time.mktime(today.timetuple()) * 1000)
        print(ts)
        # ts = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(today_time/1000))
        # 期货API，连接
        # okcoinFuture = OKCoinFuture(self.okcoinRESTURL, self.apikey, self.secretkey)
        print(u' kline ' + str(self.stime))
        # rs_bch_usdt = okcoinSpot.kline('bch_usdt', ts)
        rs_bch_usdt = okcoinFuture.future_kline(sym,contractType,ts)
        # print(rs_bch_usdt)
        # 原始数据写入txt depth_20180724.txt
        # 判断文件夹是否存在
        self.create_file()
        path = self.path_txt + '//kline_'+ sym +'_'+ contractType +'_' + self.today_date + '.txt'
        with open(path, 'w', encoding='utf-8') as f:
            writer = f.write(json.dumps(rs_bch_usdt) + '\n')
        f.close()
        return rs_bch_usdt

    # 每天创建一个csv
    def create_exccl(self):
        # csv路径
        #file_name = self.path + u'kline_bch_usdt_' + self.today_date + '.csv'
        # 创建csv
        #获取数据
        #卖出价格数据,买入价格数据,
        rs_bch_usdt = self.download(OK.sym,OK.contractType)

        #获取爬取时间
        # t = [str(self.stime)]
        #最近交易开始-------
        # 设置存储路径
        # file_name = self.path + u'kline_bch_usdt_' + self.today_date + '.csv'
        file_name = self.path_txt + '/kline_'+ OK.sym +'_'+ OK.contractType +'_' + self.today_date + '.csv'
        if os.path.exists(file_name):
            with open(file_name, 'w', encoding='utf-8', newline='') as f:
                for i in rs_bch_usdt:
                    # print(i)
                    # 整合数据
                    # heads = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    result = {}
                    result['symbol'] = OK.sym
                    result['contractType'] = OK.contractType
                    result['ts'] = i[0]
                    result['tm_intv'] = '1m'
                    result['id'] = ''
                    result['open'] = i[1]
                    result['close'] = i[4]
                    result['low'] = i[3]
                    result['high'] = i[2]
                    result['amount'] = ''
                    result['vol'] = i[5]
                    result['count'] = ''
                    result['vol_btc_ltc'] = i[6]
                    writer = csv.writer(f)
                    writer.writerow(result.keys())
                    writer.writerow(result.values())
            f.close()
        else:
            with open(file_name, 'w', encoding='utf-8', newline='') as f:
                heads=['symbol','contractType','ts','tm_intv','id','open','close','low','high','amount','vol','count','vol_btc_ltc']
                writer = csv.writer(f)
                writer.writerow(heads)
                for i in rs_bch_usdt:
                    # print(i)
                    # print(i[0])
                    # writer = csv.writer(f)
                    result = {}
                    result['symbol'] = OK.sym
                    result['contractType'] = OK.contractType
                    result['ts'] = i[0]
                    result['tm_intv'] = '1m'
                    result['id'] = i[0]
                    result['open'] = i[1]
                    result['close'] = i[4]
                    result['low'] = i[3]
                    result['high'] = i[2]
                    result['amount'] = ''
                    result['vol'] = i[5]
                    result['count'] = ''
                    result['vol_btc_ltc'] = i[6]
                    # print(result)
                    writer = csv.writer(f)
                    writer.writerow(result.values())
            f.close()
            # 最近交易_bch_usdt_结束-------
    #循环
    def loop(self):
        # rs_bch_usdt = self.create_exccl()
        while True:
            try:
                rs_bch_usdt = self.create_exccl()
                time.sleep(59)
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








