#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
# 最近交易
from python.OkcoinSpotAPI import OKCoinSpot
import time
import os
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
        self.path = '/data/'
        # 时间
        self.today_date = time.strftime("%Y%m%d")
        self.stime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        self.ts = int(time.time())  # 十位时间戳
        # 文件夹第一层 /data/20180723
        self.path_file = self.path + self.today_date
        # 文件夹第二层 /data/20180723/okex
        self.path_txt = self.path_file + '/okex_rest'
    # 创建文件夹
    def create_file(self):
        if not os.path.exists(self.path_txt):
            os.makedirs(self.path_txt)
    # 连接网址获取数据
    def download(self,sym):
        # 现货API，连接
        okcoinSpot = OKCoinSpot(self.okcoinRESTURL, self.apikey, self.secretkey)
        # 期货API，连接
        # okcoinFuture = OKCoinFuture(self.okcoinRESTURL, self.apikey, self.secretkey)
        print(u' 最近交易 '+str(self.stime))
        rs_bch_usdt = okcoinSpot.trades(sym)
        # 原始数据写入txt depth_20180724.txt
        # 判断文件夹是否存在
        self.create_file()
        path = self.path_txt + '/trades_'+ sym +'_' + self.today_date + '.txt'
        with open(path, 'a+', encoding='utf-8') as f:
            # print(path)
            writer = f.write(json.dumps(rs_bch_usdt) + '\n')
        f.close()
        # print(rs_bch_usdt)
        return rs_bch_usdt

    # 每天创建一个csv
    def create_exccl(self):
        print(OK.sym)
        rs_bch_usdt = self.download(OK.sym)
        # 设置存储路径
        file_name = self.path_txt + '/trades_'+ OK.sym +'_' + self.today_date + '.csv'
        #设置存放格式
        result = {}
        if os.path.exists(file_name):
            with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                for i in rs_bch_usdt:
                    # print(i)
                    result['symbol'] = OK.sym
                    result['id'] = i['tid']
                    result['ts'] = i['date_ms']
                    result['direction'] = i['type']
                    result['amount'] = i['amount']
                    result['price'] = i['price']
                    writer = csv.writer(f)
                    writer.writerow(result.values())
            f.close()
        else:
            with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                headers = ['symbol','id','ts','direction','amount','price']
                writer.writerow(headers)
                for i in rs_bch_usdt:
                    # print(i)
                    result['symbol'] = OK.sym
                    result['id'] = i['tid']
                    result['ts'] = i['date_ms']
                    result['direction'] = i['type']
                    result['amount'] = i['amount']
                    result['price'] = i['price']
                    writer = csv.writer(f)
                    writer.writerow(result.values())
            f.close()
            # 最近交易_bch_usdt_结束-------
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
    thread = Thread(target=OK.loop)
    thread.start()
    thread.join()
    print('done')








