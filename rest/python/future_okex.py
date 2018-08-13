# !-*-coding:utf-8 -*-
# @TIME    : 2018/6/11/0011 15:32
# @Author  : Nogo
import python.config.symbol as symbol
import subprocess
from multiprocessing import Process
from threading import Thread

#开进程调用ticker
def do_ticker(sym,contractType):
    cmd = '{0}{1}{2}{3}'.format('python /root/yanyan/rest/python/future_ticker.py ', sym,' ',contractType)
    print(cmd)
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
    print(pipe.read())
    print('finished to getting trades information')
#开进程调用ticker
def do_kline(sym, contractType):
    cmd = '{0}{1}{2}{3}'.format('python /root/yanyan/rest/python/future_kline.py ', sym,' ',contractType)
    print(cmd)
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
    print(pipe.read())
    print('finished to getting trades information')
#开进程调用depth
def do_depth(sym, contractType):
    cmd = '{0}{1}{2}{3}'.format('python /root/yanyan/rest/python/future_depth.py ', sym,' ',contractType)
    print(cmd)
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
    print(pipe.read())
    print('finished to getting trades information')
#开进程调用trades
def do_trades(sym, contractType):
    cmd = '{0}{1}{2}{3}'.format('python /root/yanyan/rest/python/future_trades.py ', sym,' ',contractType)
    print(cmd)
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
    print(pipe.read())
    print('finished to getting trades information')
class MarketApp(object):
    def __init__(self):
        self.c = []

    # 循环ticker
    def loop_ticker(self):
        # 循环调取depth
        for sy in symbol.f_symbol:
            for contractType in symbol.contractType:
            # self.do_trades(sy)
                print(sy,contractType)
                p = Process(target=do_ticker, args=(sy,contractType,))
                print('syncing trades information will start.')
                p.start()

    # 循环kline
    def loop_kline(self):
        # 循环调取depth
        for sy in symbol.f_symbol:
            for contractType in symbol.contractType:
                # self.do_trades(sy)
                print(sy, contractType)
                p = Process(target=do_kline, args=(sy, contractType,))
                print('syncing trades information will start.')
                p.start()
    # 循环depth
    def loop_depth(self):
        # 循环调取depth
        for sy in symbol.f_symbol:
            for contractType in symbol.contractType:
                # self.do_trades(sy)
                print(sy, contractType)
                p = Process(target=do_depth, args=(sy, contractType,))
                print('syncing trades information will start.')
                p.start()
    # 循环trades
    def loop_trades(self):
        # 循环调取depth
        for sy in symbol.f_symbol:
            for contractType in symbol.contractType:
                # self.do_trades(sy)
                print(sy, contractType)
                p = Process(target=do_trades, args=(sy, contractType,))
                print('syncing trades information will start.')
                p.start()

if __name__ == '__main__':
    run = MarketApp()
    ticker = Thread(target=run.loop_ticker)
    kline = Thread(target=run.loop_kline)
    trades = Thread(target=run.loop_trades)

    depth = Thread(target=run.loop_depth)
    ticker.start()
    trades.start()
    kline.start()
    depth.start()

    trades.join()
    depth.join()
    ticker.join()
    kline.join()
    print('done')
