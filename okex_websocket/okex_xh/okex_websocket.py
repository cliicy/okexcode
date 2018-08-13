# -*- coding: utf-8 -*-
import okex_xh.websocket as websocket
import time
import sys
import json
import os
import csv
#创建文件夹
def create_file(self):
    if not os.path.exists(self.path_txt):
        os.makedirs(self.path_txt)

def on_open(self):
    #subscribe okcoin.com spot ticker
    self.send("[{'event':'addChannel','channel':'ok_sub_spot_btc_usdt_ticker'},"
              "{'event':'addChannel','channel':'ok_sub_spot_bch_usdt_ticker'}]")

# 获得返回值并写入文件
def download(self,evt,sym):
    # 设置文件存放路径
    path = 'D:\\data\\'
    today_date = time.strftime("%Y%m%d")
    path_txt = path + today_date + '\\okex'
    # 判断文件是否存在
    if not os.path.exists(path_txt):
        print('创建文件夹')
        os.makedirs(path_txt)
    else:
        # 判断是否为需要数据
        if evt['channel'] is 'addChannel':
            pass
        else:
            # 将原始文件写入txt
            ok_path = path_txt + '\\ticker_' + today_date + '.txt'
            with open(ok_path, 'a+', encoding='utf-8') as f:
                writer = f.write(json.dumps(evt) + '\n')
            f.close()
            # 调整数据格式存入csv
            # 头部
            if evt['channel'] == 'addChannel':
                pass
            else:
                headers = {'symbol','ts','latest_price','latest_amount','max_buy1_price','max_buy1_amt','min_sell1_price','min_sell1_amt','pre_24h_price','pre_24h_price_max','pre_24h_price_min','pre_24h_bt_finish_amt','pre_24h_usd_finish_amt'}
                # 内容
                # 解析获得文件的格式
                result = {}
                result['symbol'] = ''.join(evt['channel'].split('_')[3:5])
                result['ts'] = evt['data']['timestamp']
                result['latest_price'] = evt['data']['last']
                result['latest_amount'] = ''
                result['max_buy1_price'] = evt['data']['buy']
                result['max_buy1_amt'] = ''
                result['min_sell1_price'] = evt['data']['sell']
                result['min_sell1_amt'] = ''
                result['pre_24h_price'] = ''
                result['pre_24h_price_max'] = evt['data']['high']
                result['pre_24h_price_min'] = evt['data']['low']
                result['pre_24h_bt_finish_amt'] = evt['data']['vol']
                result['pre_24h_usd_finish_amt'] = ''
                # 设置存储路径
                file_name = path_txt + '\\ticker_' + today_date + '.csv'
                # 判断文件是否存在，如果存在则直接写入数据，如果不存在则创建文件
                if os.path.exists(file_name):
                    with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(result.values())
                    f.close()
                else:
                    with open(file_name, 'a+', encoding='utf-8', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(result.keys())
                        writer.writerow(result.values())
                    f.close()
                    # 现货深度卖出结束-------
def on_message(self,evt):
    # 将数据格式调整为字典格式
    # 用json转换成列表格式
    data = json.loads(evt)
    rdata = data[0]
    download(self,rdata,sym)

def on_error(self,evt):
    print('7777777777777777777')
    print (evt)

def on_close(self):
    print ('DISCONNECT')

if __name__ == "__main__":
    url = "wss://real.okex.com:10441/websocket"      #if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    api_key = '0a5ba5fe-2ce4-4f2a-b308-1f6f17d3e6ec'
    secret_key = '82B69BD2B7DBBABF726D046C37C7969F'
    host = url
    sym = 'btc_usdt'
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
    # while True:
    ws.run_forever()
