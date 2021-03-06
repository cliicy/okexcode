#  -*- coding:utf-8 -*-
import pika
import sys


class MqSender:
    """
    mq消息生产者
    """
    # rabbitmq_host = "10.0.131.74"
    # rabbitmq_username = "wuxiaobing"
    # rabbitmq_pwd = "wuxiaobing"
    # queue_name = ''
    rabbitmq_host = "10.0.131.79"
    rabbitmq_username = "wuxiaobing"
    rabbitmq_pwd = "wuxiaobing"
    queue_name = ''

    def __init__(self, platform, data_type):
        username = self.rabbitmq_username  # 指定远程rabbitmq的用户名密码
        pwd = self.rabbitmq_pwd
        user_pwd = pika.PlainCredentials(username, pwd)
        self.s_conn = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host, credentials=user_pwd))  # 创建连接
        print("isopen", self.s_conn.is_open)
        self.chan = self.s_conn.channel()  # 在连接上创建一个频道
        self.queue_name = '%s_%s' % (platform, data_type)
        self.chan.queue_declare(queue=self.queue_name)  # 声明一个队列，生产者和消费者都要声明一个相同的队列，用来防止万一某一方挂了，另一方能正常运行

    def send(self, msg):
        # if msg is None:
        #     msg = ""
        if self.s_conn.is_closed:
            self.conn_()

        self.chan.basic_publish(exchange='',  # 交换机
                           routing_key=self.queue_name,  # 路由键，写明将消息发往哪个队列，本例是将消息发往队列hello
                           body=msg)  # 生产者要发送的消息
        print("send ", msg)



    def conn_(self):
        username = self.rabbitmq_username  # 指定远程rabbitmq的用户名密码
        pwd = self.rabbitmq_pwd
        user_pwd = pika.PlainCredentials(username, pwd)
        self.s_conn = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host, credentials=user_pwd))  # 创建连接
        self.chan = self.s_conn.channel()

    def close(self):
        self.s_conn.close()  # 当生产者发送完消息后，可选择关闭连接
if __name__ == '__main__':
    sender = MqSender("huobi", "kline")
    sender.send("aaa")
    sender.send("bbb")
    sender.send("aaccca")
    sender.close()
