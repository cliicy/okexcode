# import time
# print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(1533818160000/1000)))
import time
#
# now_time = int(time.time()*1000)
# day_time = now_time - now_time % 86400 + time.timezone
# # day_time_str = time.asctime(time.localtime(day_time))
# print(now_time)
# print(day_time)
# print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(day_time/1000)))
# # print(day_time_str)
import datetime
today = datetime.date.today()
print(today)
print(time.mktime(today.timetuple()))
today_time = int(time.mktime(today.timetuple())*1000)
print(today_time)
# 1533819840000
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(1533906180000/1000)))
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(1533819840000/1000)))
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(1533830400000/1000)))