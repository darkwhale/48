# -*- coding: UTF-8 -*-
# 系统的logs模块灵活性太差，因此自定义一个logs模块；
# 根据日期生成日志文件；
import time
import os

log_dir = "logs"

if not os.path.exists(log_dir):
    os.makedirs(log_dir)


# 定位目标日志文件，如果不存在则创建该文件；
def locate_log():
    date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    return os.path.join(log_dir, date)


# 记录日志文件；
def make_log(type, log_str):
    if type not in ["INFO", "WARNING", "ERROR"]:
        raise ValueError

    write_log = time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime(time.time())) + \
        "   " + type.ljust(7, ' ') + "  " + log_str + '\n'
    with open(locate_log(), 'a') as writer:
        writer.write(write_log)



