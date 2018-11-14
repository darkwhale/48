# -*- coding: UTF-8 -*-
import threading
from data_queue import monitor
from answer import answer
from m_socket import receive
from logs import make_log


"""
answer_thread 用于接收客户端请求并返回系统的状态，帮助客户机决定要向哪台主机发送数据；占用端口号为12346
receive_thread 用于接收客户端传输的文件，并向mask_file中写入记录；
mask_thread 用于监控mask_file文件，并进行数据清洗和数据处理；
"""


if __name__ == '__main__':
    # 开启线程用于返回系统状态；
    answer_thread = threading.Thread(target=answer, args=(), name='answer')
    answer_thread.start()
    make_log("INFO", "监听程序已开启-------------")
    print("监听程序已开启-------------")

    # 开启线程用于接收文件;
    receive_thread = threading.Thread(target=receive, args=(), name='receive')
    receive_thread.start()
    make_log("INFO", "文件接受程序已开启--------------")
    print("文件接受程序已开启--------------")

    # 开启线程用于监控mask文件;
    mask_thread = threading.Thread(target=monitor, args=(5,), name='mask')
    mask_thread.start()
    make_log("INFO", "文件处理程序已开启--------------")
    print("文件处理程序已开启--------------")



