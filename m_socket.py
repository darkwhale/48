# -*- coding: UTF-8 -*-
# 多线程接收数据；
import socket
import struct
import threading
from disk import get_min_disk
from data_queue import mask_file
import os
import shutil
from logs import make_log
from zip_file import get_database

# 接收文件接口；
port = 12345


# 多线程，传输完毕可直接进行清洗；
def receive_thread(connection):

    try:
        connection.settimeout(600)

        file_info_size = struct.calcsize('128sl')

        buf = connection.recv(file_info_size)

        if buf:
            file_name, file_size = struct.unpack('128sl', buf)

            file_name = file_name.decode().strip('\00')

            # 查找最小的目录用于存储文件；
            disk_list = get_min_disk()

            # todo 生产时替换；
            # 在receive下用时间戳创建新的文件夹，防止命名冲突；
            # file_new_dir = os.path.join('/HDATA', str(disk_list),
            #                             'receive', str(int(time.time())))
            file_new_dir = os.path.join('receive', get_database(file_name),
                                        file_name[:-4])
            if not os.path.exists(file_new_dir):
                os.makedirs(file_new_dir)

            file_new_name = os.path.join(file_new_dir, file_name)

            received_size = 0

            w_file = open(file_new_name, 'wb')

            print("start receiving file:", file_name)

            out_contact_times = 0

            while not received_size == file_size:
                # process_bar.process_bar(float(received_size) / file_size)
                if file_size - received_size > 10240:
                    r_data = connection.recv(10240)
                    received_size += len(r_data)

                else:
                    r_data = connection.recv(file_size - received_size)
                    received_size = file_size

                # 记录未接收到数据的次数；
                if not r_data:
                    out_contact_times += 1
                else:
                    out_contact_times = 0

                # 1000次未接收到数据，可断开；
                if out_contact_times == 1000:
                    connection.close()
                    w_file.close()

                    # 删除掉未接收完毕的数据；
                    print('连接断开，将清除未传输的文件')
                    make_log("ERROR", "连接断开,清除未完成的文件")
                    shutil.rmtree(file_new_dir)
                    exit(1)

                w_file.write(r_data)

            w_file.close()

            print("\n接收完成！\n")
            make_log("INFO", "传输完成： %s" % file_new_dir)

            # 写到记录文件里；
            # 每个文件记录为一行，第一个代表文件名，第二个代表数据库名；
            # 好处：
            # 1.不需要对入库过程上锁，以免造成同时写入库文件的错误；
            # 2.当系统重启时可以继续执行文件清洗和入库过程；
            # print('##'+os.path.abspath(file_new_name))
            with open(mask_file, 'a') as record_mask:
                print("#######################")
                record_mask.write(os.path.abspath(file_new_name) + '\n')

        connection.close()

    except socket.timeout:
        print("连接超时！")
        connection.close()


def receive():
    host = socket.gethostname()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(5)

    print("服务已启动---------------")

    while True:
        connection, address = sock.accept()
        print("接收地址：", address)
        thread = threading.Thread(target=receive_thread, args=(connection, ))
        thread.start()

