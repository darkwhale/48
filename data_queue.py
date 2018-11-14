import os
import time
import zip_file
import shutil
from logs import make_log
from zip_file import get_database
from zip_file import get_date
import write_protect
import threading
import subprocess
from dataclean.clean import process_dir
from hive import hive

# 开启额外的线程用于监控是否有需要入库的文件，防止当服务器中断时还有文件未入库；
# 使用记录文件来记录需要入库的文件；
mask_file = 'mask/mask'

if not os.path.exists(os.path.dirname(mask_file)):
    os.makedirs(os.path.dirname(mask_file))
if not os.path.exists(mask_file):
    os.mknod(mask_file)


# 计算该类的线程数；
def thread_nums(name="monitor"):
    thread_list = [thread for thread in threading.enumerate()
                   if thread.getName().startswith(name)]

    return len(thread_list)


# 定义锁字典；
lock_dict = {}


# 清洗入库单个文件；
def monitor_data(file, in_monitor):
    try:
        print("数据清洗：", file)
        make_log("INFO", "数据清洗：" + file)

        # todo 解析数据库名字和日期;利用这些信息清洗和入库；
        basename = os.path.basename(file)

        database_name = get_database(basename)
        month = get_date(basename)

        # 解压缩数据，并返回压缩后的文件夹；
        unzip_dir = zip_file.unzip_file(file)

        # todo 清洗，入库;待调试；
        # merge_dir = process_dir(unzip_dir, date)

        # hive(merge_dir, database_name, date)
        time.sleep(15)

        # make_log("INFO", "清洗完成：" + file)

        write_protect.write_lock.acquire()
        with open(mask_file, 'r') as read_mask:
            # 重新读取文件；因为在别的线程可能会修改文件；
            mask_str = read_mask.readlines()

        mask_str = [mask for mask in mask_str if mask.strip() != file]

        with open(mask_file, 'w') as write_mask:
            write_mask.write(''.join(mask_str))
        write_protect.write_lock.release()

        # 删除所有的文件，包括压缩文件，解压文件以及清洗后的文件；
        shutil.rmtree(os.path.dirname(file))

        print("清洗完毕：" + file)

        make_log("INFO", "清洗完毕：" + file)

    except subprocess.CalledProcessError:
        print("数据入库未完成：", file)
        make_log("ERROR", "数据入库未完成：" + file)

    except FileNotFoundError:
        print("file not found")

        make_log("ERROR", "文件不存在" + file)

        # 删除该文件的记录；
        write_protect.write_lock.acquire()
        with open(mask_file, 'r') as read_mask:
            # 重新读取文件；因为在别的线程可能会修改文件；
            mask_str = read_mask.readlines()

        mask_str.remove(file)

        with open(mask_file, 'w') as write_mask:
            write_mask.write(''.join(mask_str))
        write_protect.write_lock.release()
    finally:
        in_monitor.remove(file + '\n')


# 监控数据并调用清洗接口；
def monitor(thread_num=5):

    # 定义正在处理中的文件；
    in_monitor = []

    while True:
        time.sleep(3)

        if not os.path.exists(mask_file):
            make_log("ERROR", "mask文件不存在")
            print("mask文件不存在！")
            exit(1)

        # 读取mask文件，如果为空，则继续监控，否则处理文件；
        # mask_str代表了所有需要处理的文件；
        with open(mask_file, 'r') as read_mask:
            mask_str = read_mask.readlines()

        # new_monitor代表了所有未进行处理的文件；
        new_monitor = [file for file in mask_str if file not in in_monitor]

        # 计算最大可新建的线程数；
        free_thread_nums = thread_num - thread_nums("monitor")

        max_thread_num = free_thread_nums if free_thread_nums \
                                             < len(new_monitor) else len(new_monitor)

        # 开启max_thread_num个线程用于处理数据；
        for i in range(max_thread_num):
            compress_thread = threading.Thread(target=monitor_data,
                                               args=(new_monitor[i].strip(), in_monitor),
                                               name="monitor")

            # 将该文件夹放入in_compress列表中；
            in_monitor.append(new_monitor[i])

            compress_thread.start()



