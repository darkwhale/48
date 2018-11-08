import os
import time
import zip_file
import shutil
from logs import make_log
from zip_file import get_database
from zip_file import get_date
import write_protect
from dataclean.clean import process_dir
from hive import hive

# # 开启额外的线程用于监控是否有需要入库的文件，防止当服务器中断时还有文件未入库；
# # 使用记录文件来记录需要入库的文件；
mask_file = 'mask/mask'

if not os.path.exists(os.path.dirname(mask_file)):
    os.makedirs(os.path.dirname(mask_file))
if not os.path.exists(mask_file):
    os.mknod(mask_file)


# 监控数据并调用清洗接口；
def monitor_data():
    while True:
        time.sleep(3)

        if not os.path.exists(mask_file):
            make_log("ERROR", "mask文件不存在")
            print("mask文件不存在！")
            exit(1)

        # 读取mask文件，如果为空，则继续监控，否则处理文件；
        with open(mask_file, 'r') as read_mask:
            mask_str = read_mask.readlines()

        # 文件不为空，则进行数据解压，清洗，入库，最后删除文件；
        if mask_str:
            # 取第一条数据进行操作；
            # first_mask为需要操作的压缩文件；
            first_mask = mask_str[0].strip()
            try:

                print("数据清洗：", first_mask)
                make_log("INFO", "数据清洗：" + first_mask)

                # todo 解析数据库名字和日期;利用这些信息清洗和入库；
                basename = os.path.basename(first_mask)

                database_name = get_database(basename)
                date = get_date(basename)

                # 解压缩数据，并返回压缩后的文件夹；
                unzip_dir = zip_file.unzip_file(first_mask)

                # todo 清洗，入库;待调试；
                # merge_dir = process_dir(unzip_dir, date)
                # hive(merge_dir, database_name, date)
                time.sleep(15)

                # make_log("INFO", "clean finished：" + first_mask)
                # 清除第一条数据；

                # 注意这里的写保护；
                write_protect.write_protect.acquire()
                with open(mask_file, 'r') as read_mask:

                    # 重新读取文件并删除第一行；因为在别的线程可能会修改文件；
                    mask_str = read_mask.readlines()

                with open(mask_file, 'w') as write_mask:

                    write_mask.write(''.join(mask_str[1:]))
                write_protect.write_protect.release()

                # 删除所有的文件，包括压缩文件，解压文件以及清洗后的文件；
                shutil.rmtree(os.path.dirname(first_mask))

                print("清洗完毕：" + first_mask)

                make_log("INFO", "清洗完毕：" + first_mask)

            except FileNotFoundError:
                print("file not found")

                make_log("ERROR", "文件不存在" + first_mask)
                with open(mask_file, 'w') as write_mask:
                    write_mask.write(''.join(mask_str[1:]))
        else:
            continue




