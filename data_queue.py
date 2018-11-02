import os
import time
import zip_file
import shutil
from logs import make_log

# # 开启额外的线程用于监控是否有需要入库的文件，防止当服务器中断时还有文件未入库；
# # 使用记录文件来记录需要入库的文件；
# todo 生产时替换；
# mask_file = "/HDATA`/1/mask/mask"
mask_file = '/home/zxy/PycharmProjects/mask/mask'

if not os.path.exists(os.path.dirname(mask_file)):
    os.makedirs(os.path.dirname(mask_file))
if not os.path.exists(mask_file):
    os.mknod(mask_file)


# 监控数据并调用清洗接口；
def monitor_data():
    while True:
        time.sleep(30)

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

                # 解析数据库名字;
                basename = os.path.basename(first_mask)
                database_name = ''.join([z for z in basename if z.isalpha()])

                # 解压缩数据，并返回压缩后的文件夹；
                unzip_dir = zip_file.unzip_file(first_mask)

                # todo 清洗，入库;
                time.sleep(100)

                # make_log("INFO", "clean finished：" + first_mask)
                # 清除第一条数据；
                with open(mask_file, 'w') as write_mask:
                    write_mask.write(''.join(mask_str[1:]))

                # 删除所有的文件，包括压缩文件，解压文件以及清洗后的文件；
                shutil.rmtree(os.path.dirname(first_mask))

                print("清洗完毕：" + first_mask)

                make_log("INFO", "清洗完毕：" + first_mask)

            except FileNotFoundError:
                print("file not found")

                make_log("ERROR", "file not found" + first_mask)
                with open(mask_file, 'w') as write_mask:
                    write_mask.write(''.join(mask_str[1:]))
        else:
            continue




