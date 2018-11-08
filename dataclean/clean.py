from dataclean.utils import clean_folder
from dataclean.utils import merge_files
import sys


# 处理文件；目录,数据库名,日期
def process_dir(folder, database_name, date):

    print('hello')
    # 清洗文件夹，获取清洗后的目录名；
    new_data_dir = clean_folder(folder, date)

    # 合并小文件；
    merge_data_dir = merge_files(new_data_dir, database_name, date)

    return merge_data_dir


if __name__ == '__main__':
    folder = sys.argv[1]
    database_name = sys.argv[2]
    date = sys.argv[3]
    # 清洗文件夹，获取清洗后的目录名；
    new_data_dir = clean_folder(folder, date)

    # 合并小文件；
    merge_files(new_data_dir, database_name, date)
