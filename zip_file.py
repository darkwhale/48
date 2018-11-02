import zipfile
import os


def unzip_file(zip_file_path):
    base_dir = os.path.dirname(zip_file_path)

    # 解压文件的目录；
    unzip_dir = os.path.join(base_dir, 'unzip')
    if not os.path.exists(unzip_dir):
        os.mkdir(unzip_dir)
    try:
        zipper = zipfile.ZipFile(zip_file_path, 'r')
        zipper.extractall(unzip_dir)
    except FileExistsError:
        print("解压文件已存在，将直接使用该文件")

    return unzip_dir
