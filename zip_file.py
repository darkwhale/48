import zipfile
import os
import shutil


def get_date(basename):
    return basename[:6]


def get_database(basename):
    return ''.join([z for z in basename[:-4] if z.isalpha()])


def unzip_file(zip_file_path):
    base_dir = os.path.dirname(zip_file_path)

    # 解压文件的目录；
    unzip_dir = os.path.join(base_dir, 'unzip')
    if os.path.exists(unzip_dir):
        shutil.rmtree(unzip_dir)
    os.mkdir(unzip_dir)
    try:
        zipper = zipfile.ZipFile(zip_file_path, 'r')
        zipper.extractall(unzip_dir)
    except FileExistsError:
        print("解压文件已存在，将重新解压")

        # 删掉旧文件,重新解压；
        # 经实验，zip_file可自动覆盖文件，因此也可不要该异常检测机制；
        shutil.rmtree(unzip_dir)
        os.mkdir(unzip_dir)
        zipper = zipfile.ZipFile(zip_file_path, 'r')
        zipper.extractall(unzip_dir)

    return unzip_dir
