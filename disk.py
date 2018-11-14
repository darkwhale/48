import psutil
import subprocess

cpu_thresold = 70
memory_thresold = 90


# 获取目标文件夹硬盘大小；
# 在生产环境中，共有1,2,3,4,5,6,7,8这些文件夹，每个文件夹对应一个磁盘，需要返回每个文件夹的已使用大小；
def get_disk_size():

    # disk_bytes = subprocess.check_output(['du', '/HDATA', '-d', '1'])
    # disk_str = disk_bytes.decode()
    # disk_dict = {}
    #
    # for sub_str in disk_str.strip().split('\n'):
    #     used_size, disk = sub_str.strip().split('\t')
    #
    #     if disk[-1].isdigit():
    #         disk_dict[int(disk[-1])] = int(used_size)
    #
    # disk_list = sorted(disk_dict.items(),key = lambda d:d[0])
    # return [element[1] for element in disk_list]

    return [121324852, 23, 345107584, 3255576, 4, 4, 4, 373965804]


def get_min_disk():
    disk_list = get_disk_size()
    return disk_list.index(min(disk_list)) + 1


def get_cpu_memory_status():
    cpu_percent = psutil.cpu_percent(None)
    memory_percent = psutil.virtual_memory().percent

    return cpu_percent, memory_percent


# 获取整个电脑的可用空间，以40G为单位；
def get_size_of_computer():
    disk_list = get_disk_size()

    use_able_size = 0

    for disk in disk_list:
        part_useable_size = (961544192 - disk) - (961544192 - disk) \
                            % (40 * 1024 * 1024)
        use_able_size = part_useable_size if use_able_size < part_useable_size else use_able_size

    return use_able_size


# 返回系统是否可用以及cpu占用率；
def get_status(src_size):
    cpu_percent, memory_percent = get_cpu_memory_status()

    if cpu_percent > cpu_thresold or memory_percent > memory_thresold:
        return "0"

    if get_size_of_computer() > src_size / 1024:
        return "1" + str(cpu_percent)

    else:
        return "0"

