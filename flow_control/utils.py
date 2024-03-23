import os
from multiprocessing import Process
from multiprocessing import shared_memory

from config import CPU_COUNT
from utilities import logger
from utilities.utilities import get_log_file_handler


# 在这个里面搞多进程
def restore_intersection_result(hash_dir, intersection_dir, output_dir):
    if os.path.exists(output_dir) is False:
        os.makedirs(output_dir)
    # 我们假定2个 DIR 都是遵守 CPU_COUNT 数量规格的
    for i in range(CPU_COUNT):
        process_cnt = i + 1 
        hash_path = os.path.join(hash_dir, "hash_%d.csv"%( process_cnt))
        intersection_path = os.path.join(intersection_dir, "intersection_%d.csv"%( process_cnt))
        output_path = os.path.join(output_dir, "result_%d.csv"%( process_cnt))
        restore_intersection_result_single(hash_path, intersection_path, output_path)
        pass
    pass 

# 再扩展多进程，看行不行
def restore_intersection_result_mp(hash_dir, intersection_dir, output_dir):
    if os.path.exists(output_dir) is False:
        os.makedirs(output_dir)
    # 我们假定2个 DIR 都是遵守 CPU_COUNT 数量规格的
    process_list = []
    for i in range(CPU_COUNT):
        process_cnt = i + 1 
        hash_path = os.path.join(hash_dir, "data_%d.csv"%( process_cnt))
        intersection_path = os.path.join(intersection_dir, "intersection_%d.csv"%( process_cnt))
        output_path = os.path.join(output_dir, "result_%d.csv"%( process_cnt))
        # 这里开展多进程
        # restore_intersection_result_single(hash_path, intersection_path, output_path)
        p = Process(target= restore_intersection_result_single,\
            args= (hash_path, intersection_path, output_path))
        p.start()
        process_list.append(p)
        pass
    for p in process_list:
        p.join()
    pass 

def restore_intersection_result_single(hash_path, intersection_path, output_path):
    # 1. build dict 
    f_hash = open(hash_path, "r")
    hash_lines = f_hash.readlines()
    f_hash.close()
    hash_dict = {}
    for each_line in hash_lines:
        hash_value = each_line.split(',')[0].strip()
        id_value   = each_line.split(',')[1].strip()
        hash_dict[hash_value] = id_value
    # 2. restore result
    f_inter = open(intersection_path, "r")
    intersection_lines = f_inter.readlines()
    f_inter.close()
    # 3. calculate result 
    res_list = []
    for each_res in intersection_lines:
        id_value = hash_dict[each_res.strip()]
        res_list.append(id_value + '\n')
    # 4. output csv 
    # 输出之后
    f = open(output_path, 'w')
    f.writelines(sorted(res_list))
    f.close()

def get_intersection_single_process_shm(st_index, ed_index, process_name = None, q = None):
    print("Start process: %s for hash calculation in %s." %(process_name, os.getpid()))
    long_list = shared_memory.ShareableList( name = "long")
    short_list = shared_memory.ShareableList( name = "short")
    print('0')
    # 应该传的是 sorted 的 list
    nums1 = long_list[st_index : ed_index]
    nums2 = short_list
    result = []
    i, j = 0, 0
    print("1")
    while i<len(nums1) and j<len(nums2):
        if nums1[i]==nums2[j]:
            result.append(nums1[i])
            i += 1
            j += 1
        elif nums1[i]>nums2[j]:
            j += 1
        elif nums1[i]<nums2[j]:
            i += 1
    print("2")
    q.put({"res": result})
    print("%s Done" % process_name)
    return 

def get_intersection_single_process_queue(long_list, short_list, process_name = None, q = None):
    print("Start process: %s for hash calculation in %s." %(process_name, os.getpid()))
    assert type(long_list) is list and type(short_list) is list
    assert len(long_list) > 0 and len(short_list) > 0
    # 应该传的是 sorted 的 list
    nums1 = long_list
    nums2 = short_list
    result = []
    i, j = 0, 0
    while i<len(nums1) and j<len(nums2):
        if nums1[i]==nums2[j]:
            result.append(nums1[i])
            i += 1
            j += 1
        elif nums1[i]>nums2[j]:
            j += 1
        elif nums1[i]<nums2[j]:
            i += 1
    q.put({"res": result})
    print("%s Done" % process_name)
    return 

def calculate_intersection_f2f(input_path, input_prefix, process_cnt, logger_path = None):
    # 参数详解
    # input_path： 输入 list 的路径，也就是 intermediate result 的路径
    # input_prefix 和 process_cnt 组成了 本地 hash 的 path，形成 short_list
    # logger_path 是日志的输出 path 
    if logger_path is not None: 
        the_handler = get_log_file_handler(logger_path)
        logger.addHandler(the_handler)
    logger.info("Intersection calculation subprocess %d reporting in." % process_cnt)
    # 文件打不开直接返回即可
    # 先读 short_list ，否则 long_list 读很费时间和 IO
    # 这个是本地的hash
    try:
        current_f_path = input_prefix + "%d.csv" % process_cnt
        logger.info("Subprocess %d input_hash_file: " % process_cnt + current_f_path)
        current_f = open(current_f_path, 'r')
    except FileNotFoundError:
        logger.warning("User list not found, exit subprocess %d" % process_cnt)
        return
    short_list = current_f.readlines()
    if len(short_list) == 0:
        logger.warning("User list %d is empty, exit subprocess%d" % (process_cnt, process_cnt))
        return
    logger.info("Subprocess %d local list length: " % process_cnt+ str(len(short_list)))
    current_f.close()
    # 这个是输入的 list 
    try:
        input_f = open(input_path, 'r')
    except FileNotFoundError:
        logger.warning("input_f not found, exit subprocess %d" % process_cnt)
        return

    long_list = input_f.readlines()
    logger.info("Subprocess %d party list length: "% process_cnt +  str(len(long_list)))
    input_f.close()
    # test area 
    input_long_list = []
    for i in long_list:
        input_long_list.append(i.strip())
    # short list 由于是读的 hash，需要
    input_short_list = []
    for i in short_list:
        input_short_list.append(i.split(',')[0].strip())
    # 排序
    logger.info("Subprocess %d Start Sort ID lists."% process_cnt)
    input_short_list = sorted(input_short_list)
    input_long_list = sorted(input_long_list)
    logger.info("Subprocess %d ID lists sort finished."% process_cnt)
    # 现在2个都读取完毕了，
    res_list = get_intersection_simple(input_long_list, input_short_list)
    current_f_path = current_f_path.replace("hash_dicts/", "intersection/")
    current_f_path = current_f_path.replace("hash_", "intersection_")
    logger.info("Subprocess %d Output_hash_file: " % process_cnt  + str(current_f_path))
    logger.info("Subprocess %d Output hash length: "% process_cnt + str(len(res_list)))
    current_f_path = current_f_path.replace("intersection_dicts", "intersection")
    dir_path_list = current_f_path.split('\\')[:-1]
    dir_path = '/'.join(dir_path_list)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    current_f = open(current_f_path, 'w') # 除了第一个 party 是新创建之外，后面的都是覆盖
    current_f.writelines(res_list)
    current_f.close()
    logger.info("Subprocess %d finished." % process_cnt)
    return

# 这个是供每个函数调用的方法，写的尽量简单即可
# 注意！这里传入的2个list 都是排序好的
def get_intersection_simple(long_list, short_list):
    assert type(long_list) is list and type(short_list) is list
    if len(long_list) == 0 or len(short_list) == 0:
        print("empty list detected")
        return []
    # 应该传的是 sorted 的 list
    nums1 = long_list
    nums2 = short_list
    result = []
    i, j = 0, 0
    while i<len(nums1) and j<len(nums2):
        if nums1[i]==nums2[j]:
            result.append(str(nums2[j])+"\n")
            i += 1
            j += 1
        elif nums1[i]>nums2[j]:
            j += 1
        elif nums1[i]<nums2[j]:
            i += 1
    return result