
import os
import pickle
import random
import time
from multiprocessing import Process, Queue

from config import mpc_data_dir, mpc_job_dir, CPU_COUNT, SALT
from utilities.utilities import string_to_md5


def long_time_calculate():
    print("Start %s at %s." % (time.ctime(), os.getpid()))
    time.sleep(3)
    print("End %s at %s." %  (time.ctime(), os.getpid()))

# 很单纯，输入一个列表以及 logger
def calculate_hash_queue(input_id_list, logger, process_name, salt, q):
    hash_dict = {}
    logger.info("Start process: %s for hash calculation in %s." %(process_name, os.getpid()))
    for each_id in input_id_list:
        # 这里需要去掉空格或者换行符
        each_hash = string_to_md5(each_id.strip() + salt)
        hash_dict[each_hash] = each_id.strip()
    logger.info("Calculation count of process %s is %d" %(process_name, len(input_id_list)))
    q.put(hash_dict)
    logger.info("Process: %s for hash calculation Finished." % process_name)

def list_to_hash_dict(input_list):
    res_dict = {}
    for i in list('0123456789abcdef'):
        res_dict[i] = []
    for each_id in list(set(input_list)):
        each_hash = string_to_md5(each_id.strip() + SALT)
        each_hash = each_hash.lower()
        each_str = each_hash + "," + each_id + '\n'
        res_dict[each_hash[0]].append(each_str)
    return res_dict

def to_hash_list(input_list):
    res_list = []
    for each_id in list(set(input_list)):
        each_hash = string_to_md5(each_id.strip() + SALT)
        each_hash = each_hash.lower()
        each_str = each_hash + "," + each_id # + '\n'
        res_list.append(each_str)
    return res_list

def write_hash_dict(hash_dict, output_dir):
    # 处理目录问题
    if not os.path.exists(output_dir):
        os.makedirs(output_dir) 
    # 然后创建16个f ，进行写入
    for i in list('0123456789abcdef'):
        file_path = os.path.join(output_dir, "hash_" + i + '.csv')
        f = open(file_path, 'a')
        hash_list = hash_dict[i]
        # 不知道多个进程同时追加写同一个文件，会不会有冲突？
        f.writelines(hash_list)
        f.close()
        time.sleep(0.1 * random.random() + 0.005)
    # 结束
    pass

# 使用 file to file 的方式输出 
# 不在这个函数中搞多进程 
def calculate_hash_f2f(data_dir, output_dir, logger, process_cnt):
    index = process_cnt
    process_name = "MPC_Hash_Process-" + str(process_cnt + 1)
    logger.info("Start process: %s for hash calculation in %s." %(process_name, os.getpid()))
    # 计算 hash ， 形成结果
    file_path = os.path.join(data_dir, "data_split_" + str(index+1)+".csv" )
    # 如果文件不存在，那么就不做后面的计算了
    if not os.path.exists(file_path):
        logger.warning("File split not exist: %s." % file_path)
        return 
    f = open(file_path, "r")
    data_list = f.readlines()
    f.close()
    output_path = os.path.join(output_dir, "data_" + str(process_cnt+1) + '.csv')
    f = open(output_path, "w")
    f.writelines(data_list)
    f.close()
    logger.info("%s Finished." % process_name)

# 改进版
# 在这个函数中搞多线程
def hash_parse_f2f(data_source_name, job_id, local_logger = None):
    # 处理日志logger
    if local_logger is None:
        from utilities import logger
    else:
        logger = local_logger
    # 1. 先找 data source 的 csv 
    data_dir = os.path.join(mpc_data_dir, data_source_name)
    # 2. 确定读和写的2个路径
    output_dir = os.path.join(mpc_job_dir, job_id, "data_dicts")
    # 判断路径存在和创建路径
    if os.path.exists(output_dir) is False: os.makedirs(output_dir)
    # 3. 开始执行，这里是进行 f2f的执行，一个进程对付一个 data_dir/data_split_x.csv 
    logger.info("Start get data.")
    all_st_time = time.time()
    # 开始循环创建 进程
    process_list = []
    for i in range(CPU_COUNT):
        # 这里写这个处理函数
        p = Process(target = calculate_hash_f2f,\
            args=(data_dir, output_dir, logger, i))
        p.start() 
        process_list.append(p)
    # 都开启之后进行 join 
    logger.info("Waiting for all data process done.")
    for p in process_list:
        p.join()
    all_ed_time = time.time()
    logger.info("All data process done.")
    time_used = all_ed_time - all_st_time
    logger.info("Data get time used: %g second." % time_used)
    return True

# ---------------------------------------------------------------- #
def rsa_parse_f2f(data_source_name, job_id, local_logger = None):
    # 处理日志logger
    if local_logger is None:
        from utilities import logger
    else:
        logger = local_logger
    # 1. 先找 data source 的 csv
    data_dir = os.path.join(mpc_data_dir, data_source_name)
    # 2. 确定读和写的2个路径
    output_dir = os.path.join(mpc_job_dir, job_id, "hash_dicts")
    # 判断路径存在和创建路径
    if os.path.exists(output_dir) is False: os.makedirs(output_dir)
    # f_hash_output = open(output_path_hash, "w")
    # 3. 开始执行，这里是进行 f2f的执行，一个进程对付一个 data_dir/data_split_x.csv
    logger.info("Start calculate hash.")
    all_st_time = time.time()
    # 开始循环创建 进程
    process_list = []
    for i in range(CPU_COUNT):
        # 这里写这个处理函数
        p = Process(target = calculate_hash_f2f,\
            args=(data_dir, output_dir, logger, i))
        p.start()
        process_list.append(p)
    # 都开启之后进行 join
    logger.info("Waiting for all Hash calculation process done.")
    for p in process_list:
        p.join()
    # p.close()
    # p.join()
    all_ed_time = time.time()
    logger.info("All Hash calculation process done.")
    time_used = all_ed_time - all_st_time
    logger.info("Hash calculation time used: %g second." % time_used)
    logger.info("Function < hash_pars_f2f > finished.")
    return True

# 这个函数已经不能直接用了
def hash_parse_queue(data_source_name, job_id, local_logger = None):
    # 处理日志logger
    if local_logger is None:
        from utilities import logger
    else:
        logger = local_logger
    # 1. 先找 data source 的 csv 
    csv_path = os.path.join(mpc_data_dir, data_source_name+".csv")
    try:
        f_csv = open(csv_path, 'r')
    except:
        return False
    id_list = f_csv.readlines()
    # print(str(id_list)[:100])
    f_csv.close()
    # 2. 确定读和写的2个路径
    output_path_hash = os.path.join(mpc_job_dir, job_id, "hash_dict.pickle")
    # f_hash_output = open(output_path_hash, "w")
    # 3. 读 datasource， 去重复
    id_list = list(set(id_list)) 
    # id_list = id_list * 100
    id_length = len(id_list)
    logger.info("The id list lenght is: %d." % id_length)
    # 4. 计算哈希，
    # 这部分以后可以换成多线程处理，优化速度
    # 4000万的 id ，单线程消耗27秒
    logger.info("Start calculate hash.")
    all_st_time = time.time()
    process_list = []
    if id_length < 10000:
    # if True:
        process_count = 1
    elif id_length < 4000000:
        process_count = 4
    # elif id_length < 40000000:
    #     process_count = 4
    else:
        process_count = CPU_COUNT
    thread_length = int(id_length / process_count) 
    # 进程间通信用的   q 
    q = Queue()
    # 开始循环
    for i in range(process_count):
        process_name = "MPC_Hash_Process-" + str(i+1)
        # print(thread_name)
        if i == process_count - 1:
            st_index = thread_length * i
            ed_index = id_length
        else:
            st_index = i*thread_length
            ed_index = (i+1)*thread_length
        # p.apply_async(calculate_hash, args=(thread_name, st_index, ed_index))
        # p.apply_async(long_time_calculate)
        # p = Process(target=long_time_calculate)
        p = Process(target = calculate_hash_queue, args=(id_list[st_index : ed_index], logger, process_name, "_salt_X", q))
        p.start() 
        process_list.append(p)
    # 都开启之后进行 join 
    logger.info("Waiting for all Hash calculation process done.")    
    for p in process_list:
        p.join()
    # p.close()
    # p.join()
    all_ed_time = time.time()
    logger.info("All Hash calculation process done.")    
    time_used = all_ed_time - all_st_time
    logger.info("Hash calculation time used: %g second." % time_used)
    logger.info("Hash calculation finished.")
    # print(q)
    # 5. 从 Queue 中恢复hash，并保存到相应目录中
    hash_dict = merge_dict_from_queue(q)
    # print(hash_dict)
    with open(output_path_hash, 'wb') as f:
        pickle.dump(hash_dict, f)
        logger.info("Hash dict saved to %s" %output_path_hash)
    logger.info("Function < hash_parse > finished.")
    return True

# merge dict 
def merge_dict_from_queue(q):
    res_dict = {}
    while not q.empty():
        each_dict = q.get()
        res_dict.update(each_dict)
    # merge 
    return res_dict