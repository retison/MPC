import os
import time
from multiprocessing import Process

from config import mpc_data_dir, mpc_job_dir, CPU_COUNT

# 使 file to file 的方式输出
# 不在这个函数中搞多进程 
def calculate_data_f2f(data_dir, output_dir, logger, process_cnt):
    index = process_cnt
    process_name = "MPC_Data_Process-" + str(process_cnt + 1)
    logger.info("Start process: %s for calculation in %s." %(process_name, os.getpid()))
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
def data_parse_f2f(data_source_name, job_id, local_logger = None):
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
        p = Process(target = calculate_data_f2f, \
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
