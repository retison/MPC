import os
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from mpc.protocol import rsa
from mpc.utils import get_rsa_key
from mpc_handler.base import data_base_handler
# from utilities import global_var as global_dict
from utilities.database_manager import database_manager
from utilities.status_code import *
from utilities.utilities import get_log_file_handler
from utilities.utilities import wc_count


class IntermediateGetHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=8)

    @run_on_executor
    def post(self):
        self.create_logger()
        self.logger.info("Start Decode Check.")
        if self.decode_check(["job_id"], [str]) is False:
            self.logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        self.logger.info("Decode Check Success.")
        # 取数据
        self.job_id = self.request_dict["job_id"]
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        self.mpc_method = self.extract_mpc_method_from_job_id() 
        if self.mpc_method == 0:
            self.return_parse_result(OPERATION_FAILED,\
                "Wrong job_id format.", {"requested_job_id": self.job_id})
            return 
        self.logger.info("Get mpc_method: %d." %self.mpc_method)
        # 初始 index 
        try:
            self.st_index = self.request_dict["st_index"]
        except:
            self.st_index = 0
        # 新增的 split 参数
        try:
            self.split = self.request_dict["split"]
        except:
            self.split = 1
        # 长度
        try:
            self.length = self.request_dict["length"]
        except:
            self.length = 0
        self.logger.info("Get parameters")
        # 增加对 split 的检查，毕竟每个机器 split 不一样
        # 其他机器可以通过制定 split = 0 探明需要循环的次数
        if self.split >= CPU_COUNT + 1 or self.split <= 0:
            split_length = []
            # 探明每个 split 的长度
            for i in range(CPU_COUNT):
                hash_path = os.path.join(self.job_dir, "hash_dicts", "hash_%d.csv" % (i + 1))
                if self.action == "result_get":
                    hash_path = os.path.join(self.job_dir, "mpc_result", "result_%d.csv" % (i + 1))
                line_count = wc_count(hash_path)
                self.logger.info("Get line count of %s: %d" % (hash_path, line_count))
                split_length.append(line_count)
                pass
            self.return_parse_result(0, SUCCESS, {"split_count": CPU_COUNT, "split_length_list": split_length,"result": [] })
            return
        # 做必要的检查
        if self.st_index < 0 or self.length < 0 or self.split < 1:
            self.return_parse_result(OPERATION_FAILED,\
                "Request parameter smaller than 0.", \
                {"requested_st_index": self.st_index, "request_length": self.length})
            return 
        self.logger.info("Parameter check OK.")
        # 检查job_id是否存在
        if self.check_job_id_exists() is False:
            self.return_parse_result(OPERATION_FAILED,\
                "Requested job_id NOT exist.", {"requested_job_id": self.job_id})
            return
        self.logger.info("Job exists.")
        self.dbm = database_manager(local_db_ip,local_db_port,\
                      local_db_username, local_db_passwd, local_db_dbname)
        # print(self.job_id)
        # 检查 job status 
        job_status = self.get_job_status()
        # print(job_status)
        if job_status != "ready" and job_status != "running":
        # if job_status != "ready":
            # print('job status error')
            resp_data = {"requested_job_status": job_status}
            self.return_parse_result(OPERATION_FAILED,\
                "Job status is not ready, please check the requested job",resp_data)
            return 
        self.logger.info("Job status OK.")
        # 此时我们有 self.job_dir，需要：
        # 1. 建立单独的 log handler 
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("Get Intermediate Result API called")
        
        # 2. 根据情况取 hash csv  内容
        if self.mpc_method == 1: # hash
            res_list, hash_length = self.get_intermediate_hash()
            self.logger.info("Get Intermediate Result with length %d" % len(res_list))
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], \
                {"split_count": CPU_COUNT, "split_length": hash_length, "result_length": len(res_list),"result": res_list} ) 
            self.logger.info("Get Intermediate Result API finished.")
            return
        else:
            self.logger.error("Unsupported mpc method.")
            self.return_parse_result(OPERATION_FAILED,\
                "Unsupported mpc method.", {})
            return    
    
    # 2022.10.17 
    # 需要重写！
    # 因为 hash 的存储形式发生了变化
    # 搞定，后续测试即可
    def get_intermediate_hash(self):
        # assert self.mpc_method == 2
        # 针对 intermediate get API , 拼接 pickle path  
        hash_path = os.path.join(self.job_dir, "hash_dicts", "hash_%d.csv" % self.split)
        
        # 针对其他 API （result get）
        if self.action == "result_get": 
            hash_path = os.path.join(self.job_dir, "mpc_result", "result_%d.csv" % self.split)
        
        self.logger.info("Get result from hash path: %s" % hash_path)
        if os.path.exists(hash_path) is False:
            self.logger.error("hash_%d.csv NOT exist." % self.st_index)
            return []
        
        # 然后开始载入 pickle
        with open(hash_path, "r") as f:
            hash_list = f.readlines()
        hash_lengh = len(hash_list)
        self.logger.info("Use parameter st_index:%d and length:%d" % (self.st_index, self.length))
        if self.st_index == 0 and self.length == 0:
            hash_list = hash_list
        elif self.st_index == 0 and self.length != 0:
            hash_list = hash_list[:self.length]
        elif self.st_index != 0 and self.length == 0:
            hash_list = hash_list[self.st_index: ]
        else:
            hash_list = hash_list[self.st_index: self.st_index + self.length]
        # 搞定之后返回
        # 处理 res_list
        res_list = []
        for i in hash_list:
            hash_value = i.split(',')[0].strip()
            res_list.append(hash_value)
        return res_list, hash_lengh
