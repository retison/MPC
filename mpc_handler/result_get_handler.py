import os
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
# from utilities import global_var as global_dict
from utilities.database_manager import database_manager
from utilities.status_code import *
from utilities.utilities import get_log_file_handler


class ResultGetHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=4)

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
        
        # 需要考虑一下还要不要做状态检查
        # 检查 job status 
        job_status = self.get_job_status()
        # print(job_status)
        if job_status == "running":
            # print('job status error')
            self.return_parse_result(OPERATION_FAILED,\
                "Job status is still running, please wait.", {"requested_job_status": self.job_status})
            return
        elif job_status == "failed":
            # print('job status error')
            self.return_parse_result(OPERATION_FAILED,\
                "Job failed.", {"requested_job_status": self.job_status})
            return
        self.logger.info("Job status OK.")

        # 此时我们有 self.job_dir，需要：
        # 1. 建立单独的 log handler 
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("Get Result API called")
        
        # 2. 根据情况取 pickle 内容
        if self.mpc_method == 1: # hash 
            res_list = self.get_result_hash()
            self.logger.info("Get Intermediate Result with length %d" % len(res_list))
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], \
                {"split_count": CPU_COUNT, "result": res_list} ) 
            self.logger.info("Get Intermediate Result API finished.")
            return
        # TODO rsa pickle 内容
        elif self.mpc_method == 2: # hash
            res_list = self.get_result_hash()
            self.logger.info("Get Intermediate Result with length %d" % len(res_list))
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], \
                {"split_count": CPU_COUNT, "result": res_list} )
            self.logger.info("Get Intermediate Result API finished.")
            return
        #TODO ot pickle 内容
        elif self.mpc_method == 1:  # ot
            res_list = self.get_result_hash()
            self.logger.info("Get Intermediate Result with length %d" % len(res_list))
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], \
                                     {"split_count": CPU_COUNT, "result": res_list})
            self.logger.info("Get Intermediate Result API finished.")
            return
        else:
            self.logger.error("Unsupported mpc method.")
            self.return_parse_result(OPERATION_FAILED,\
                "Unsupported mpc method.", {})
            return    
    
    def get_result_hash(self):
        # assert self.mpc_method == 1
        # 拼接 pickle path 
        hash_path = os.path.join(self.job_dir, "mpc_result", "result_%d.csv" % self.split)
        self.logger.info("Get result from hash path: %s" % hash_path)
        if os.path.exists(hash_path) is False:
            self.logger.error("intersection split %d NOT exist." % self.st_index)
            return []
        # 然后开始载入 pickle
        with open(hash_path, "r") as f:
            hash_list = f.readlines()
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
        return res_list
