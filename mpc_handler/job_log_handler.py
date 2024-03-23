import os
# from utilities import global_var as global_dict
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from mpc_handler.base import data_base_handler
from utilities.status_code import *
from utilities.utilities import get_log_file_handler


class JobLogHandler(data_base_handler.DataBaseHandler):
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
        # 检查job_id是否存在
        if self.check_job_id_exists() is False:
            self.return_parse_result(OPERATION_FAILED,\
                "Requested job_id NOT exist.", {"requested_job_id": self.job_id})
            return
        # 获取 count 并且检查
        try:
            self.count = self.request_dict["count"]
        except:
            self.count = 0
        # 数值检查
        if self.count < 0 :
            self.return_parse_result(OPERATION_FAILED,\
                "Request parameter smaller than 0.", \
                {"requested_count": self.count})
            return 
        # 此时我们有 self.job_dir，需要：
        # 1. 根据情况取 log 内容
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        with open(job_log_path,'r') as f:
            res_list = f.readlines()
        if self.count != 0:
            res_list = res_list[(-1) * self.count:]
        # 2. 建立单独的 log handler 
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("Get Job Log API called")
        # 3. 返回结果
        self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {'log': res_list})
        return   
        
