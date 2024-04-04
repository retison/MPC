
import json
import os
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
# from utilities import global_var as global_dict
from utilities.database_manager import database_manager
from utilities.sql_template import get_mpc_job_query_sql
from utilities.status_code import *
from utilities.utilities import get_log_file_handler


class JobQueryHandler(data_base_handler.DataBaseHandler):
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
        # 获取 job_id
        self.job_id = self.request_dict["job_id"]
        self.logger.info("The job_id is: %s." % self.job_id)
        self.job_dir = os.path.join(mpc_job_dir, self.job_id )
        # 创建专属文件夹
        if not os.path.exists(self.job_dir): os.makedirs(self.job_dir)
        # 增加日志的 handler 
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        # 主要信息获取完毕，开始查job的各种内容
        # 主要耗时在这里，但是还好
        self.dbm = database_manager(local_db_ip,local_db_port,\
        local_db_username, local_db_passwd, local_db_dbname)
        # query 改这里了 
        job_query_sql = get_mpc_job_query_sql(self.job_id)
        self.logger.debug("The job query SQL is: %s." % job_query_sql.replace("\n", " "))
        query_res = self.dbm.query(job_query_sql)
        self.logger.debug("The query result is: %s." % query_res)
        # 检查状态之后返回
        
        # 准备返回
        if len(query_res) == 0:
            self.logger.error("The query result is empty.")
            self.return_parse_result(OPERATION_FAILED,\
                "Requested job_id NOT exist.", {"requested_job_id": self.job_id})
            return 
        else:
            query_res = query_res[0] # 取第一个元素即可，这里应该是一个完整的列表了
        # 下面组织返回的 response
        data_dict = {}
        data_dict["job_id"] = self.job_id
        data_dict["status"] = query_res[0]
        data_dict["create_time"] = query_res[4]
        data_dict["update_time"] = query_res[5]
        data_dict["start_time"] = query_res[6]
        data_dict["end_time"] = query_res[7]
        # 返回结果
        self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], data_dict)
        return 


        
        