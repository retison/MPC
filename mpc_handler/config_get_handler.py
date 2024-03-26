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
from utilities.utilities import wc_count


class GetConfigHandler(data_base_handler.DataBaseHandler):
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
        # 检查job_id是否存在
        if self.check_job_id_exists() is False:
            self.return_parse_result(OPERATION_FAILED, \
                                     "Requested job_id NOT exist.", {"requested_job_id": self.job_id})
            return
        self.logger.info("Job exists.")
        self.dbm = database_manager(local_db_ip, local_db_port, \
                                    local_db_username, local_db_passwd, local_db_dbname)
        # print(self.job_id)
        # 检查 job status 
        job_status = self.get_job_status()
        if job_status != "ready" and job_status != "running":
            resp_data = {"requested_job_status": job_status}
            self.return_parse_result(OPERATION_FAILED, \
                                     "Job status is not ready, please check the requested job", resp_data)
            return
        self.logger.info("Job status OK.")
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("Get Config API called")
        # TODO 接受ini文件并返回成功
        try:
            config_place = os.path.join(self.job_dir, "config.ini")
            config = self.request_dict["config"]
            with open(config_place, "w") as f:
                f.write(config)
            f.close()
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {"job_id": self.job_id})
            self.logger.info("Get config API finished.")
            return
        except:
            self.logger.error("have not gotten the config.")
            self.return_parse_result(OPERATION_FAILED, \
                                     "have not gotten the config.", {})
            return

