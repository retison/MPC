import json
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
# from utilities import global_var as global_dict
from utilities.database_manager import database_manager
from utilities.status_code import *
from utilities.utilities import get_log_file_handler
from utilities.utilities import wc_count


class ResultGetHandler(data_base_handler.DataBaseHandler):
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
        job_status = self.get_job_status()
        if job_status != "success":
            resp_data = {"requested_job_status": job_status}
            self.return_parse_result(OPERATION_FAILED, \
                                     "Job status is not success", resp_data)
            return
        self.logger.info("Job status OK.")
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        result_csv = self.merge_csv_files(os.path.join(self.job_dir, "decryption_result"))
        try:
            # 将二维数组转换为 JSON 格式
            json_data = json.dumps(result_csv.values.tolist())
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {"result_csv":json_data})
            self.logger.info("Get result API finished.")
            return
        except:
            self.logger.error("have not gotten the result.")
            self.return_parse_result(OPERATION_FAILED, \
                                     "have not gotten the result.", {})
            return

    def merge_csv_files(self, folder_path):
        all_files =["psi_result.csv"]
        file_num = len(os.listdir(folder_path))
        for file_name in range(file_num-1):
            all_files.append(f"mpc_result_{file_name}.csv")
        # 确保文件夹中有 CSV 文件
        if not all_files:
            self.logger.error("There is not any csv")
            return
        combined_df = pd.DataFrame()
        for file in all_files:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path,header=None)
            name = file.split(".")[0]
            combined_df[name] = df.iloc[:, 0]
        self.logger.info(f"job {self.job_id}result csv get")
        return combined_df
