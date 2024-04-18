import asyncio
import os
import random
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username, MPC_PORT
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
from mpc_handler.utils import list_operation, is_port_available, revert_data
import mpyc.runtime as runtime
from utilities.database_manager import database_manager
from utilities.status_code import *
from utilities.utilities import get_log_file_handler


class ArithHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=8)

    @run_on_executor
    def post(self):
        self.create_logger()
        self.logger.info("Start Decode Check.")
        if self.decode_check(["job_id", "operation", "result_name"], [str, list, list]) is False:
            self.logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        self.logger.info("Decode Check Success.")
        # 取数据
        self.job_id = self.request_dict["job_id"]
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        # 是否有信息
        self.logger.info("Get parameters")
        # 检查job_id是否存在
        if self.check_job_id_exists() is False:
            self.return_parse_result(OPERATION_FAILED, \
                                     "Requested job_id NOT exist.", {"requested_job_id": self.job_id})
            return
        self.logger.info("Job exists.")
        self.operation = self.request_dict["operation"]
        self.result_name = self.request_dict["result_name"]
        if len(self.result_name) != len(self.operation):
            self.return_parse_result(OPERATION_FAILED, \
                                     f"task name is not equal to task num", {})
            return
        self.operators = self.extract_variables()
        for operator in self.operators:
            for element in operator:
                if not os.path.exists(os.path.join(self.job_dir, element)):
                    continue
                self.return_parse_result(OPERATION_FAILED, \
                                         f"job {self.job_id} data get failed", {})
                self.logger.info(f"job {self.job_id} data get failed")
                return
        self.dbm = database_manager(local_db_ip, local_db_port, \
                                    local_db_username, local_db_passwd, local_db_dbname)
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
        self.logger.info("Arith API called")
        self.execute_at_backend(self.calculation_job())
        self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {})
        return

    def calculation_job(self):
        key = int(self.get_job_key(self.job_id), 10)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for task in range(len(self.operation)):
            curr_mpc = runtime.setup()
            loop.run_until_complete(self.mpc_calculate(self.operation[task], curr_mpc, key,self.operators[task],self.result_name[task]))
        loop.close()
        self.change_job_status(self.job_id,"success")

    async def mpc_calculate(self, method, mpc, key,operators,result_name):
        data_length = -1
        data_dicts = {}
        curr_result_list = []
        secfxp = mpc.SecFxp(128, 96, key)
        for operator in operators:
            data_list = self.get_data(operator)
            data_dicts[operator] = data_list
            data_length = len(data_list)
        for curr_place in range(data_length):
            variables = {}
            for operator in operators:
                a = secfxp(10)
                a.share.value = revert_data(data_dicts[operator][curr_place],key)
                variables[operator] = a
            result = eval(method, variables)
            curr_result_list.append(result)
        curr_result_list = await mpc.gather(curr_result_list)
        self.store_data(curr_result_list, result_name)
        self.logger.info(f"job {self.job_id} mpc calculation finish")

    def extract_variables(self):
        pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
        variables = []
        for operation in self.operation:
            variable = re.findall(pattern, operation)
            variables.append(variable)
        return variables

    def get_data(self, data_name):
        data_dir = os.path.join(self.job_dir, "data_dicts")
        data_dir = os.path.join(data_dir, data_name)
        if not os.path.exists(data_dir):
            self.logger.error("data dir is not exists!")
        file_list = os.listdir(data_dir)
        data_list = []
        for lis in file_list:
            with open(os.path.join(data_dir, lis), "r") as f:
                data_list += f.readlines()
            f.close()
        data_list = [int(i[:-1]) for i in data_list]
        self.logger.info("get data success!")
        return data_list

    def store_data(self, result_list,name):
        result_dicts = os.path.join(self.job_dir, "result_dicts")
        if not os.path.exists(result_dicts):
            os.makedirs(result_dicts)
        curr_result_dicts = os.path.join(result_dicts, name + ".csv")
        with open(curr_result_dicts, "w") as f:
            for data in result_list:
                f.write(str(data) + "\n")
        f.close()
