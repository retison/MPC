import asyncio
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username, MPC_PORT
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
from mpc_handler.utils import list_operation, is_port_available
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
        if self.decode_check(["job_id"], [str]) is False:
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
        self.mpc_method = self.get_mpc_method()
        self.logger.info("Get mpc_method: %s." % self.mpc_method)
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
        # 此时我们有 self.job_dir，需要：
        # 1. 建立单独的 log handler
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("Integer API called")

        # TODO 需要先读取本地文件，然后创建一个argv，然后开始mpc.run
        # TODO 启动即可
        self.res_list = self.get_data()
        if self.res_list is None:
            self.return_parse_result(OPERATION_FAILED, "split too huge", {"is_ok": False})
            return
        th = threading.Thread(target=self.abc)
        with open(f"tmp/job/{self.job_id}/config.ini", 'r') as f:
            port = f.readlines()
        f.close()
        curr_port = -1
        for i in range(len(port)):
            if port[i] == 'host = \n':
                curr_port = re.findall(r"port = (.+?)\n", port[i + 1])[0]
                break
        curr_port = int(curr_port, 10)
        if is_port_available(curr_port):
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {})
            th.start()
            return
        else:
            self.return_parse_result(OPERATION_FAILED, "port is used.", {})

    def abc(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        sys.argv.append(f"-C../tmp/job/{self.job_id}/config.ini")
        sys.argv.append("-ssl")
        curr_mpc = runtime.setup()
        # 在事件循环中运行异步任务
        loop.run_until_complete(self.mpc_calculate(curr_mpc, self.mpc_method, self.res_list))
        loop.close()

    def get_data(self):
        data_dir = os.path.join(self.job_dir, "data_dicts")
        if not os.path.exists(data_dir):
            self.logger.error("data dir is not exists!")
        file_list = os.listdir(data_dir)
        data_list = []
        for lis in file_list:
            with open(os.path.join(data_dir, lis), "r") as f:
                data_list += f.readlines()
            f.close()
        data_list = [float(i[:-1]) for i in data_list]
        return data_list

    async def mpc_calculate(self, curr_mpc, method, data_list):
        await curr_mpc.start()
        key = int(self.get_job_key(self.job_id), 10)
        secint = curr_mpc.SecFxp(128, 96, key)
        input_value = list(map(secint, data_list))
        input_res = curr_mpc.input(input_value)
        result = list_operation(input_res, method)
        await curr_mpc.output(result, receivers=[0])
        await curr_mpc.shutdown()
        self.logger.info(f"job {self.job_id} mpc calculation finish")
