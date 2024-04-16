import asyncio
import base64
import csv
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username, \
    mpc_data_dir
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
from mpyc import runtime
from utilities.database_manager import database_manager
from utilities.status_code import *
from utilities.utilities import get_log_file_handler
from utilities.utilities import wc_count


class OutputGetHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=8)

    @run_on_executor
    def post(self):
        self.create_logger()
        self.logger.info("Start Decode Check.")
        if self.decode_check(["job_id", "data_name","type"], [str, str,str]) is False:
            self.logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        self.logger.info("Decode Check Success.")
        # 取数据
        self.job_id = self.request_dict["job_id"]
        self.data_name = self.request_dict["data_name"]
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        self.key = self.get_job_key(self.job_id)
        self.key = int(self.key, 10)
        self.type =self.request_dict["type"]
        # 初始 index
        self.logger.info("Get parameters")

        # 检查job_id是否存在
        if self.check_job_id_exists() is False:
            self.return_parse_result(OPERATION_FAILED, \
                                     "Requested job_id NOT exist.", {"requested_job_id": self.job_id})
            return
        self.logger.info("Job exists.")
        self.dbm = database_manager(local_db_ip, local_db_port, \
                                    local_db_username, local_db_passwd, local_db_dbname)
        # 检查 job status
        job_status = self.get_job_status()
        if job_status != "success":
            resp_data = {"requested_job_status": job_status}
            self.return_parse_result(OPERATION_FAILED, \
                                     "Job status is not success, please check the requested job", resp_data)
            return
        self.logger.info("Job status OK.")
        # 此时我们有 self.job_dir，需要：
        # 1. 建立单独的 log handler
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("Get Intermediate Result API called")
        self.change_job_status(self.job_id, "running")
        self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {})
        self.execute_at_backend(self.get_output_data)
        return

    def get_output_data(self):
        psi_dir = os.path.join(self.job_dir, "decryption_result", "psi_result.csv")
        if os.path.exists(psi_dir):
            pass
        else:
            if not os.path.exists(os.path.join(self.job_id,"decryption_result")):
                os.mkdir(os.path.join(self.job_dir,"decryption_result"))
            source_dir = os.path.join(mpc_data_dir,f"{self.job_id}_psi")
            target_dir = os.path.join(self.job_dir, "decryption_result", "psi_result.csv")
            merge_csv_files(source_dir, target_dir)
            shutil.rmtree(source_dir)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        sys.argv.append(f"-C../tmp/job/{self.job_id}/config.ini")
        sys.argv.append("-ssl")
        curr_mpc = runtime.setup()
        data_dir = os.path.join(self.job_dir, "result_dicts", self.data_name + ".csv")
        res_list = self.get_data(data_dir)
        loop.run_until_complete(self.mpc_calculate(curr_mpc, res_list, self.data_name))
        loop.close()

    async def mpc_calculate(self, curr_mpc, data_list,data_name):
        await curr_mpc.start()
        key = int(self.get_job_key(self.job_id), 10)
        if self.type == "str":
            secfxp = curr_mpc.SecInt(128,key)
        else:
            secfxp = curr_mpc.SecFxp(128, 96, key)
        input_value = []
        for data in data_list:
            secdata = secfxp(10)
            secdata.share.value = data
            input_value.append(secdata)
        res = await curr_mpc.output(input_value, receivers=[0])
        await curr_mpc.shutdown()
        if None not in res:
            #TODO 这里是写入还是返回呢，这时候结果已经在本机了之后该怎么办呢
            with open(os.path.join(self.job_dir, "decryption_result", data_name + ".csv"),"w") as f:
                for i in res:
                    f.write(str(i) + "\n")
            f.close()
        self.logger.info(f"job {self.job_id} mpc calculation finish")
        self.change_job_status(self.job_id, "success")

    def get_data(self, data_dir):
        if not os.path.exists(data_dir):
            self.logger.error("data dir is not exists!")
        data_list = []
        with open(data_dir, "r") as f:
            data_list += f.readlines()
        f.close()
        data_list = [int(i[:-1]) for i in data_list]
        self.logger.info("get data success!")
        return data_list


def merge_csv_files(source_folder, target_file):
    with open(target_file, 'w', newline='') as target_file:
        csv_writer = csv.writer(target_file)
        for filename in os.listdir(source_folder):
            if filename.endswith('.csv'):
                file_path = os.path.join(source_folder, filename)
                with open(file_path, 'r') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    for row in csv_reader:
                        csv_writer.writerow(row)
