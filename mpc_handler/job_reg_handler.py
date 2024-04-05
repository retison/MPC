import asyncio
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username, mpc_data_dir
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
from mpc_handler.utils import generate_prim
from mpyc import runtime
from mpyc.runtime import mpc
from utilities import logger
from utilities.database_manager import database_manager
from utilities.sql_template import get_mpc_job_insert_sql
from utilities.status_code import *
from utilities.utilities import get_log_file_handler


# TODO 还有对limit/order by/group by 的
class JobRegHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=4)

    @run_on_executor
    def post(self):
        self.create_logger()
        logger.info("Start Decode Check.")
        if self.decode_check(["job_id", "data_list", "data_length","config","data_from"],
                             [str, list, int,str,list]) is False:
            logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        logger.info("Decode Check Success.")
        self.data_list = self.request_dict["data_list"]
        self.data_from = self.request_dict["data_from"]
        self.data_length = self.request_dict["data_length"]
        self.job_id = self.request_dict["job_id"] + "_host"
        if "key" in self.request_dict.keys():
            self.key = self.request_dict["key"]
        else:
            self.key = generate_prim()
        logger.info("The job_id is: %s." % self.job_id)
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        # 创建专属文件夹
        if not os.path.exists(self.job_dir): os.makedirs(self.job_dir)
        # 增加日志的 handler 
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")

        self.job_log_handler = get_log_file_handler(job_log_path)
        logger.addHandler(self.job_log_handler)
        logger.info("MPC Job %s created." % self.job_id)
        try:
            config_place = os.path.join(self.job_dir, "config.ini")
            config = self.request_dict["config"]
            with open(config_place, "w") as f:
                f.write(config)
            f.close()
            self.logger.info("Get config API finished.")
        except:
            self.logger.error("have not gotten the config.")
            self.return_parse_result(OPERATION_FAILED, \
                                     "have not gotten the config.", {})
        # 检查完毕，开始往db里写任务信息
        # 主要耗时在这里，但是还好
        self.dbm = database_manager(local_db_ip, local_db_port, \
                                    local_db_username, local_db_passwd, local_db_dbname)
        current_time = int(time.time())
        job_insert_sql = get_mpc_job_insert_sql(self.job_id, self.key,
                                                "registered", current_time, current_time)
        logger.debug("The job insert SQL is: %s." % job_insert_sql.replace("\n", " "))
        self.dbm.insert(job_insert_sql)
        # 检查状态之后返回
        if self.dbm.insert_success is True:
            self.return_parse_result(0, 'success', {"job_id": self.job_id})
        else:
            self.return_parse_result(OPERATION_FAILED, \
                                     status_msg_dict[OPERATION_FAILED] + ": job database insert failed", {})
        self.execute_at_backend(self.abc)  # 后台执行
        return

    def abc(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        sys.argv.append(f"-C../tmp/job/{self.job_id}/config.ini")
        sys.argv.append("-ssl")
        curr_mpc = runtime.setup()
        data_dir = mpc_data_dir
        result_dir = os.path.join(self.job_dir, "data_dicts")
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        for data_id in range(len(self.data_list)):
            curr_data_dir = os.path.join(data_dir,self.data_list[data_id])
            job_data_name = self.data_list[data_id].split("_")[:-1]
            job_data_name = '_'.join(job_data_name)
            curr_result_dir = os.path.join(result_dir,job_data_name)
            if not os.path.exists(curr_result_dir):
                os.makedirs(curr_result_dir)
            if os.path.exists(curr_data_dir):
                res_list = self.get_data(self.data_list[data_id])
            else:
                res_list = [0] * self.data_length
            loop.run_until_complete(self.mpc_calculate(curr_mpc, res_list, curr_result_dir, self.data_from[data_id]))
        loop.close()
        self.change_job_status(self.job_id, "running")



    def get_data(self, data_id):
        data_dir = os.path.join(mpc_data_dir, data_id)
        if not os.path.exists(data_dir):
            logger.error("data dir is not exists!")
        file_list = os.listdir(data_dir)
        data_list = []
        for lis in file_list:
            with open(os.path.join(data_dir, lis), "r") as f:
                data_list += f.readlines()
            f.close()
        data_list = [float(i[:-1]) for i in data_list]
        logger.info("get data success!")
        return data_list

    async def mpc_calculate(self, curr_mpc, data_list, result_dir, data_from):
        await curr_mpc.start()
        key = int(self.get_job_key(self.job_id), 10)
        secint = curr_mpc.SecFxp(128, 96, key)
        input_value = list(map(secint, data_list))
        input_res = curr_mpc.input(input_value,senders=[data_from])
        await curr_mpc.shutdown()
        result = await curr_mpc.gather(input_res[0])
        with open(os.path.join(result_dir, "split.csv"), "w") as f:
            for data in result:
                f.write(str(data) + "\n")
            f.close()
        logger.info(f"job {self.job_id} mpc calculation finish")
