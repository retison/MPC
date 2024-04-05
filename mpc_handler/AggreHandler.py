import asyncio
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username, MPC_PORT
from config import mpc_job_dir
from flow_control.mpc_aggre.utils import get_operator
from mpc_handler.base import data_base_handler
from mpc_handler.utils import list_operation, is_port_available
import mpyc.runtime as runtime
from utilities.database_manager import database_manager
from utilities.status_code import *
from utilities.utilities import get_log_file_handler


class AggreHandler(data_base_handler.DataBaseHandler):
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
        self.operators = self.extract_variables(None)
        aggre_operations = ["sum","max","min","count","avg"]
        for aggre_operation in aggre_operations:
            try:
                for operator in self.operators:
                    operator.remove(aggre_operation)
            except:
                pass
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
        # job_status = self.get_job_status()
        # if job_status != "ready" and job_status != "running":
        #     resp_data = {"requested_job_status": job_status}
        #     self.return_parse_result(OPERATION_FAILED, \
        #                              "Job status is not ready, please check the requested job", resp_data)
        #     return
        # self.logger.info("Job status OK.")
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")

        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("Arith API called")
        self.arith = 0
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
        secfxp = mpc.SecFxp(128, 96, key)
        data_dicts = {}
        for operator in operators:
            data_list = self.get_data(operator)
            data_dicts[operator] = []
            for i in data_list:
                change_data = secfxp(10)
                change_data.share.value = i
                data_dicts[operator].append(change_data)
        text_result, variables = self.get_mpc_operation(method, data_dicts,mpc)
        result = variables[text_result]
        all_result = await mpc.gather(result)
        self.store_data(all_result, result_name)
        self.logger.info(f"job {self.job_id} mpc calculation finish")

    def extract_variables(self,text):
        pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
        if text is None:
            variables = []
            for operation in self.operation:
                variable = re.findall(pattern, operation)
                variables.append(variable)
        else:
            variables = re.findall(pattern, text)
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

    def get_mpc_operation(self,text, variables,mpc):
        pattern = r'(max|min|count|avg|sum)\((.*?)\)'
        functions = {
            'max': lambda x: mpc.max(x),
            'min': lambda x: mpc.min(x),
            'sum': lambda x: mpc.sum(x),
            'count': lambda x: len(x),
            'avg': lambda x: mpc.statistics.mean(x)
        }

        def arith_operation(text, variables):
            result = []
            operators = self.extract_variables(text)
            length = len(variables[operators[0]])
            for curr_place in range(length):
                curr_variables = {}
                for operator in operators:
                    curr_variables[operator] = variables[operator][curr_place]
                result.append(eval(text, curr_variables))
            variables[f"arith{self.arith}"] = result
            self.arith += 1
            return f"arith{self.arith - 1}"

        def replace_function(match):
            func_name, args = match.groups()
            data_name = args
            if args not in variables:
                data_name = arith_operation(args,variables)
            # 调用加密类的函数获取结果
            result = functions[func_name](variables[data_name])
            # 创建新变量存储结果，并返回新变量名
            new_var_name = f'{data_name}_{func_name}_result'
            variables[new_var_name] = [result]
            return new_var_name

        # 使用正则表达式替换函数调用
        result = re.sub(pattern, replace_function, text)
        result = arith_operation(result,variables)

        return result, variables

