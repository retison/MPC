import base64
import os
from concurrent.futures import ThreadPoolExecutor

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from mpc_handler.base import data_base_handler
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
        if self.decode_check(["job_id"], [str]) is False:
            self.logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        self.logger.info("Decode Check Success.")
        # 取数据
        self.job_id = self.request_dict["job_id"]
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        self.key = self.get_job_key(self.job_id)
        self.key = self.key.encode()[:16]
        self.mpc_method = self.get_mpc_method()
        if self.mpc_method != "substring":
            self.return_parse_result(OPERATION_FAILED, \
                                     "Wrong job_id format.", {"requested_job_id": self.job_id})
            return
        self.logger.info("Get mpc_method: %s." % self.mpc_method)
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
        # 增加对 split 的检查，毕竟每个机器 split 不一样
        # 其他机器可以通过制定 split = 0 探明需要循环的次数
        if self.split >= CPU_COUNT + 1 or self.split <= 0:
            split_length = []
            # 探明每个 split 的长度
            for i in range(CPU_COUNT):
                data_path = os.path.join(self.job_dir, "data_dicts", "data_%d.csv" % (i + 1))
                if self.action == "result_get":
                    data_path = os.path.join(self.job_dir, "mpc_result", "result_%d.csv" % (i + 1))
                line_count = wc_count(data_path)
                self.logger.info("Get line count of %s: %d" % (data_path, line_count))
                split_length.append(line_count)
                pass
            self.return_parse_result(0, SUCCESS,
                                     {"split_count": CPU_COUNT, "split_length_list": split_length, "result": []})
            return
        # 做必要的检查
        if self.st_index < 0 or self.length < 0 or self.split < 1:
            self.return_parse_result(OPERATION_FAILED, \
                                     "Request parameter smaller than 0.", \
                                     {"requested_st_index": self.st_index, "request_length": self.length})
            return
        self.logger.info("Parameter check OK.")
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
        # print(job_status)
        if job_status != "ready" and job_status != "running":
            # if job_status != "ready":
            # print('job status error')
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
        self.logger.info("Get Intermediate Result API called")

        # 2. 根据情况取 data csv  内容
        if self.mpc_method == 'substring':  # data
            iv_list, ct_list, data_length = self.get_output_data()
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], \
                                     {"split_count": CPU_COUNT, "split_length": data_length,
                                      "result_length": len(iv_list), "iv": iv_list, "ct": ct_list})
            self.logger.info("Get Intermediate Result API finished.")
            return
        else:
            self.logger.error("Unsupported mpc method.")
            self.return_parse_result(OPERATION_FAILED, \
                                     "Unsupported mpc method.", {})
            return

            # 2022.10.17

    # 需要重写！
    # 因为 data 的存储形式发生了变化
    # 搞定，后续测试即可
    def get_output_data(self):
        data_path = os.path.join(self.job_dir, "data_dicts", "data_%d.csv" % self.split)

        # 针对其他 API （result get）
        if self.action == "result_get":
            data_path = os.path.join(self.job_dir, "mpc_result", "result_%d.csv" % self.split)

        self.logger.info("Get result from data path: %s" % data_path)
        if os.path.exists(data_path) is False:
            self.logger.error("data_%d.csv NOT exist." % self.st_index)
            return []

        # 然后开始载入 pickle
        with open(data_path, "r") as f:
            data_list = f.readlines()
        data_lengh = len(data_list)
        self.logger.info("Use parameter st_index:%d and length:%d" % (self.st_index, self.length))
        if self.st_index == 0 and self.length == 0:
            data_list = data_list
        elif self.st_index == 0 and self.length != 0:
            data_list = data_list[:self.length]
        elif self.st_index != 0 and self.length == 0:
            data_list = data_list[self.st_index:]
        else:
            data_list = data_list[self.st_index: self.st_index + self.length]
        # 搞定之后返回
        # 处理 res_list
        res_list = []
        for i in data_list:
            data_value = i.strip()
            res_list.append(data_value)
        iv, ct = self.encrypt_message(res_list)
        return iv, ct, data_lengh

    def encrypt_message(self, messages):
        iv_list = []
        ct_list = []
        for message in messages:
            cipher = AES.new(self.key, AES.MODE_CBC)
            ct_bytes = cipher.encrypt(pad(message.encode(), AES.block_size))
            iv = base64.b64encode(cipher.iv).decode('utf-8')
            iv_list.append(iv)
            ct = base64.b64encode(ct_bytes).decode('utf-8')
            ct_list.append(ct)
        return iv_list, ct_list
