import os
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from gmpy2 import gmpy2
from tornado.concurrent import run_on_executor

from config import CPU_COUNT, local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from mpc.datastructure import bloom_filter
from mpc.protocol import rsa
from mpc.protocol.OT import sender_oprf, sender_hash
from mpc.utils import get_rsa_key
from mpc_handler.base import data_base_handler
# from utilities import global_var as global_dict
from utilities.database_manager import database_manager
from utilities.status_code import *
from utilities.utilities import get_log_file_handler
from utilities.utilities import wc_count


class InteractionHandler(data_base_handler.DataBaseHandler):
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
        self.mpc_method = self.extract_mpc_method_from_job_id()
        if self.mpc_method == 0:
            self.return_parse_result(OPERATION_FAILED, \
                                     "Wrong job_id format.", {"requested_job_id": self.job_id})
            return
        self.logger.info("Get mpc_method: %d." % self.mpc_method)
        # 是否有信息
        self.logger.info("Get parameters")
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
        self.logger.info("Interaction Result API called")
        # 2. 根据情况取 hash csv  内容
        # TODO 获取RSA 的中间内容
        if self.mpc_method == 2:  # rsa
            try:
                self.data_list = self.request_dict["data_list"]
            except:
                self.data_list = None
            self.logger.info("satrt create server.")
            # 创建一个server，这样创建的server和初始化的server是一样的
            server = rsa.Server()
            with open(os.path.join(self.job_dir, "public_key.txt"), "r") as f:
                public_key_str = f.readlines()
            f.close()
            with open(os.path.join(self.job_dir, "private_key.txt"), "r") as f:
                private_key_str = f.readlines()
            f.close()
            public_key_str = ''.join(public_key_str)
            private_key_str = ''.join(private_key_str)
            server.public_key = get_rsa_key(public_key_str)
            server.private_key = get_rsa_key(private_key_str)
            self.logger.info("finish create server.")
            # 没有data_list的就是想要获取bf
            if self.data_list is None:
                self.logger.info("get data hash")
                try:
                    split = self.request_dict["split"]
                except:
                    self.return_parse_result(OPERATION_FAILED, \
                                             "without split.", {})
                res_list = self.get_hash_data(split)
                if res_list is None:
                    self.return_parse_result(OPERATION_FAILED, "split too huge", {"is_ok": False})
                    return
                self.logger.info("get split %d" % split)
                # server 计算 signed_server_set 和 构建 bf
                res_list = [int(i, 16) for i in res_list]
                signed_server_set = server.sign_set(res_list)
                # must encode to bytes
                signed_server_set = [str(sss) for sss in signed_server_set]
                self.return_parse_result(SUCCESS, "IS ONLION", {"result": signed_server_set, "is_ok": True})
                self.logger.info("return bf split %d" % split)
                return
            # 有data_list的就是想要根据A获取B
            A = self.data_list
            A = [gmpy2.mpz(element) for element in A]
            B = server.sign_set(A)
            B = [int(i) for i in B]
            self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {"result": B})
            self.logger.info("Interaction API finished.")
            return

        # TODO 获取OT的中间内容
        elif self.mpc_method == 3:  # ot
            self.logger.info("get job %s information" % self.job_id)
            with open(os.path.join(self.job_dir, "job_info.txt")) as f:
                n = f.readline()
                s = f.readline()
                salt = f.readline()
            f.close()
            n = int(n[:-1], 10)
            s = int(s[:-1], 10)
            salt = int(salt[:-1], 10)
            ns = n + s
            random_bits_path = os.path.join(self.job_dir, "randombits.csv")
            select_bit_path = os.path.join(self.job_dir, "select_bit.txt")
            try:
                random_bits = self.request_dict["data_list"]
            except:
                random_bits = []
            # 是否对面发送了random_bits过来，发送了就是本地要保存的，否则就是要发送的
            if len(random_bits) > 0:
                count = self.request_dict["count"]
                self.logger.info("store random_bits")
                if ns != len(random_bits[0]):
                    self.return_parse_result(OPERATION_FAILED, \
                                             "Transmission errors.", {})
                    return
                f = open(random_bits_path, "a")
                for line in random_bits:
                    f.write(str(line) + "\n")
                f.close()
                # 如果randombits够了
                if count == 256:
                    self.logger.info("create a sender")
                    oprf = sender_oprf.sender_oprf(128)
                    with open(select_bit_path, "w") as f:
                        for i in oprf.select_bit:
                            f.write(str(i) + "\n")
                    f.close()
                    self.return_parse_result(SUCCESS, "get rand bits", {"is_ok": True})
                else:
                    self.return_parse_result(SUCCESS, "get rand bits", {"is_ok": False})
                return

            # 对方根据哈希函数和分片来寻求相应文件的哈希的结果
            split = self.request_dict["split"]
            self.logger.info("ask to get split %d" % split)
            hash_num = self.request_dict["hash_num"]
            f = open(random_bits_path, "r")
            random_bits_all = f.readlines()
            f.close()
            # 调整random_bits数据的格式
            self.logger.info("Start get random bits")
            int_random_bits_all = []
            random_bits_all = [list(i[:-1]) for i in random_bits_all]
            for i in random_bits_all:
                int_random_bits = []
                for j in i:
                    int_random_bits.append(int(j))
                int_random_bits_all.append(int_random_bits)
            random_bits_all = np.array(int_random_bits_all)
            self.logger.info("get random bits")
            # 恢复一个oprf类的初始操作
            self.logger.info("recover oprf")
            oprf = sender_oprf.sender_oprf(128)
            f = open(select_bit_path, "r")
            select_bits = f.readlines()
            f.close()
            oprf.select_bit = np.array([int(i[:-1], 2) for i in select_bits])
            oprf.get_F(random_bits_all)
            self.logger.info("recover oprf finished!")
            cuckoo_hash = sender_hash.sender_hash(n, s, 128, salt)
            hash_path = os.path.join(self.job_dir, "hash_dicts")
            file_sum = len(os.listdir(hash_path))
            result = []
            # 根据文件个数判读是不是应该要发送hash结果
            if split <= file_sum:
                words = self.get_hash_data(split)
                words = [int(i, 16) for i in words]
                self.logger.info("calculate split %d val" % split)
                for word in words:
                    h = cuckoo_hash.hash_functions[hash_num](word)
                    # 计算不经意函数值
                    val = oprf.oprf_eval(h, word)
                    val = val.tolist()
                    val = [str(i) for i in val]
                    result.append(''.join(val))
                if split < file_sum - 1:
                    # 还有文件
                    self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {"result": result, "is_ok": False})
                    self.logger.info("return split %d/%d" % (split, file_sum))
                else:
                    # 没有文件了
                    self.return_parse_result(SUCCESS, status_msg_dict[SUCCESS], {"result": result, "is_ok": True})
                    self.logger.info("return all split")
            self.logger.info("Interaction API finished.")
            return
        else:
            self.logger.error("Unsupported mpc method.")
            self.return_parse_result(OPERATION_FAILED, \
                                     "Unsupported mpc method.", {})
            return

    def get_hash_data(self, split):
        hash_dir = os.path.join(self.job_dir, "hash_dicts")
        if not os.path.exists(hash_dir):
            self.logger.error("hash dir is not exists!")
        file_list = os.listdir(hash_dir)
        if split > len(file_list) - 1:
            return None
        with open(os.path.join(hash_dir, file_list[split]), "r") as f:
            data_list = f.readlines()
        f.close()
        data_list = list(set(data_list))
        data_list = [i.split(',')[0] for i in data_list]
        return data_list
