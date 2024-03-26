import os
# from utilities import global_var as global_dict
# from utilities.database_manager import database_manager
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import mpc_job_dir
from mpc_handler.base import data_base_handler
from utilities import logger
from utilities.status_code import *


# 这里不需要 split
# 因为自己持有的数据已经 split 了，每个核处理这个所有数据即可
class IntermediateImportHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=4)

    @run_on_executor
    def post(self):
        logger.info("Start Decode Check.")
        if self.decode_check(["job_id", "res_list", "party"], [str, list, int]) is False:
            logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        logger.info("Decode Check Success.")
        # 理论上 party 后续也需要做一些必要的输入检查
        self.party = self.request_dict["party"]
        self.job_id = self.request_dict["job_id"]
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        self.mpc_method = self.extract_mpc_method_from_job_id() 
        print(self.mpc_method)
        if self.mpc_method == 0:
            self.return_parse_result(OPERATION_FAILED,\
                "Wrong mpc_method.", {"requested_job_id": self.job_id})
            return 
        self.logger.info("Get mpc_method: %d." %self.mpc_method)
        # 基本上检查完毕，开始写入数据
        self.data_list = self.request_dict['res_list']
        # 这里就要去重复！
        self.data_list = list(set(self.data_list))
        # 重新计算长度
        self.data_length = len(self.data_list)
        if self.data_length == 0:
            self.logger.warning("Empty input list detected.")
            self.return_parse_result(OPERATION_FAILED,\
                "Empty input list detected.", {"requested_job_id": self.job_id})
            return 
        # 开始写数据
        self.return_parse_result(0, 'success', {})
        return 

    def write_data_to_disk(self):
        # 一次注册的数据量过小时
        # 随机放进一个分片中
        # 确定文件路径
        data_dir = os.path.join(mpc_job_dir, self.job_id, "output_input")
        if not os.path.exists(data_dir): os.makedirs(data_dir)
        # 这里 index + 1 是为了和线程对应起来
        file_path = os.path.join(data_dir, "input_%d" % self.party )
        # 这里注意采用的是追加写，方便多次写
        # 数据量巨大的时候，可以分批完成
        f = open(file_path, 'a')
        # new_string = '\n'.join(data) + '\n'
        input_string = ""
        for i in self.data_list:
            input_string = input_string + str(i) + '\n'
        f.write(input_string)
        f.close()
        

        
        