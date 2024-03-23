import os
# from utilities import global_var as global_dict
# from utilities.database_manager import database_manager
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import mpc_data_dir
from mpc_handler.base import data_base_handler
from utilities import logger
from utilities.status_code import *
from utilities.utilities import is_valid_variable_name


class DataDelHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=4)

    @run_on_executor
    def post(self):
        logger.info("Start Decode Check.")
        if self.decode_check(["data_id"], [str]) is False:
            logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        logger.info("Decode Check Success.")
        # 检查一下 data id 
        data_id = self.request_dict["data_id"]
        valid = is_valid_variable_name(data_id)
        if valid is False:
            logger.warning("Invalid Data ID.")
            self.return_parse_result(DATA_VALUE_ERROR,\
                status_msg_dict[DATA_VALUE_ERROR] + ": Invalid Data ID.", {})
            return 
        logger.info("Data ID check Success.")
        # 基本上检查完毕，开始写入数据
        
        file_path = os.path.join(mpc_data_dir, data_id+".csv")
        file_md5_path = file_path.replace(".csv", ".md5.csv")

        try:
            os.remove(file_path)
            logger.info("File delete Success.")
            os.remove(file_md5_path)
            logger.info("MD5 File delete Success.")
        except Exception as e:
            logger.warning("Error occur when delete files: %s" % e)
        # 如果是 dh 算法（method = 2），还需要返回public key 
        # 目前先搞基于 hash 的
        self.return_parse_result(0, 'success', {})
        # 后面可以单开线程进行计算等内容
        return 

        
        