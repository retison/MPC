import random
import os
import random
# from utilities import global_var as global_dict
# from utilities.database_manager import database_manager
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import mpc_data_dir, split_count
from mpc_handler.base import data_base_handler
from utilities import logger
from utilities.status_code import *
from utilities.utilities import is_valid_variable_name


class DataImportHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=16)

    @run_on_executor
    def post(self):
        logger.info("Start Decode Check.")
        if self.decode_check(["data_id", "id_list"], [str, list]) is False:
            logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        logger.info("Decode Check Success.")
        self.data_id = self.request_dict["data_id"]
        valid = is_valid_variable_name(self.data_id)
        if valid is False:
            logger.warning("Invalid Data ID.")
            self.return_parse_result(DATA_VALUE_ERROR,\
                status_msg_dict[DATA_VALUE_ERROR] + ": Invalid Data ID.", {})
            return 
        logger.info("Data ID check Success.")
        # 基本上检查完毕，开始写入数据
        self.data_list = self.request_dict['id_list']
        # 这里就要去重复！ 
        # TODO 调试时候去掉，后面再加回来
        self.data_list = list(set(self.data_list))
        # 重新计算长度
        self.data_length = len(self.data_list)
        if self.data_length <= split_count * 10:
            logger.info("Start write without split.")
            self.write_data_to_disk()
            logger.info("Write to disk without split success.")
        else:
            logger.info("Start write with split.")
            self.write_data_with_split()
            logger.info("Write Success.")
        self.return_parse_result(0, 'success', {})
        return 
    
    def write_data_with_split(self):
        thread_length = int(self.data_length / split_count) 
        # 这个先顺序写入匹配结构吧
        # process_name = "MPC_Hash_Process-" + str(i+1)
        # print(thread_name)
        for i in range(split_count):
            logger.info("Split %d start." % (i+1) )
            if i == split_count - 1:
                st_index = thread_length * i
                ed_index = self.data_length
            else:
                st_index = i*thread_length
                ed_index = (i+1)*thread_length
            logger.debug("Index info: st_index={}, ed_index={}".format(st_index, ed_index))
            local_data = self.data_list[st_index:ed_index]
            # logger.debug("Split data preview: %s ..." %(str(local_data) [:200] ))
            self.write_data_to_disk(index = i, st_index = st_index, ed_index = ed_index)
            logger.info("Split %d success." % (i+1) )
        logger.info("Write to disk finished.")

    def write_data_to_disk(self, index = None, st_index = None, ed_index = None):
        # 一次注册的数据量过小时
        # 随机放进一个分片中
        if index is None:
            index = random.choice(list(range(split_count))) 
        if st_index is None:
            st_index = 0
            ed_index = self.data_length
        data = self.data_list[st_index: ed_index]
        # 确定文件路径
        data_dir = os.path.join(mpc_data_dir, self.data_id)
        if not os.path.exists(data_dir): os.makedirs(data_dir)
        # 这里 index + 1 是为了和线程对应起来
        file_path = os.path.join(data_dir, "data_split_" + str(index+1)+".csv" )
        logger.info("Split %d to path: %s"%(index +1, file_path))
        logger.info("Split %d length: %d."%(index +1, len(data)))
        f = open(file_path, 'a')
        # new_string = '\n'.join(data) + '\n'
        new_string = ""
        for i in data:
            new_string = new_string + str(i) + '\n'
        f.write(new_string)
        f.close()
        

        
        