# 这个作为一个和 data 相关的基类进行开发
# 相应 url 的时候需要继承这个 base class
# 首先还是一堆 import 
import json
import logging.config
import os
import threading
import time
from json.decoder import JSONDecodeError

import tornado.web

from config import local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import logger_config_path
from config import mpc_data_dir
from config import mpc_job_dir
from utilities import logger
from utilities.database_manager import database_manager
from utilities.sql_template import get_s_party_list, change_mpc_job_status
from utilities.status_code import *

# 经过测试可用
logging.config.fileConfig(logger_config_path)
logger = logging.getLogger('FED_SQL')


class DataBaseHandler(tornado.web.RequestHandler):
    # 和 任务相关的 logger，使用 logger 进行处理
    def create_logger(self):
        import logging.config
        from config import logger_config_path
        # 经过测试可用
        logging.config.fileConfig(logger_config_path)
        tmstmp = str(time.time())
        self.logger = logging.getLogger('handler_self_logger' + tmstmp)

    def initialize(self, action, target=None) -> None:
        # print(action)
        if action not in action_list:
            logger.error("Wrong Action %s requested." % action)
            raise ValueError("Wrong Action %s requested." % action)
        self.action = action
        logger.info('API: `%s` Called' % action_method_dict[self.action])
        self.target = target
        if self.target is not None:
            logger.info("API target is %s" % self.target)

    def prepare(self):
        self.set_header('Content-Type', 'application/json;')
        self.set_header("Server", "MPC-Comp/Version-1.0")
        self.res_dict = {}  # 备用作为返回的dict，不一定用

    # 一个常用的方法
    def return_parse_result(self, code, msg, data):
        res_dict = {}
        res_dict['code'] = code
        res_dict['msg'] = msg
        res_dict['data'] = data
        self.write(res_dict)
        logger.info('API `%s` response returned.' % action_method_dict[self.action])
        return

        # 沿用了 sql check 的函数

    def decode_check(self, required_key_list, required_type_list):
        res_dict = {}
        try:
            request_dict = json.loads(self.request.body)
            logger.info("Json dict loaded.")
            request_key_list = list(request_dict.keys())
            # 不管是 reg 还是 del
            length = len(required_key_list)
            for i in range(length):
                current_key = required_key_list[i]
                logger.debug("Checking required key: %s." % current_key)
                if type(request_dict[current_key]) is not required_type_list[i]:
                    res_dict['code'] = DATA_TYPE_ERROR
                    res_dict['msg'] = 'Input Type Error: key `%s` has type %s' % (
                        current_key, type(request_dict[current_key]))
                    res_dict['data'] = {}
                    self.res_dict = res_dict
                    logger.error("Input Type Error, the request body is: " + str(self.request.body))
                    return False
                logger.debug("Required key: %s check OK" % current_key)
            self.request_dict = request_dict
            return True
        except JSONDecodeError:
            logger.warning("Occur Json Decode Error.")
            res_dict['code'] = REQUEST_DECODE_ERROR
            res_dict['msg'] = status_msg_dict[REQUEST_DECODE_ERROR]
            res_dict['data'] = {}
            self.res_dict = res_dict
            logger.error("Json Decode Error: " + str(self.request.body))
            return False
        except KeyError as e:
            logger.warning(
                "Occur Key Error in Calculation Handler: %s, got key list: %s." % (str(e), str(request_key_list)))
            res_dict['code'] = KEY_ERROR
            res_dict['msg'] = 'Missing at Least 1 Request Key: %s, got key-list: %s' % (str(e), str(request_key_list))
            res_dict['data'] = {}
            self.res_dict = res_dict
            logger.error("Json Key Error, the request body is: " + str(self.request.body))
            return False
        # decode_check 函数结束

    def get_data_list(self):
        # code here ...
        file_list = os.listdir(mpc_data_dir)
        res_file_list = []
        for i in file_list:
            if '.' not in i:
                res_file_list.append(i)
        return res_file_list

    # 获取 party（参与方） 的列表
    def get_party_list(self):
        if self.dbm is not None:
            local_dbm = self.dbm
        else:
            local_dbm = database_manager(local_db_ip, local_db_port, \
                                         local_db_username, local_db_passwd, local_db_dbname)
        # 然后开始查询
        sql = get_s_party_list()
        select_res = local_dbm.query(sql)
        # print(select_res) # [[0, 'LEAGUE_CREATOR'], [2, 'PARTNER2'], [3240, '#sdafdfasf']]
        length = len(select_res)
        party_id_list = []
        party_name_list = []
        for i in range(length):
            party_id_list.append(select_res[i][0])
            party_name_list.append(select_res[i][1])
        return party_id_list, party_name_list

    # 单开线程执行
    def execute_at_backend(self, input_func, input_args=()):
        t = threading.Thread(target=input_func, args=input_args)
        t.daemon = True
        t.start()

    # 更改任务状态
    def change_job_status(self, job_id, status):
        if self.dbm is not None:
            local_dbm = self.dbm
        else:
            local_dbm = database_manager(local_db_ip, local_db_port, \
                                         local_db_username, local_db_passwd, local_db_dbname)
        # 获得sql
        change_status_sql = change_mpc_job_status(job_id, status)
        # 执行更改状态
        local_dbm.insert(change_status_sql)
        return local_dbm.insert_success

    # 获取任务状态
    def get_job_status(self, job_id=None):
        if self.job_id is not None:
            job_id = self.job_id
        elif job_id is None:
            logger.error("Job id not specified")
            return "failed"
        query_sql = """
            SELECT status FROM %s.b_mpc_job 
            WHERE job_id = "%s";  """ % (local_db_dbname, job_id)
        # 准备查询数据库
        local_dbm = database_manager(local_db_ip, local_db_port, \
                                     local_db_username, local_db_passwd, local_db_dbname)
        try:
            status_res = local_dbm.query(query_sql)
        except:
            logger.error("Failed to execute query: %s" % query_sql)
            return "Failed"
        result = status_res[0][0]
        logger.info("Get job Status query result: %s", result)
        return result

    # 从 job_id 中提取 mpc_method
    def get_mpc_method(self):
        query_sql = '''SELECT mpc_method FROM %s.b_mpc_job where job_id = "%s";''' % (local_db_dbname, self.job_id)
        # 准备查询数据库
        local_dbm = database_manager(local_db_ip, local_db_port, \
                                     local_db_username, local_db_passwd, local_db_dbname)
        try:
            status_res = local_dbm.query(query_sql)
        except:
            logger.error("Failed to execute query: %s" % query_sql)
            return "Failed"
        result = status_res[0][0]
        logger.info("Get job method query result: %s", result)
        return result

    # 检查 job_id 是否存在
    def check_job_id_exists(self):
        # 生成 job_id 的 path
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        if not os.path.exists(self.job_dir):
            return False
        else:
            return True

    def get_job_key(self, job_id):
        query_sql = '''SELECT job_key FROM %s.b_mpc_job where job_id = "%s";''' % (local_db_dbname, job_id)
        # 准备查询数据库
        local_dbm = database_manager(local_db_ip, local_db_port, \
                                     local_db_username, local_db_passwd, local_db_dbname)
        try:
            status_res = local_dbm.query(query_sql)
        except:
            logger.error("Failed to execute query: %s" % query_sql)
            return "Failed"
        result = status_res[0][0]
        logger.info("Get job key query result: %s", result)
        return result
