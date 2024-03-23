import json
import os
import time

import requests

from config import local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from utilities.database_manager import database_manager
from utilities.sql_template import change_mpc_job_status
from utilities.utilities import get_log_file_handler


# 这个class 作为最初的基类进行开发
# 后面所有跟 flow 、算法相关的模块，都可以继承这个类型
# 这个类型除了定义
class FlowControllerBase(object):
    def __init__(self, job_id = None):
        self.party_info_cache_dict = {}
        self.ready = True
        if job_id is not None:
            self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        self.job_id = job_id
    
    def parameter_check(self):
        # self.job_id = job_id
        # 把一些需要链接以及日志的内容
        # 从 __init__ 中拆出来
        self.create_logger()
        # 如果有 job_id，取一些必要信息
        if self.job_id is not None:
            operation_res = self.update_job_party_list()
            if operation_res is False:
                self.logger.error('Get party_list failed, please check your job_id.')
            #  增加 current intersection 的目录
            self.current_intersection_dir = os.path.join(mpc_job_dir, self.job_id, "intersection")
            if not os.path.exists(self.job_dir):
                self.logger.error("Job Dir not exist!")
            # hash dir
            self.hash_dir = os.path.join(self.job_dir, "data_dicts")
        else:
            self.logger.warning("Job id is not specified.")
        pass
    '''
    @property
    def job_id(self):
        return self.job_id
    
    @job_id.setter
    def job_id(self, value):
        self.job_id = value
    
    @job_id.deleter
    def job_id(self):
        self.job_id = None
    '''
    
    # 和 任务相关的 logger，使用 self.logger 进行处理
    def create_logger(self):
        import logging.config
        from config import logger_config_path
        # 经过测试可用
        logging.config.fileConfig(logger_config_path)
        tmstmp = str(time.time())
        self.logger = logging.getLogger('flow_controller_logger' + tmstmp)
        # 这里还需要增加针对 job id 的 logger 
        if self.job_id is not None:
            job_log_path = os.path.join(self.job_dir, "mpc_application.log")
            self.job_log_handler = get_log_file_handler(job_log_path)
            self.logger.addHandler(self.job_log_handler)
            self.logger.info("Flow controller logging in.")
            pass
    
    # 创建数据库链接
    def create_dbm(self):
        dbm = database_manager(local_db_ip,local_db_port,\
                      local_db_username, local_db_passwd, local_db_dbname)
        if dbm.conn_status != "success":
            self.logger.warning("Local Database Connection Failed: %s" % dbm.conn_status)
            self.ready = False
        return dbm
    
    # 给定ID，获取 party 的链接信息（名称、ip，端口）
    def get_party_info(self, party_id):
        assert self.ready is True
        assert type(party_id) is int
        assert party_id >= 0 

        dbm = self.create_dbm()

        if party_id in self.party_info_cache_dict.keys():
            self.logger.info("Return party info for party %s from cache." % (str(party_id)))
            return self.party_info_cache_dict[party_id]
        res = {}
        res['party_id'] = party_id
        select_sql = '''
            SELECT party_name, party_mpc_ip, party_mpc_port FROM %s.s_party where id = %d;''' % (local_db_dbname, party_id)
        query_res = dbm.query(select_sql)
        if len(query_res) == 0:
            return res
        query_res = query_res[0]
        res['party_name'] = query_res[0]
        res['party_mpc_ip'] = query_res[1]
        res['party_mpc_port'] = query_res[2]
        # 存 cache 
        self.party_info_cache_dict[party_id] = res
        return res 
    
    # 向 DB 查询 job party 以及 mpc method 
    # 同时更新到列变量中
    def update_job_party_list(self):
        assert self.ready is True
        assert self.job_id is not None
        dbm = self.create_dbm()
        select_sql = '''
            SELECT job_party, mpc_method FROM %s.b_mpc_job WHERE job_id = "%s";''' \
                %(local_db_dbname, self.job_id)
        query_res = dbm.query(select_sql)
        if len(query_res) == 0:
            return False
        query_res = query_res[0]
        # 取结果
        self.mpc_method = query_res[1]
        query_res[0] = query_res[0].replace("\'","\"")
        self.party_list = json.loads(query_res[0]) # tyoe 是 list
        if "NOT_SHOWN_TO_HOST" in self.party_list:
            self.party_list.remove('NOT_SHOWN_TO_HOST')
        self.logger.info("The job party_list is: %s", self.party_list)
        self.logger.info("The job mpc_method is: %s", self.mpc_method)
        return True # 成功返回 True
            
    def send_restful_request(self, url, request_body, timeout = 10, retry = 40):
        assert self.ready is True
        success = False
        try_count = 0
        res = "{}"
        while not success and try_count < retry:
            # 这里循环 x
            try_count += 1
            try:
                r = requests.post(url, data = json.dumps(request_body), timeout = timeout)
                self.logger.info("Request %s success, count = %d"% (url, try_count))
                success = True
                res = r.text
                self.logger.info("Response: %s" % res[:200] )
            except requests.ConnectionError:
                self.logger.warning("Request %s failed (ConnectionError), try_count = %d"\
                    % (url, try_count)) 
                time.sleep(1.5)
            except requests.ConnectTimeout:
                self.logger.warning("Request %s failed (ConnectTimeout), try_count = %d" % (url, try_count))  
            # self.logger.info("Try Count %d, Success: %s" % (try_count, success))
            pass 
        # 搞定，返回是否成功 + 内容即可
        print(success,res)
        return success, json.loads(res)

    # 待完善
    def send_grpc_request(self):
        raise NotImplementedError("GRPC not implemented.")
    
    @staticmethod
    def assemble_url(ip, port, method):
        return 'http://' + ip + ':' + str(port) + method

    # 更改任务状态
    def change_job_status(self, status):
        job_id = self.job_id
        local_dbm = database_manager(local_db_ip,local_db_port,\
                      local_db_username, local_db_passwd, local_db_dbname)
        # 获得sql
        change_status_sql = change_mpc_job_status(job_id, status)
        # 执行更改状态
        local_dbm.insert(change_status_sql)
        return local_dbm.insert_success        
    
    # 查询现在的任务状态 
    def get_job_status(self):
        query_sql = """
            SELECT status FROM %s.b_mpc_job 
            WHERE job_id = "%s";  """ % (local_db_dbname, self.job_id)
        # 准备查询数据库
        local_dbm = database_manager(local_db_ip,local_db_port,\
                      local_db_username, local_db_passwd, local_db_dbname)
        try:
            status_res = local_dbm.query(query_sql)
        except:
            self.logger.error("Failed to execute query: %s" % query_sql)
            return "Failed" 
        result = status_res[0][0]
        self.logger.info("Get job Status query result: %s", result)
        return result

    # 查询现在的任务数据
    def get_job_data(self):
        query_sql = """
            SELECT job_data FROM %s.b_mpc_job 
            WHERE job_id = "%s";  """ % (local_db_dbname, self.job_id)
        # 准备查询数据库
        local_dbm = database_manager(local_db_ip,local_db_port,\
                      local_db_username, local_db_passwd, local_db_dbname)
        try:
            status_res = local_dbm.query(query_sql)
        except:
            self.logger.error("Failed to execute query: %s" % query_sql)
            return "Failed" 
        result = status_res[0][0]
        self.logger.info("Get job data query result: %s", result)
        return result
    
    # 检查 job_id 的 DIR 是否存在
    # 这里不从 db 里面查，而是从 
    def check_job_id_exists(self):
        # 生成 job_id 的 path 
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        if not os.path.exists(self.job_dir):
            return False
        else:
            return True
    pass

    def get_job_key(self):
        query_sql = '''SELECT job_key FROM %s.b_mpc_job where job_id = "%s";''' % (local_db_dbname, self.job_id)
        # 准备查询数据库
        local_dbm = database_manager(local_db_ip,local_db_port,\
                      local_db_username, local_db_passwd, local_db_dbname)
        try:
            status_res = local_dbm.query(query_sql)
        except:
            self.logger.error("Failed to execute query: %s" % query_sql)
            return "Failed"
        result = status_res[0][0]
        self.logger.info("Get job key query result: %s", result)
        return result
# x = FlowControllerBase()