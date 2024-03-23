import os
import time
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from flow_control.mpc_hash.hash_flow_guest import HashFlowGuest
from flow_control.mpc_ot.ot_flow_guest import OTFlowGuest
from flow_control.mpc_rsa.rsa_flow_guest import RSAFlowGuest
from mpc.hash_utils import hash_parse_f2f, rsa_parse_f2f
from mpc.protocol import rsa
from mpc.utils import get_rsa_key
from mpc_handler.base import data_base_handler
from mpc_handler.utils import arith_operation, aggre_operation, other_operation, generate_prim
# from utilities import global_var as global_dict
from utilities.database_manager import database_manager
from utilities.sql_template import get_mpc_job_insert_sql
from utilities.status_code import *
from utilities.utilities import get_log_file_handler, is_valid_variable_name, string_to_md5


class JobRegHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=4)

    def generate_job_id(self):
        time_str = time.strftime("%Y%m%d%H%M%S", time.localtime()) + str(time.time()).split('.')[-1][:3]
        sorted_data_list = sorted(self.data_list)
        data_list_str = "@".join(sorted_data_list)
        md5_str = string_to_md5(data_list_str)
        mpc_method = self.request_dict["mpc_method"]
        # 这里 mpc_method 的已经有单独的字段进行表示了
        # 我们把 mpc method 放在中间，后面有的时候还是非常方便地取用的
        return time_str + "_" + md5_str[:6]

    @run_on_executor
    def post(self):
        self.create_logger()
        self.logger.info("Start Decode Check.")
        if self.decode_check(["data_list", "mpc_method", "party_list"], [list, str, list]) is False:
            self.logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        self.logger.info("Decode Check Success.")
        self.logger.info(self.request_dict)
        # 获取特殊参数
        try:
            self.from_machine = self.request_dict["from_machine"]
        except:
            self.from_machine = False
        # party id 的检查
        self.party_list = self.request_dict["party_list"]
        # 只有这种情况下，才进行检查
        if 0 not in self.party_list and self.from_machine is False:
            self.return_parse_result(DATA_VALUE_ERROR, \
                                     status_msg_dict[DATA_VALUE_ERROR] + ": party list should contain 0  ", {})
            return
            # 检查 data list 以及 data id 内容
        self.data_list = self.request_dict["data_list"]
        self.length = len(self.data_list)
        if self.length == 0:
            self.logger.warning("Empty Data List.")
            self.return_parse_result(DATA_VALUE_ERROR, \
                                     status_msg_dict[DATA_VALUE_ERROR] + ": Empty Data List.", {})
            return
        for data_id in self.data_list:
            valid = is_valid_variable_name(data_id)
            if valid is False:
                self.logger.warning("Invalid Data ID.")
                self.return_parse_result(DATA_VALUE_ERROR, \
                                         status_msg_dict[DATA_VALUE_ERROR] + ": Invalid Data ID.", {})
                return
                # for each_data_id 结束
        self.logger.info("Data List check Success.")
        # 检查 job_id
        if "job_id" in self.request_dict.keys():
            self.job_id = self.request_dict["job_id"]
        else:
            self.job_id = self.generate_job_id()
        if "key" in self.request_dict.keys():
            self.key = self.request_dict["key"]
        else:
            self.key = generate_prim()
        self.logger.info("The job_id is: %s." % self.job_id)
        self.job_dir = os.path.join(mpc_job_dir, self.job_id)
        # 创建专属文件夹
        if not os.path.exists(self.job_dir): os.makedirs(self.job_dir)
        # 增加日志的 handler 
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")

        self.job_log_handler = get_log_file_handler(job_log_path)
        self.logger.addHandler(self.job_log_handler)
        self.logger.info("MPC Job %s created." % self.job_id)
        self.mpc_method = self.request_dict["mpc_method"]
        # 检查完毕，开始往db里写任务信息
        # 主要耗时在这里，但是还好
        self.dbm = database_manager(local_db_ip, local_db_port, \
                                    local_db_username, local_db_passwd, local_db_dbname)
        current_time = int(time.time())
        job_insert_sql = get_mpc_job_insert_sql(self.job_id, self.data_list, self.party_list, self.mpc_method, self.key,
                                                "registered", current_time, current_time)
        self.logger.debug("The job insert SQL is: %s." % job_insert_sql.replace("\n", " "))
        self.dbm.insert(job_insert_sql)
        # 检查状态之后返回
        if self.dbm.insert_success is True:
            self.return_parse_result(0, 'success', {"job_id": self.job_id})
        else:
            self.return_parse_result(OPERATION_FAILED, \
                                     status_msg_dict[OPERATION_FAILED] + ": job database insert failed", {})
        # 新开线程处理 md5
        self.logger.info("Response Calculation Finished.")
        if self.mpc_method in arith_operation:
            # TODO 算数运算
            self.execute_at_backend(self.hash_preprocessing)  # 后台执行
        elif self.mpc_method in aggre_operation:
            # TODO 聚合操作的
            self.execute_at_backend(self.rsa_preprocessing)
        elif self.mpc_method in other_operation:
            # TODO limit\grop by\order by操作的
            self.execute_at_backend(self.ot_preprocessing)
        else:
            self.logger.warning("Method is not exist")
            exit(0)
        return

    def hash_preprocessing(self):
        self.logger.info("MPC method is: Hash, start preprocessing.")
        self.change_job_status(self.job_id, "running")
        # 找出属于自己的 data source 
        disk_data_list = self.get_data_list()
        try:
            self_data_id = self.data_list[0]
        except:
            self_data_id = None
        # 查看是否检索到了 self data id
        if self_data_id is None or self_data_id not in disk_data_list:
            self.change_job_status(self.job_id, 'failed')
            self.logger.error("Local data ID NOT found!")
            # 更新任务状态failed
            return
        self.logger.info("Local Data ID is %s" % self_data_id)
        # 检索到之后，再进行hash 处理
        self.logger.info("Start Hash Calculation.")
        # 直接套try执行
        try:
            # parse_res = hash_parse_queue(self_data_id, self.job_id, self.logger)
            parse_res = hash_parse_f2f(self_data_id, self.job_id, self.logger)
        except:
            parse_res = False
        # 再更新任务状态
        if parse_res:
            self.change_job_status(self.job_id, "ready")
            # print(self.get_job_status(self.job_id))
        else:
            self.change_job_status(self.job_id, "failed")
        self.logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
        self.logger.info("Job ID %s ready." % self.job_id)

        # 再来，后续就是guest 调用hosts接口进行剩下的操作了！
        fcx = HashFlowGuest(self.job_id)
        fcx.run_protocol()

    def rsa_preprocessing(self):
        self.logger.info("MPC method is: RSA, start preprocessing.")
        self.change_job_status(self.job_id, "running")
        # 找出属于自己的 data source
        disk_data_list = self.get_data_list()
        try:
            # 这里更改了一下 self data id 的赋值流程
            self_data_id = self.data_list[0]
        except:
            self_data_id = None
        # 查看是否检索到了 self data id
        if self_data_id is None or self_data_id not in disk_data_list:
            self.change_job_status(self.job_id, 'failed')
            self.logger.error("Local data ID NOT found!")
            # 更新任务状态failed
            return
        self.logger.info("Local Data ID is %s" % self_data_id)
        # 检索到之后，再进行hash 处理
        self.logger.info("Start Hash Calculation.")
        # 直接套try执行
        try:
            # 生成哈希将各种类型数据转化为数字
            parse_res = rsa_parse_f2f(self_data_id, self.job_id, self.logger)
        except:
            parse_res = False
        # 再更新任务状态
        if parse_res and 'NOT_SHOWN_TO_HOST' not in self.party_list:
            self.change_job_status(self.job_id, "ready")
            self.logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
            self.logger.info("Job ID %s ready." % self.job_id)
            # print(self.get_job_status(self.job_id))
        elif not parse_res:
            self.change_job_status(self.job_id, "failed")
            self.logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
            self.logger.info("Job ID %s ready." % self.job_id)

        fcx = RSAFlowGuest(self.job_id)
        fcx.run_protocol()
        if 'NOT_SHOWN_TO_HOST' in self.party_list:
            self.change_job_status(self.job_id, "ready")
            self.logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
            self.logger.info("Job ID %s ready." % self.job_id)

    def ot_preprocessing(self):
        self.logger.info("MPC method is: Hash, start preprocessing.")
        self.change_job_status(self.job_id, "running")
        # 找出属于自己的 data source
        disk_data_list = self.get_data_list()
        try:
            # 这里更改了一下 self data id 的赋值流程
            self_data_id = self.data_list[0]
        except:
            self_data_id = None
        # 查看是否检索到了 self data id
        if self_data_id is None or self_data_id not in disk_data_list:
            self.change_job_status(self.job_id, 'failed')
            self.logger.error("Local data ID NOT found!")
            # 更新任务状态failed
            return
        self.logger.info("Local Data ID is %s" % self_data_id)
        # 检索到之后，再进行hash 处理
        self.logger.info("Start Hash Calculation.")
        # 直接套try执行
        try:
            # parse_res = hash_parse_queue(self_data_id, self.job_id, self.logger)
            parse_res = hash_parse_f2f(self_data_id, self.job_id, self.logger)
        except:
            parse_res = False
        # 再更新任务状态
        if parse_res:
            self.change_job_status(self.job_id, "ready")
            # print(self.get_job_status(self.job_id))
        else:
            self.change_job_status(self.job_id, "failed")
        self.logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
        self.logger.info("Job ID %s ready." % self.job_id)

        # 再来，后续就是guest 调用hosts接口进行剩下的操作了！
        fcx = OTFlowGuest(self.job_id)
        fcx.run_protocol()

    # 这个操作已经不需要了
    '''
    def cancel_log_handler(self):
        # from utilities import logger
        self.logger.info("Before job log Handler removed.")
        self.logger.removeHandler(self.job_log_handler)
        # 先用 sleep 10秒 代替，后面可以轮询查询
        time.sleep(10)
        self.logger.info("Job log Handler removed.")
        pass
    '''
