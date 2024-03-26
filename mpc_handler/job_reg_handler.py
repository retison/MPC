import os
import time
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from config import local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from config import mpc_job_dir
from flow_control.mpc_arith.arith_flow_guest import ArithFlowGuest
from flow_control.mpc_substring.substring_flow_guest import SubtringFlowGuest
from flow_control.mpc_aggre.aggre_flow_guest import RSAFlowGuest
from mpc.hash_utils import data_parse_f2f
from mpc_handler.base import data_base_handler
from mpc_handler.utils import arith_operation, aggre_operation, other_operation, generate_prim
from utilities import logger
from utilities.database_manager import database_manager
from utilities.sql_template import get_mpc_job_insert_sql
from utilities.status_code import *
from utilities.utilities import get_log_file_handler, is_valid_variable_name, string_to_md5

# TODO 还有对limit/order by/group by 的
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
        logger.info("Start Decode Check.")
        if self.decode_check(["data_list", "mpc_method", "party_list"], [list, str, list]) is False:
            logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        logger.info("Decode Check Success.")
        logger.info(self.request_dict)
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
            logger.warning("Empty Data List.")
            self.return_parse_result(DATA_VALUE_ERROR, \
                                     status_msg_dict[DATA_VALUE_ERROR] + ": Empty Data List.", {})
            return
        for data_id in self.data_list:
            valid = is_valid_variable_name(data_id)
            if valid is False:
                logger.warning("Invalid Data ID.")
                self.return_parse_result(DATA_VALUE_ERROR, \
                                         status_msg_dict[DATA_VALUE_ERROR] + ": Invalid Data ID.", {})
                return
                # for each_data_id 结束
        logger.info("Data List check Success.")
        # 检查 job_id
        if "job_id" in self.request_dict.keys():
            self.job_id = self.request_dict["job_id"]
        else:
            self.job_id = self.generate_job_id()
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
        self.mpc_method = self.request_dict["mpc_method"]
        # 检查完毕，开始往db里写任务信息
        # 主要耗时在这里，但是还好
        self.dbm = database_manager(local_db_ip, local_db_port, \
                                    local_db_username, local_db_passwd, local_db_dbname)
        current_time = int(time.time())
        job_insert_sql = get_mpc_job_insert_sql(self.job_id, self.data_list, self.party_list, self.mpc_method, self.key,
                                                "registered", current_time, current_time)
        logger.debug("The job insert SQL is: %s." % job_insert_sql.replace("\n", " "))
        self.dbm.insert(job_insert_sql)
        # 检查状态之后返回
        if self.dbm.insert_success is True:
            self.return_parse_result(0, 'success', {"job_id": self.job_id})
        else:
            self.return_parse_result(OPERATION_FAILED, \
                                     status_msg_dict[OPERATION_FAILED] + ": job database insert failed", {})
        # 新开线程处理 md5
        logger.info("Response Calculation Finished.")
        if self.mpc_method in arith_operation:
            # TODO 算数运算
            self.execute_at_backend(self.hash_preprocessing)  # 后台执行
        elif self.mpc_method in aggre_operation:
            # TODO 聚合操作的
            self.execute_at_backend(self.rsa_preprocessing)
        elif self.mpc_method[:9] == "substring":
            self.execute_at_backend(self.substring_preprocessing)
        elif self.mpc_method in other_operation:
            # TODO limit\grop by\order by操作的
            self.execute_at_backend(self.ot_preprocessing)
        else:
            logger.warning("Method is not exist")
            exit(0)
        return

    def hash_preprocessing(self):
        logger.info("MPC method is: RSA, start preprocessing.")
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
            logger.error("Local data ID NOT found!")
            # 更新任务状态failed
            return
        logger.info("Local Data ID is %s" % self_data_id)
        # 检索到之后，再进行hash 处理
        logger.info("Start Hash Calculation.")
        # 直接套try执行
        try:
            parse_res = data_parse_f2f(self_data_id, self.job_id, logger)
        except:
            parse_res = False
        # 再更新任务状态
        if parse_res:
            self.change_job_status(self.job_id, "ready")
            # print(self.get_job_status(self.job_id))
        else:
            self.change_job_status(self.job_id, "failed")
        logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
        logger.info("Job ID %s ready." % self.job_id)

        # 再来，后续就是guest 调用hosts接口进行剩下的操作了！
        fcx = ArithFlowGuest(self.job_id)
        fcx.run_protocol()

    def rsa_preprocessing(self):
        logger.info("MPC method is: Hash, start preprocessing.")
        self.change_job_status(self.job_id, "running")
        if self.from_machine:
            # 找出属于自己的 data source
            disk_data_list = self.get_data_list()
            try:
                self_data_id = self.data_list[0]
            except:
                self_data_id = None
            # 查看是否检索到了 self data id
            if self_data_id is None or self_data_id not in disk_data_list:
                self.change_job_status(self.job_id, 'failed')
                logger.error("Local data ID NOT found!")
                # 更新任务状态failed
                return
            logger.info("Local Data ID is %s" % self_data_id)
            # 检索到之后，再进行hash 处理
            logger.info("Start Hash Calculation.")
            # 直接套try执行
            try:
                parse_res = data_parse_f2f(self_data_id, self.job_id, logger)
            except:
                parse_res = False
            # 再更新任务状态
            if parse_res:
                self.change_job_status(self.job_id, "ready")
                # print(self.get_job_status(self.job_id))
            else:
                self.change_job_status(self.job_id, "failed")
        else:
            self.change_job_status(self.job_id, "ready")
        logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
        logger.info("Job ID %s ready." % self.job_id)

        # 再来，后续就是guest 调用hosts接口进行剩下的操作了！
        fcx = RSAFlowGuest(self.job_id)
        fcx.run_protocol()

    def substring_preprocessing(self):
        logger.info("MPC method is: Hash, start preprocessing.")
        self.change_job_status(self.job_id, "running")
        if self.from_machine:
            # 找出属于自己的 data source
            disk_data_list = self.get_data_list()
            try:
                self_data_id = self.data_list[0]
            except:
                self_data_id = None
            # 查看是否检索到了 self data id
            if self_data_id is None or self_data_id not in disk_data_list:
                self.change_job_status(self.job_id, 'failed')
                logger.error("Local data ID NOT found!")
                # 更新任务状态failed
                return
            logger.info("Local Data ID is %s" % self_data_id)
            # 检索到之后，再进行hash 处理
            logger.info("Start Hash Calculation.")
            # 直接套try执行
            try:
                parse_res = data_parse_f2f(self_data_id, self.job_id, logger)
            except:
                parse_res = False
            # 再更新任务状态
            if parse_res:
                self.change_job_status(self.job_id, "ready")
            else:
                self.change_job_status(self.job_id, "failed")
        else:
            self.change_job_status(self.job_id, "ready")
        logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
        logger.info("Job ID %s ready." % self.job_id)

        fcx = SubtringFlowGuest(self.job_id)
        fcx.run_protocol()

    def ot_preprocessing(self):
        logger.info("MPC method is: Hash, start preprocessing.")
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
            logger.error("Local data ID NOT found!")
            # 更新任务状态failed
            return
        logger.info("Local Data ID is %s" % self_data_id)
        # 检索到之后，再进行hash 处理
        logger.info("Start Hash Calculation.")
        # 直接套try执行
        try:
            # parse_res = hash_parse_queue(self_data_id, self.job_id, logger)
            parse_res = data_parse_f2f(self_data_id, self.job_id, logger)
        except:
            parse_res = False
        # 再更新任务状态
        if parse_res:
            self.change_job_status(self.job_id, "ready")
            # print(self.get_job_status(self.job_id))
        else:
            self.change_job_status(self.job_id, "failed")
        logger.info("Job ID %s dir is %s" % (self.job_id, self.job_dir))
        logger.info("Job ID %s ready." % self.job_id)

        # 再来，后续就是guest 调用hosts接口进行剩下的操作了！
        fcx = SubtringFlowGuest(self.job_id)
        fcx.run_protocol()
