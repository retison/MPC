import os
import random
import socket
from concurrent.futures import ThreadPoolExecutor
from cryptography.hazmat.primitives import serialization
from tornado.concurrent import run_on_executor

from config import local_db_ip, local_db_username, local_db_passwd, local_db_dbname, local_db_port
from mpc_handler.base import data_base_handler
from utilities.database_manager import database_manager
from utilities.generate_certificates import create_request, create_key, create_certificate, save_key, save_certificate
from utilities.status_code import OPERATION_FAILED

class PortGetHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=8)

    @run_on_executor
    def post(self):
        if self.decode_check(["job_id"], [str]) is False:
            self.logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        job_id = self.request_dict["job_id"]
        select_sql = '''
                    SELECT job_id FROM %s.b_mpc_job where job_id = "%s";''' % (
            local_db_dbname, job_id)
        self.dbm = database_manager(local_db_ip, local_db_port,
                                    local_db_username, local_db_passwd, local_db_dbname)
        query_res = self.dbm.query(select_sql)
        if len(query_res) == 0:
            self.return_parse_result(OPERATION_FAILED,
                                     "Requested job_id NOT exist.", {"job_id": job_id})
            return
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sock.bind(("localhost",0))
            sock.listen(1)
            port = sock.getsockname()[1]
            sock.close()
            self.return_parse_result(0, 'success', {"port": port})
            return
        except:
            self.return_parse_result(OPERATION_FAILED, "There is not free port", {"job_id": job_id})
            return


