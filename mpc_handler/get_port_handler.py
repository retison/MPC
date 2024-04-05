import os
import random
import socket
from concurrent.futures import ThreadPoolExecutor
from cryptography.hazmat.primitives import serialization
from tornado.concurrent import run_on_executor

from config import local_db_ip, local_db_username, local_db_passwd, local_db_dbname, local_db_port
from mpc_handler.base import data_base_handler
from utilities import logger
from utilities.database_manager import database_manager
from utilities.generate_certificates import create_request, create_key, create_certificate, save_key, save_certificate
from utilities.status_code import OPERATION_FAILED

class PortGetHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=8)

    @run_on_executor
    def post(self):
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sock.bind(("localhost",0))
            sock.listen(1)
            port = sock.getsockname()[1]
            sock.close()
            logger.info(f"free port is {port}")
            self.return_parse_result(0, 'success', {"port": port})
            return
        except:
            self.return_parse_result(OPERATION_FAILED, "There is not free port", {})
            return


