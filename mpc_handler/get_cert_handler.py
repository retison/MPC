import os
from concurrent.futures import ThreadPoolExecutor
from cryptography.hazmat.primitives import serialization
from tornado.concurrent import run_on_executor

from config import local_db_ip, local_db_username, local_db_passwd, local_db_dbname, local_db_port
from mpc_handler.base import data_base_handler
from utilities import logger
from utilities.database_manager import database_manager
from utilities.generate_certificates import create_request, create_key, create_certificate, save_key, save_certificate
from utilities.status_code import OPERATION_FAILED


class GetCertHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=8)

    def create_dbm(self):
        dbm = database_manager(local_db_ip, local_db_port, \
                               local_db_username, local_db_passwd, local_db_dbname)
        if dbm.conn_status != "success":
            self.logger.warning("Local Database Connection Failed: %s" % dbm.conn_status)
            self.ready = False
        return dbm

    @run_on_executor
    def post(self):
        if self.decode_check(["party_id"], [int]) is False:
            logger.info("Decode Check Failed.")
            self.write(self.res_dict)
            return
        party_id = self.request_dict["party_id"]
        # TODO 读取id是否存在，存在创建并返回，否则返回失败,测试时可以不用
        assert party_id >= 0
        dbm = self.create_dbm()
        select_sql = '''
                    SELECT party_name FROM %s.s_party where id = %d;''' % (
            local_db_dbname, party_id)
        query_res = dbm.query(select_sql)
        if len(query_res) == 0:
            self.return_parse_result(OPERATION_FAILED, \
                                     "Requested party_id NOT exist.", {"requested_party_id": party_id})
            return
        with open('config/mpyc_ca.crt', "rb") as f:
            ca_crt_data = f.read()
        f.close()
        with open('config/mpyc_ca.key', "rb") as f:
            ca_key_data = f.read()
        f.close()
        ca_key = serialization.load_pem_private_key(ca_key_data, password=None)
        ca_csr = create_request(ca_key, 'MPyC Certification Authority')
        party_key = create_key(2048)
        party_csr = create_request(party_key, f'MPyC party {party_id}')
        serial_base = 2 ** 16 * 5 ** 5  # several trailing zeros in bin/oct/dec/hex representations
        party_crt = create_certificate(party_csr, ca_csr, ca_key, serial_base + party_id)
        if not os.path.exists(f"config/party_{party_id}"):
            os.mkdir(f"config/party_{party_id}")
        save_key(party_key, f'config/party_{party_id}/party_{party_id}.key')
        save_certificate(party_crt, f'config/party_{party_id}/party_{party_id}.crt')
        with open(f'config/party_{party_id}/party_{party_id}.key', "rb") as f:
            party_key = f.read()
        f.close()
        with open(f'config/party_{party_id}/party_{party_id}.crt', "rb") as f:
            party_crt = f.read()
        f.close()
        logger.info("get CA certification!")
        self.return_parse_result(0, 'success', {"ca_crt": ca_crt_data.decode(),
                                                "party_key": party_key.decode(), "party_crt": party_crt.decode()})
        return
