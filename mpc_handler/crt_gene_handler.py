import argparse
import os
from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor

from mpc_handler.base import data_base_handler
from utilities import logger

from utilities.generate_certificates import create_key, create_request, create_certificate, save_key, save_certificate


class CerGenerateHandler(data_base_handler.DataBaseHandler):
    executor = ThreadPoolExecutor(max_workers=4)

    @run_on_executor
    def post(self):
        logger.info("Start generate crt.")
        parser = argparse.ArgumentParser()
        parser.add_argument('-p', '--prefix',
                            help='output filename prefix')
        parser.add_argument('-k', '--key-size', type=int,
                            help='key size')
        parser.add_argument('-m', '--parties', dest='m', type=int,
                            help='number of parties')
        parser.set_defaults(key_size=2048, prefix='party_')
        options = parser.parse_args()
        logger.debug("Initialization finish")
        # self-signed CA certificate
        ca_key = create_key(options.key_size)
        ca_csr = create_request(ca_key, 'MPyC Certification Authority')
        ca_crt = create_certificate(ca_csr, ca_csr, ca_key, 1)
        if not os.path.exists("config"):
            os.mkdir("config")
        save_key(ca_key, 'config/mpyc_ca.key')
        save_certificate(ca_crt, 'config/mpyc_ca.crt')
        self.return_parse_result(0, 'success', {})
        return 

        
        