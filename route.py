from mpc_handler.AggreHandler import AggreHandler
from mpc_handler.ArithHandler import ArithHandler
from mpc_handler.crt_gene_handler import CerGenerateHandler
from mpc_handler.data_del_handler import DataDelHandler
from mpc_handler.data_import_handler import DataImportHandler
from mpc_handler.default_handler import DefaultHandler, HelloHandler
from mpc_handler.get_cert_handler import GetCertHandler
from mpc_handler.get_port_handler import PortGetHandler
from mpc_handler.output_get_handler import OutputGetHandler
from mpc_handler.job_log_handler import JobLogHandler
from mpc_handler.job_query_handler import JobQueryHandler
from mpc_handler.job_reg_handler import JobRegHandler
from mpc_handler.config_get_handler import GetConfigHandler
from mpc_handler.result_get_handler import ResultGetHandler
from mpc_handler.sql_import_handler import SQLImportHandler


def get_handlers():
    data_handlers = [
        ("/1.0/mpc/data/import", DataImportHandler, dict(action="data_import")),
        ("/1.0/mpc/data/del", DataDelHandler, dict(action="data_del")),
        ("/1.0/mpc/data/SQL_import", SQLImportHandler, dict(action="sql_data_import")),
    ]

    crt_handlers = [
        ("/1.0/mpc/crt/gene_crt", CerGenerateHandler, dict(action="ca_import")),
        ("/1.0/mpc/crt/get_crt", GetCertHandler, dict(action="cert_get"))
    ]

    job_handlers = [
        ("/1.0/mpc/job/create", JobRegHandler, dict(action="job_create")),
        ("/1.0/mpc/job/reg", JobRegHandler, dict(action="job_reg")),
        ("/1.0/mpc/job/query", JobQueryHandler, dict(action="job_query")),  # 查询任务状态
        ("/1.0/mpc/log/get", JobLogHandler, dict(action="job_log")),  # 获取任务日志
        # 重要秘密的发送函数
        ("/1.0/mpc/job/get_config", GetConfigHandler, dict(action="config_send")),
        ("/1.0/mpc/job/arith", ArithHandler, dict(action="handle_arith")),
        ("/1.0/mpc/job/get_port", PortGetHandler, dict(action="get_port")),
        ("/1.0/mpc/job/aggre", AggreHandler, dict(action="handle_aggre")),
        # 结果的获取函数
        ("/1.0/mpc/job/output/get", OutputGetHandler, dict(action="output_get")),
        # 获取本地的文件结果
        ("/1.0/mpc/job/result/get", ResultGetHandler, dict(action="result_get"))
    ]

    default_handlers = [
        (r"/hello", HelloHandler),
        (r"/test", HelloHandler),
        (r'/(.*)?', DefaultHandler),
    ]

    res = data_handlers + crt_handlers + job_handlers + default_handlers

    return res
