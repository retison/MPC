from mpc_handler.IntegerHandler import IntegerHandler
from mpc_handler.InteractionHandler import InteractionHandler
from mpc_handler.crt_gene_handler import CerGenerateHandler
from mpc_handler.data_del_handler import DataDelHandler
from mpc_handler.data_import_handler import DataImportHandler
from mpc_handler.default_handler import DefaultHandler, HelloHandler
from mpc_handler.get_cert_handler import GetCertHandler
from mpc_handler.get_port_handler import PortGetHandler
from mpc_handler.intermediate_get_handler import IntermediateGetHandler
from mpc_handler.intermediate_import_handler import IntermediateImportHandler
from mpc_handler.job_log_handler import JobLogHandler
from mpc_handler.job_query_handler import JobQueryHandler
from mpc_handler.job_reg_handler import JobRegHandler
from mpc_handler.config_get_handler import  GetConfigHandler


def get_handlers():
    data_handlers = [
        ("/1.0/mpc/data/import", DataImportHandler, dict(action="data_import")),
        ("/1.0/mpc/data/del", DataDelHandler, dict(action="data_del")),
    ]

    crt_handlers = [
        ("/1.0/mpc/crt/gene_crt", CerGenerateHandler, dict(action="ca_import")),
        ("/1.0/mpc/crt/get_crt",GetCertHandler,dict(action = "cert_get"))
    ]

    #TODO 需要写加减乘除，逻辑比较，聚合函数（max，min，sum,avg,count）,substring等
    #TODO 大致流程：先将任务部署在本机、中心服务器和各个参与方，然后在本地生成大数字发送给中心服务器和各个参与方
    #TODO 3天完成加减乘除与运算，浮点数和整形的区别必须注意啊,只有相同类型的才可以进行计算
    #TODO 关于浮点数可以使用TEST10和TEST11的东西，但是要考虑一个问题，就是可能结果一定有浮点数，需要考虑所有数据类型是否均是.0结尾
    #TODO 对于聚合函数直接就能获取啊，不用思考那么多了就这样呗，没必要管那么多了
    #TODO 对于substring请直接用AES哦，我想就直接让他把数据发到本地然后直接裁剪吧
    #TODO order by\limit\group by这三个中前两个还是很有关系的，但不知道该咋办，这三个还是要多思考一会
    job_handlers = [
        ("/1.0/mpc/job/reg", JobRegHandler, dict(action="job_reg")),
        ("/1.0/mpc/job/query", JobQueryHandler, dict(action="job_query")),  # 查询任务状态
        ("/1.0/mpc/log/get", JobLogHandler, dict(action="job_log")),  # 获取任务日志
        # 重要秘密的发送函数
        ("/1.0/mpc/job/get_config", GetConfigHandler, dict(action="config_send")),
        ("/1.0/mpc/job/integer", IntegerHandler, dict(action="handle_integer")),
        ("/1.0/mpc/job/get_port", PortGetHandler, dict(action="get_port")),
        # ("/1.0/mpc/job/float", FloatHandler, dict(action="handle_Float")),


        ("/1.0/mpc/job/interaction", InteractionHandler, dict(action="interaction")),
        # 中间结果的获取函数
        ("/1.0/mpc/job/intermediate/get", IntermediateGetHandler, dict(action="intermediate_get")),
        ("/1.0/mpc/job/intermediate/import", IntermediateImportHandler, dict(action="intermediate_import")),
        ("/1.0/mpc/job/intermediate/transfer", IntermediateImportHandler, dict(action="intermediate_import")),
        ("/1.0/mpc/result/get", IntermediateGetHandler, dict(action="result_get")),

    ]

    default_handlers = [
        (r"/hello", HelloHandler),
        (r"/test", HelloHandler),
        (r'/(.*)?', DefaultHandler),
    ]

    res = data_handlers + crt_handlers + job_handlers + default_handlers

    return res
