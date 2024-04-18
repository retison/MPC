
import os
import time

import setproctitle
import tornado.ioloop
import tornado.web
from tornado.httpserver import HTTPServer

from config import PORT, mpc_data_dir, mpc_job_dir
from route import get_handlers
from utilities import global_var as global_dict
from utilities import logger
from utilities.generate_certificates import generate_CA
from utilities.utilities import check_db_connection, check_servicing_in_running, check_table

def initialize():
    # 0. 检查端口占用 / 服务器底哦那个情况
    check_servicing_in_running()
    # 1. 首先测试db连接，能连接上继续；
    table_list = check_db_connection()
    # 2. 检查数据库表情况，如果没有（初次启动），需要建表；
    check_table(table_list)
    # 3. 设定 proc 名称
    setproctitle.setproctitle('MPC_Comp')
    logger.info("Proc Name Set: MPC_Comp.")
    # 4. 设定全局变量 dict 
    global_dict._init()
    global_dict.set_value("last_update", time.time())
    # 5. 建立缓存目录
    if not os.path.exists(mpc_data_dir): os.makedirs(mpc_data_dir)
    if not os.path.exists(mpc_job_dir): os.makedirs(mpc_job_dir)
    logger.info("tmp/ dir OK.")
    # 6.生成CA证书
    generate_CA()


def make_app():
    handlers = get_handlers()
    application = tornado.web.Application(handlers = handlers)
    return application


def main():
    initialize()

    app = make_app()
    server = HTTPServer(app)
    server.bind(PORT)
    server.start()
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()


