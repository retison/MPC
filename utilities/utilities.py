import hashlib
import json
import logging
import re
import subprocess
import sys
import time
from keyword import iskeyword

import requests

from config import PORT
from config import local_db_passwd, local_db_ip, local_db_dbname, local_db_port, local_db_username
from utilities import logger
from utilities.database_manager import database_manager
from utilities.sql_template import b_mpc_job_create


# 检查本机端口是否已经被占用
def check_port_in_use(port, host='127.0.0.1'):
    import socket
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((host, int(port)))
        return True
    except socket.error:
        return False
    finally:
        if s:
            s.close()

def check_servicing_in_running():
    # 检查服务是否开启
    in_use = check_port_in_use(PORT)
    if in_use:
        logger.error("Port in USE, Exit Launching.")
        sys.exit(0)
    logger.info("Port Check: OK")

def check_db_connection():
    # db能够连接上，并且有这3个table，算通过
    dbm = database_manager(local_db_ip,local_db_port,\
        local_db_username, local_db_passwd, local_db_dbname)
    if dbm.conn_status != 'success':
        logger.error("DB connection error, Exit Launching.")
        sys.exit(0)
    table_list = dbm.get_tables()
    logger.info("DB Connection Check: OK")
    return table_list

def check_table(table_list):
    # 目前db已经是可以连接的了
    if 'b_mpc_job' in table_list:
        logger.info('Table Check: OK')
        return 
    logger.info('Table Check: lack table')
    # 否则的话，就是第一次进入DB，需要创建表
    dbm = database_manager(local_db_ip,local_db_port,\
        local_db_username, local_db_passwd, local_db_dbname)
    if 'b_mpc_job' not in table_list:
        sql = b_mpc_job_create
        dbm.create_table(sql)
        logger.info("Table b_mpc_job Created.")

    logger.info('Table Check: Done')

def check_ip(ipAddr):
    compile_ip=re.compile('^(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|[1-9])\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)$')
    if compile_ip.match(ipAddr):
        return True
    else:
        return False

def string_to_md5(string):
    md5_val = hashlib.md5(string.encode('utf8')).hexdigest()
    return md5_val

# 用来计算文件行数的、
# MPC中，ID 是不需要表头的
def wc_count(file_name):
    try:
        out = subprocess.getoutput("wc -l %s" % file_name)
        return int(out.split()[0])
    except:
        return 0 

# 判断命名是否可以合法
# 尽量用系统提供的函数，就不用自己重复造轮子了
def is_valid_variable_name(name):
    return name.isidentifier() and not iskeyword(name)

# 添加增加的日志log
def get_log_file_handler(log_path):
    # 具体参数请参考：
    # https://docs.python.org/3/library/logging.handlers.html#timedrotatingfilehandler
    # 查路径请参考：
    # fileshandle.baseFilename
    fileshandle = logging.handlers.TimedRotatingFileHandler(log_path, when='W6', interval=5, backupCount=15, encoding = 'utf-8')
    fileshandle.suffix = "%Y%m%d_%H%M%S.log"
    fileshandle.setLevel(logging.DEBUG)
    fmt_str = '%(asctime)s %(levelname)s %(filename)s[%(lineno)d] %(message)s'
    formatter = logging.Formatter(fmt_str)
    fileshandle.setFormatter(formatter)
    return fileshandle

# 发送 restful 请求   
def send_restful_request( url, request_body, timeout = 10, retry = 40):
    # 先关掉乱七八糟的日志
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    # assert self.ready is True
    success = False
    try_count = 0
    res = "{}"
    while not success and try_count < retry:
        # 这里循环 x
        try_count += 1
        try:
            r = requests.post(url, data = json.dumps(request_body), timeout = timeout)
            # self.logger.info("Request %s success, count = %d"% (url, try_count))
            success = True
            res = r.text
            # self.logger.info("Response: %s" % res[:200] )
        except requests.ConnectionError:
            print("Request %s failed (ConnectionError), try_count = %d"\
                % (url, try_count)) 
            time.sleep(1.5)
        except requests.ConnectTimeout:
            print("Request %s failed (ConnectTimeout), try_count = %d" % (url, try_count))  
        # print("Try Count %d, Success: %s" % (try_count, success))
        pass
    # 搞定，返回是否成功 + 内容即可
    return success, json.loads(res)