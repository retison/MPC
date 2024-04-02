import os 

CPU_COUNT = os.cpu_count() # 用于多线程
PORT = 8626# mpc comp 的端口


MPC_IP = "127.0.0.1"
MPC_PORT = PORT

# 对于 hash 而言，一次最多请求 100万条，一次大概 35 M
HASH_SPLIT_INTER_COUNT_MAX = 100_0000

# 日志路径
logger_config_path = r"utilities/logging.conf"

# docker pull mysql:5.7
# docker run -itd --name mysql-mpc -p 13306:3306 -e MYSQL_ROOT_PASSWORD=123456 mysql:5.7
# 打包之后，这些内容可以通过docker的环境变量获取？
local_db_ip = "127.0.0.1"
local_db_port = 13306
local_db_username = "root"
local_db_passwd = "123456"
local_db_dbname = 'mysql'

# MPC 缓存路径
mpc_data_dir = 'tmp/data/'
mpc_job_dir   = "tmp/job/"
split_count = CPU_COUNT

SALT =  "_mpc_salt"

