import logging.config

from config import logger_config_path

# 经过测试可用
logging.config.fileConfig(logger_config_path)
logger = logging.getLogger('FED_SQL')