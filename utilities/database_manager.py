import pymysql
from pymysql.err import OperationalError, IntegrityError

from utilities import logger


# 为什么放在 utils ，因为其他模块也可能会用到的
class database_manager:

    def __init__(self, databaes_ip, database_port,\
        database_username, database_passwd,
        database_name, database_type = 'mysql') -> None:
        
        # 进行初始化 
        self.database_type = database_type
        self.databaes_ip = databaes_ip
        self.database_port = database_port 
        self.database_username = database_username
        self.database_passwd = database_passwd
        self.database_name = database_name

        # 记录状态
        self.conn = None 
        self.conn_status = "waiting"
        self.insert_success = False
        self.del_success    = False

        # 建立一个 conn，
        if self.database_type == "mysql":
            # logger.info("Establish Mysql Connection with %s:%s"%(self.databaes_ip, self.database_port))
            try:
                self.conn = pymysql.connect(host=self.databaes_ip, port = self.database_port,\
                    user=self.database_username, password=self.database_passwd, \
                    database=self.database_name)
                self.conn_status = "success"
                logger.info("DBM Connection Created.")
            except ConnectionRefusedError:
                self.conn_status = "failed"
                logger.error("DBM Connection Refused")
            except OperationalError:
                logger.error("DBM OperationalError")
                self.conn_status = "failed"
        else:
            logger.error("Not support other Database yet.")
            raise NotImplementedError("Not support other Database yet.")
        pass
    
    def query(self, sql):
        # 查询动作，把数据给提出来，以列表形式返回
        # assert 'select' in sql.lowercase()
        if 'select' not in sql.lower():
            raise ValueError("NOT a Query SQL")
        cursor = self.conn.cursor()
        select_cnt = cursor.execute(sql) # 得到结果行数
        res = []
        for _ in range(select_cnt):
            sample = list(cursor.fetchone())
            res.append(sample)
        cursor.close()
        logger.debug("Query: \"%s\", Success" % sql.replace("\n", " ").strip())
        return res # 结果一定是二维数组
    
    def insert(self, sql):
        # INSERT INTO `fed_sql`.`b_job` (`id`, `job_id`, `job_sql`, `job_ priority`, `grammar_check`, `integrity_check`, `status`, `create_time`, `update_time`, `start_time`) VALUES ('49', '1', 'selec * from A.xx;', '2', '1', '1', 'success', '1657871901', '1657871901', '1657871901');
        if 'insert' not in sql.lower() and 'update' not in sql.lower():
            raise ValueError("NOT a Insert SQL")
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql) 
            cursor.close()
        except IntegrityError:
            logger.error("Insert: \"%s\", Failed" % sql.replace("\n", " "))
            self.insert_success = False
        cursor.close()
        self.conn.commit()
        if 'insert' in sql.lower():
            logger.debug("Insert: \"%s\", Success" % sql.replace("\n", " "))
        elif 'update' in sql.lower():
            logger.debug("Update: \"%s\", Success" % sql.replace("\n", " "))
        self.insert_success = True

    def delete(self, sql):
        # DELETE FROM `fed_sql`.`s_party` WHERE (`id` = '3');
        if 'delete' not in sql.lower():
            raise ValueError("NOT a Delete SQL")
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql) 
            cursor.close()
        except Exception as e:
            logger.error("Delete: \"%s\", Failed" % sql.replace("\n", " "))
            logger.error(str(e))
            self.del_success = False
        cursor.close()
        self.conn.commit()
        logger.debug("Delete: \"%s\", Success" % sql.replace("\n", " "))
        self.del_success = True
    
    def get_tables(self):
        sql = 'show tables;'
        cursor = self.conn.cursor()
        table_cnt = cursor.execute(sql) 
        res = []
        for _ in range(table_cnt):
            table = cursor.fetchone()[0]
            res.append(table)
        return res
    
    def create_table(self, sql):
        if 'create' not in sql.lower():
            raise ValueError("NOT a Create SQL")
        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()

    def __del__(self):
        # 关闭数据库连接
        if self.conn_status == 'failed':
            logger.info("NO DBM Connection, Closed.")
        elif self.conn_status != 'closed':
            self.conn.close()
            logger.info("DBM Connection Closed.")
    
    # 手工 close 
    # 一般不太用得到
    def close(self):
        self.conn.close()
        self.conn_status = "closed"
        logger.info("DBM Connection Closed in func <close>.")

