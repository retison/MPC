import time

from config import local_db_dbname

b_mpc_job_create = """CREATE TABLE `b_mpc_job` (
    `id` int NOT NULL AUTO_INCREMENT,
    `job_id` varchar(80) NOT NULL,
    `job_data` varchar(450) NOT NULL,
    `job_party` varchar(450) NOT NULL,
    `mpc_method` varchar(20) NOT NULL,
    `job_key` varchar(300) NOT NULL,
    `status` varchar(45) DEFAULT NULL,
    `create_time` int DEFAULT NULL,
    `update_time` int DEFAULT NULL,
    `start_time` int DEFAULT NULL,
    `end_time` int DEFAULT NULL,
    PRIMARY KEY (`id`,`job_id`),
    UNIQUE KEY `id_UNIQUE` (`id`)
);"""


def get_mpc_job_insert_sql(job_id, job_data, job_party, mpc_method, key, status, create_time, update_time):
    mpc_job_insert_sql = """INSERT INTO `%s`.`b_mpc_job` 
    (`job_id`, `job_data`, `job_party`,`mpc_method`, `job_key`, `status`, `create_time`, `update_time`)
    VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s","%s"); """ % \
                         (local_db_dbname, job_id, job_data, job_party, mpc_method, key, status, create_time,
                          update_time)
    return mpc_job_insert_sql


def get_mpc_job_query_sql(job_id):
    sql = '''SELECT status, job_data, job_party, mpc_method, create_time, update_time, start_time, end_time FROM 
    %s.b_mpc_job where job_id = "%s"; ''' % (
        local_db_dbname, job_id)
    return sql


def get_s_party_list():
    return "SELECT id, party_name FROM %s.s_party;" % local_db_dbname


def change_mpc_job_status(job_id, status):
    update_time = int(time.time())
    return '''UPDATE `%s`.`b_mpc_job` SET `status` = '%s', `update_time` = '%s' WHERE (`job_id` = '%s');''' % (
        local_db_dbname, status, update_time, job_id)


def get_key(job_id):
    return '''SELECT job_key FROM %s.b_mpc_job where job_id = "%s";''' % (local_db_dbname, job_id)
