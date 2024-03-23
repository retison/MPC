# test cmd 
# ipython -i flow_control/mpc_hash/rsa_flow_guest.py

import json
import os
import shutil
import time
from multiprocessing import Process
import random

import numpy as np
from gmpy2 import gmpy2

from config import HASH_SPLIT_INTER_COUNT_MAX, CPU_COUNT
from flow_control.api_requests import get_job_registation_request, get_job_status_request
from mpc.protocol.OT import receiver, receiver_oprf
from flow_control.flow_controller_base import FlowControllerBase
from flow_control.utils import calculate_intersection_f2f
# 这个类内调用多进程总会出错，会重复运行脚本自己
from flow_control.utils import restore_intersection_result_mp
from mpc.datastructure import bloom_filter
from mpc.protocol import rsa
from mpc.protocol.OT import cuckoo
from mpc.utils import get_rsa_key
from utilities.status_code import action_method_dict


# 主要流程包括：
# 1. 找其他参与方注册任务
# 2. 等其他参与方让任务 ready
# 3. 作为client接收其他方作为server生成的公钥
# 4. 获得公钥并根据公钥生成random_factors
# 5. 向所有参与方请求其 signed_server_set 并 构建 bf
# 6. 对每个对象盲化得到blind_set并传输给所有参与方并接收所有对象的签名
# 7. 进行剩余计算
# 8. 对本地所有任务的结果进行求交
# 9. 根据交的结果找到所有元素
# 10. 任务结束，更新任务状态

# 这里需要继承 base


class OTFlowGuest(FlowControllerBase):
    # 这里就是作为发起方，在 任务 ready 之后
    # 作为回调函数，利用接口完成剩余的内容
    def __init__(self, job_id):
        self.job_id = job_id
        super(OTFlowGuest, self).__init__(job_id)
        self.intermediate_dir = os.path.join(self.job_dir, "intermediate")
        self.intersection_dir = os.path.join(self.job_dir, "intersection")
        self.output_dir = os.path.join(self.job_dir, "mpc_result")
        self.public_key = None
        self.receiver = None
        self._12n = 0

    def run_protocol(self):
        self.parameter_check()
        if len(self.party_list) > 0:
            # 1. 找其他参与方注册任务
            # 1.A 获得其他参与方的 ip + 端口
            register_job = self.register_job_to_parties()
            if register_job is True:
                self.change_job_status("running")
            else:
                self.change_job_status("failed")
                return
            # 2. 等其他参与方让任务 ready
            self.logger.info("Start check job ready status.")
            all_ready = self.check_job_ready_parties()
            if all_ready is not True:
                self.change_job_status("failed")
                return
            self.logger.info("All party job status is ready.")
            # 3. 生成布谷鸟哈希桶,并将生成的随机值传输给所有参与方
            self.logger.info("Start generate and share random values.")
            self.receiver = self.generate_receiver()
            self._12n = int(1.2 * self.receiver.cuckoo.n)
            self.send_key()
            oprf = receiver_oprf.receiver_oprf(self.receiver.key_length)
            will_send = oprf.send_oprf(keys=self.receiver.cuckoo_rand_bit)
            self.logger.info("finish generate and share random values")
            for each_party in self.party_list:
                if each_party == 0:
                    continue
                # 没有目录创建目录
                if not os.path.exists(self.intermediate_dir):
                    os.makedirs(self.intermediate_dir)
                    self.logger.info("Intermediate dir created.")
                # 从每个 split 开始取中间结果
                # 每次取最大 50_0000 条，然后放到同一个 csv 中即可
                self.send_intermediate_results(each_party, will_send)
                self.logger.info("All Intermediate Result Collected.")
                # 6. 计算交集
                self.logger.info("Start calculate intersection.")
                # 6.1 获取基于OT两方mpc的结果
                self.intersection(each_party, oprf)
            # 7. 所有两方MPC的结果求交
            file_list = os.listdir(self.intermediate_dir)
            # 处理一下路径其他问题
            for f in file_list:
                if not ".csv" in f or not "intermediate_party" in f:
                    file_list.remove(f)
            self.logger.info("local hash MPC start!")
            for each_file in file_list:
                file_path = os.path.join(self.intermediate_dir, each_file)
                self.intersection_calculation_class(file_path)
            # shutil.rmtree(self.intermediate_dir)
            self.logger.info("local hash MPC finish!")
            # 8. 恢复出来的 MPC 的结果
            self.logger.info("restore MPC result start!")
            restore_intersection_result_mp(self.hash_dir, self.intersection_dir, self.output_dir)
            # shutil.rmtree(self.intersection_dir)
            self.logger.info("restore MPC result finish!")
            self.change_job_status("success")  # 到此结束！

    # 还是通过文件传递，主要是避免大量消耗内存
    # 同时可以多进程处理问题
    # TODO 目前存在问题，不能进行多进程加速，
    # 当前版本是单进程的，能跑但是不够快
    def intersection_calculation_class(self, input_path):
        # 一个 input_path 就是来自一个 party 的 intermediate result
        # assert self.__job_id is not None
        assert self.job_id is not None
        # 如果没有 current_intersection 目录：那么读本地hash
        # 如果存在 current_intersection 目录：那么读intersection hash 目录
        # 如果没有 dir ，那么创建目录
        if not os.path.exists(self.current_intersection_dir):
            os.makedirs(self.current_intersection_dir)
            # 那么后续需要读取本地 hash 
            input_dir = self.hash_dir
            input_prefix = os.path.join(input_dir, "hash_")
        # 这里的 else 去掉
        else:
            input_dir = self.current_intersection_dir
            # 后续按照进程补充 "%d.csv"
            input_prefix = os.path.join(input_dir, "intersection_")

        # 然后接下来，单开进程计算求交，并输出到文件
        # 这里也可能有一些费内存，以后有需要了，再优化
        self.logger.info("Start calculate intersection.")
        all_st_time = time.time()
        process_list = []
        job_log_path = os.path.join(self.job_dir, "mpc_application.log")
        self.logger.info("Job log set")
        # time.sleep(5)
        for i in range(CPU_COUNT):
            process_cnt = i + 1
            p = Process(target=calculate_intersection_f2f, \
                        args=(input_path, input_prefix, process_cnt, job_log_path))
            p.start()
            process_list.append(p)
            time.sleep(0.1)  # 防止瞬间爆内存，不行以后还可以优化
            # 不用多进程的版本
            # calculate_intersection_f2f(input_path, input_prefix, process_cnt, job_log_path)
            pass
        # self.logger.info("Waiting for all Intersection calculation process done.")    
        for p in process_list:
            p.join()
        all_ed_time = time.time()
        self.logger.info("All Intersection calculation process done.")
        time_used = all_ed_time - all_st_time
        self.logger.info("Intersection calculation time used: %g second." % time_used)
        self.logger.info("Function < calculate_intersection_f2f > finished.")
        pass

    # 获取公钥
    def generate_receiver(self):
        self.logger.info("generate job based information")
        # 生成随机值保证哈希函数的复杂性
        salt = random.getrandbits(64)
        hash_dir = os.path.join(self.job_dir, "hash_dicts")
        if not os.path.exists(hash_dir):
            self.logger.error("hash dir is not exists!")
        file_list = os.listdir(hash_dir)
        self.logger.info("Start generate cuckoo hash!")
        n = 0
        count = 0
        # 读取文件确认n的大小来创建布谷鸟哈希
        for _ in file_list:
            count += 1
            current_file = os.path.join(hash_dir, "hash_" + str(count) + ".csv")
            client_set = list(set(self.get_hash_data(current_file)))
            n += len(client_set)
        self.logger.info("has %d data" % n)
        # 根据数据大小创建布谷鸟哈希，并根据布谷鸟哈希创建接收方
        cuckoo_hash = cuckoo.Cuckoo(n, int(n / 8), 128, salt)
        self.logger.info("start generate a receiver")
        rec = receiver.receiver(128, salt, cuckoo_hash)
        count = 0
        # 向接收方的布谷鸟哈希桶插入数据，得到完整的接收方
        for _ in file_list:
            count += 1
            self.logger.info("insert data for split %d" % count)
            current_file = os.path.join(hash_dir, "hash_" + str(count) + ".csv")
            client_set = self.get_hash_data(current_file)
            rec.insert_data(client_set)
        rec.fill_none()
        self.logger.info("receiver has generated")
        return rec

    # 输入随机值，向各方发送
    def send_intermediate_results(self, each_party, will_send):
        self.logger.info("Start get intermediate result for party %s" % each_party)
        return self.send_intermediate_result_single(each_party, will_send)

    # 很可以多线程加速的，先完成吧，其他的以后再说
    def send_intermediate_result_single(self, party_id, will_send_all):
        # 得到当前文件最大大小
        span = int(HASH_SPLIT_INTER_COUNT_MAX / len(will_send_all[0][0]))
        # 将will_send中的数据都变为字符串形式，方便传输
        will_send = [item for list1 in will_send_all for item in list1]
        count = 0
        will_send = [i.tolist() for i in will_send]
        will_send = ["".join([str(i) for i in j]) for j in will_send]
        while True:
            # 根据最大数据条数传输数据
            end = count + span
            if end > 256:
                end = 256
            self.logger.info("current send data is %d to %d" % (count, end))
            party_dict = self.get_party_info(party_id)
            party_ip = party_dict['party_mpc_ip']
            party_port = party_dict['party_mpc_port']
            # 传输will_send给相应的参与方
            self.logger.info("send will_send %d to %d data to party %d" % (count, end, party_id))
            inter_query_url = "http://{}:{}/1.0/mpc/job/interaction".format(party_ip, party_port)
            request_dict = {
                "job_id": self.job_id + "_host",
                "count": end,
                "data_list": will_send[count:end]
            }
            # 说明 有 party 离线
            success, res = self.send_restful_request(inter_query_url, request_dict)
            count += span
            self.logger.info("hash send data %d" % end)
            if res["data"]["is_ok"]:
                self.logger.info("has send all data to party %d" % party_id)
                break

        # 继续进行后续正常操作
        # 针对每一个文件发送一次，但其实太大了，可能得想办法缩减
        # 这一段就是获取hash 后的结果
        self.logger.info("start get party %d hash data." % party_id)
        for hash_num in range(5):
            # 由于储藏桶也用布谷鸟哈希，所以这里是5个哈希函数
            split = 1
            self.logger.info("start get hash %d party %d data" % (hash_num, party_id))
            while True:
                request_dict = {
                    "job_id": self.job_id + "_host",
                    "hash_num": hash_num,
                    "split": split,
                }
                success, res = self.send_restful_request(inter_query_url, request_dict)
                self.logger.info("get hash %d party %d data split %d" % (hash_num, party_id, split))
                res_list = res["data"]["result"]
                # 将数据储存到intermediate中
                self.write_intermediate_result(party_id, res_list, split, hash_num)
                split += 1
                if res["data"]["is_ok"]:
                    self.logger.info("get hash %d party %d all data" % (hash_num, party_id))
                    break
        self.logger.info("finish get party %d hash." % party_id)
        # 最终日志
        self.logger.info("interaction with Party %s collected." % party_id)

    # 目前没有用到 split
    # 预留了参数接口，以后有需要再启用
    def write_intermediate_result(self, party_id, res_list, split, hash_num):
        # 没有目录就创建目录
        inter_dir = self.intermediate_dir
        # 目前我们使用 split 之后的内容
        # 后面每个进程分批扫描每个 split csv 文件，然后获取结果即可
        inter_path = os.path.join(inter_dir, "intermediate_party_%d_hash_%d.csv" % (party_id, hash_num))
        # 写入文件
        f = open(inter_path, "a")
        for each_hash in res_list:
            f.write(str(each_hash) + '\n')
        f.close()
        self.logger.debug("Intermediate result of party {} split {} write to disk".format(party_id, split))
        # 结束

    # 等待 wait  秒
    # 重复 retry 次
    def check_job_ready_parties(self, wait=2, retry=40):
        all_ready = False
        try_count = 0
        while not all_ready and try_count < retry:
            try_count += 1
            ready_cnt = 0
            # 针对每个
            for each_party in self.party_list:
                if each_party == 0: continue
                res = self.check_job_ready_single_party(each_party)
                # ready 之后计入统计
                if res == 'ready':
                    ready_cnt += 1
                # 有失败的直接返回失败
                elif res == "failed":
                    return False
            # 查完一遍之后
            # print("ready cnt: %s" % ready_cnt)
            if ready_cnt == len(self.party_list) - 1:  # -1 去掉自己
                all_ready = True
                break
            else:
                time.sleep(wait)
            pass
        if all_ready:
            self.logger.info("All party ready!")
        else:
            self.logger.info("NOT all party ready, exit excution")
        return all_ready

    # 2022.11.14 修改为返回具体状态
    # 目前测试 OK
    def check_job_ready_single_party(self, party_id):
        party_dict = self.get_party_info(party_id)
        party_name = party_dict["party_name"]
        party_ip = party_dict['party_mpc_ip']
        party_port = party_dict['party_mpc_port']
        request_body = get_job_status_request(self.job_id + "_host")
        job_reg_url = "http://%s:%s%s" % (party_ip, party_port, action_method_dict["job_query"])
        success, res = self.send_restful_request(job_reg_url, request_body)
        if success is False:
            return False
        try:
            # 只有这种情况下，是没问题的
            if res["code"] == 0 and res["data"]["status"] == "ready":
                return "ready"
            elif res["code"] == 0 and res["data"]["status"] != "ready":
                return res["data"]["status"]
        except:
            return "unknown"

    # 找其他参与方注册任务
    def register_job_to_parties(self):
        self.update_job_party_list()
        # for each_party_id in self.party_list:
        for i in range(len(self.party_list)):
            each_party_id = self.party_list[i]
            if each_party_id == 0: continue  # 对自己略过
            success = self.register_job_single_party(each_party_id, i)
            if success is False:
                return False
        return True

    def register_job_single_party(self, party_id, party_index):
        # for_test !
        party_dict = self.get_party_info(party_id)
        data_list = self.get_job_data().replace("\'", '\"')
        # 针对每个 host 参与方，仅传1个data name
        data_list = [json.loads(data_list)[party_index]]
        # 开始注册任务
        party_name = party_dict["party_name"]
        self.logger.info("Registering job to party %s.", party_name)
        party_ip = party_dict['party_mpc_ip']
        party_port = party_dict['party_mpc_port']
        request_body = get_job_registation_request(self.job_id + "_host", data_list, self.mpc_method)  # for test !
        job_reg_url = "http://%s:%s%s" % (party_ip, party_port, action_method_dict["job_reg"])
        success, res = self.send_restful_request(job_reg_url, request_body)
        if success is False:
            return False
        # 成功返回了结果
        try:
            if res["code"] == 0:
                self.logger.info("Registering job to party %s success." % party_name)
                return True
            else:
                self.logger.info("Registering job to party %s failed, get response %s" % (party_name, str(res)))
                return False
        except:
            self.logger.info("Registering job to party %s failed." % party_name)
            return False

    #获取对应文件的哈希信息
    def get_hash_data(self, file):
        with open(file, "r") as f:
            data_list = f.readlines()
        f.close()
        data_list = [i.split(',')[0] for i in data_list]
        data_list = list(set(data_list))
        return data_list

    # receiver进行剩余计算，将数据保存到intersection文件夹中
    def intersection(self, each_party, oprf):
        self.logger.info("start get intersection")
        intermediate_path = os.path.join(self.intermediate_dir, "intermediate_party_%d.csv" % each_party)
        self.logger.info("Start handle hash tables")
        #开始处理哈希桶中的数据
        for hash_num in range(3):
            self.logger.info("Start handle hash table %d" % (hash_num + 1))
            res = []
            party_hash_file = os.path.join(self.intermediate_dir,
                                           "intermediate_party_%d_hash_%d.csv" % (each_party, hash_num))
            #读取对应参与方的对应哈希后的结果
            self.logger.info("read party %d hash %d result"%(each_party,hash_num))
            f = open(party_hash_file, "r")
            party_hash_result = f.readlines()
            f.close()
            os.remove(party_hash_file)
            party_hash_result = [i[:-1] for i in party_hash_result]
            party_hash_result = [int(i, 2) for i in party_hash_result]
            # 得到求交结果
            self.logger.info("get party %d hash %d interaction result" % (each_party,hash_num))
            for i in range(int(1.2 * self.receiver.cuckoo.n)):
                if self.receiver.cuckoo.table_full[i] - 1 == hash_num:
                    local_val = oprf.eval(i)
                    if local_val in party_hash_result:
                        res.append(self.receiver.cuckoo.table[i])
            #保存求交结果
            with open(intermediate_path, "a") as f:
                for line in res:
                    curr_str = hex(line)[2:]
                    while len(curr_str) < 32:
                        curr_str = "0" + curr_str
                    f.write(curr_str + "\n")
            f.close()
        #对于储藏桶做和哈希桶相同的操作
        self.logger.info("Start handle hash stash")
        for hash_num in range(2):
            res = []
            party_hash_file = os.path.join(self.intermediate_dir,
                                           "intermediate_party_%d_hash_%d.csv" % (each_party, hash_num + 3))
            f = open(party_hash_file, "r")
            party_hash_result = f.readlines()
            f.close()
            os.remove(party_hash_file)
            party_hash_result = [i[:-1] for i in party_hash_result]
            party_hash_result = [int(i, 2) for i in party_hash_result]
            # 得到求交结果
            self.logger.info("get party %d stash hash %d interaction result" % (each_party,hash_num))
            for i in range(self.receiver.cuckoo.s):
                if self.receiver.cuckoo.stash_full[i] - 1 == hash_num + 3:
                    local_val = oprf.eval(i)
                    if local_val in party_hash_result:
                        res.append(self.receiver.cuckoo.stash[i])
            with open(intermediate_path, "a") as f:
                for line in res:
                    curr_str = hex(line)[2:]
                    while len(curr_str) < 32:
                        curr_str = "0" + curr_str
                    f.write(curr_str + "\n")
            f.close()
        self.logger.info("party %d Intersection calculation finished." % each_party)

    # 向各方发送任务的关键信息
    def send_key(self):
        for each_party in self.party_list:
            if each_party == 0:
                continue
            self.logger.info("Start send based information for party %s" % each_party)
            self.send_key_single(each_party)

    # 发送信息
    def send_key_single(self, party_id):
        party_dict = self.get_party_info(party_id)
        party_name = party_dict["party_name"]
        party_ip = party_dict['party_mpc_ip']
        party_port = party_dict['party_mpc_port']

        # 如果这里还有问题，后续可以再增加
        self.logger.info("Start request key for party %s (IP: %s:%s)" % (party_name, party_ip, party_port))
        inter_query_url = "http://{}:{}/1.0/mpc/job/get_key".format(party_ip, party_port)
        # 发一个请求即可
        request_dict = {
            "job_id": self.job_id + "_host",
            "n": self._12n,
            "s": self.receiver.cuckoo.s,
            "salt": self.receiver.salt,
        }
        success, res = self.send_restful_request(inter_query_url, request_dict)
        # 最终日志
        self.logger.info("ID %d has been send information." % party_id)


'''
# 这个前缀对于跑多进程而言非常重要
if __name__ == "__main__":
    x = HashFlowGuest("20221117141552534.1_da8ed3")
    x.run_protocol()
'''
