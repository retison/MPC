import base64
import json
import os
import time
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from config import HASH_SPLIT_INTER_COUNT_MAX
from flow_control.api_requests import get_job_registation_request, get_job_status_request
from flow_control.flow_controller_base import FlowControllerBase
from utilities.status_code import action_method_dict


# 主要流程包括：
# 1. 找其他参与方注册任务
# 2. 等其他参与方让任务 ready
# 3. 各个参与方将需求数据存储到自己的data_dicts
# 4. 各个参与方将数据进行AES加密然后发送给本机
# 5. 本机使用相同的密钥将数据解密，并截取相应的长度，保存到本地
# 6. 任务结束，更新任务状态


# 这里需要继承 base
class SubtringFlowGuest(FlowControllerBase):
    # 这里就是作为发起方，在 任务 ready 之后
    # 作为回调函数，利用接口完成剩余的内容
    def __init__(self, job_id):
        self.job_id = job_id
        super(SubtringFlowGuest, self).__init__(job_id)
        self.output_dir = os.path.join(self.job_dir, "mpc_result")

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
            self.key = self.get_job_key()
            self.get_length = int(self.mpc_method.split(',')[1], 10)
            self.logger.info("Start get output result.")
            # 没有目录创建目录
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                self.logger.info("output dir created.")
            # 进行计算并将结果保存到output目录下
            self.get_output_results()
            self.logger.info("All output Result Collected.")
            self.change_job_status("success")  # 到此结束！

    def get_output_results(self):
        for each_party in self.party_list:
            if each_party == 0:
                continue
            self.logger.info("Start get output result for party %s" % each_party)
            self.get_output_result_single(each_party)
        pass

    def get_output_result_single(self, party_id):
        party_dict = self.get_party_info(party_id)
        party_name = party_dict["party_name"]
        party_ip = party_dict['party_mpc_ip']
        party_port = party_dict['party_mpc_port']
        # 3.1 先用 split 0 去查询 split 的数量以及 每个 split 的count
        inter_query_url = "http://{}:{}/1.0/mpc/job/output/get".format(party_ip, party_port)
        request_dict = {"job_id": self.job_id + "_host", "split": 0}
        # 说明 有 party 离线
        success, res = self.send_restful_request(inter_query_url, request_dict)
        # 进行一些错误检查
        failed = False
        if success == False:
            failed = True
        elif success == True and res.get("code", 500) != 0:
            failed = True
        # 如果有错误，进行一些操作
        if failed:
            self.logger.error("Party {} (IP address {}:{}) offline!".format(party_name, party_ip, party_port))
            self.logger.error("Job {} status set to failed.".format(self.job_id))
            self.change_job_status("failed")
            return False
        # 如果开始不 failed，我们就默认后面的都能 连通 上
        # 如果这里还有问题，后续可以再增加
        self.logger.info(
            "Start request output result for party %s (IP: %s:%s)" % (party_name, party_ip, party_port))
        # 继续进行后续正常操作
        split_count = res["data"]["split_count"]
        split_length_list = res["data"]["split_length_list"]
        # 然后针对每一个 split 发一组（可能是一个、也可能多个） request
        for i in range(split_count):
            split = i + 1
            split_length = split_length_list[i]
            if split_length < HASH_SPLIT_INTER_COUNT_MAX:
                # 发一个请求即可
                request_dict = {
                    "job_id": self.job_id + "_host", "split": int(split)
                }
                success, res = self.send_restful_request(inter_query_url, request_dict)
                iv_list = res["data"]["iv"]
                ct_list = res["data"]["ct"]
                res_list = self.decrypt_message(iv_list, ct_list)

                self.write_output_result(party_id, res_list, split)
                # 然后把这东西存储到 job 硬盘即可
                self.logger.info("output result Split %d (Single) collected." % split)
                continue
            # 然后处理多个请求
            request_count = int(split_length / HASH_SPLIT_INTER_COUNT_MAX) + 1
            # 曹，又要套一层循环，这个以后还是得改
            # 要不然看着就晕
            for j in range(request_count):
                st_index = j * HASH_SPLIT_INTER_COUNT_MAX
                length = HASH_SPLIT_INTER_COUNT_MAX
                request_dict = {
                    "job_id": self.job_id + "_host", "split": int(split),
                    "length": length, "st_index": st_index
                }
                success, res = self.send_restful_request(inter_query_url, request_dict)
                iv_list = res["data"]["iv"]
                ct_list = res["data"]["ct"]
                res_list = self.decrypt_message(iv_list, ct_list)
                self.write_output_result(party_id, res_list, split)
                pass
            self.logger.info("output result(split %d) collected." % split)
        # 最终日志
        self.logger.info("output result of Party %s collected." % party_id)

    # 目前没有用到 split
    # 预留了参数接口，以后有需要再启用
    def write_output_result(self, party_id, res_list, split_count):
        # 没有目录就创建目录
        inter_dir = self.output_dir
        # 目前我们使用 split 之后的内容
        # 后面每个进程分批扫描每个 split csv 文件，然后获取结果即可
        inter_path = os.path.join(inter_dir, "output_party_%d.csv" % (party_id))
        # 然后进行追加写入
        f = open(inter_path, "a")
        for each_data_list in res_list:
            f.write(each_data_list + '\n')
        f.close()
        self.logger.debug("output result of party {} split {} write to disk".format(party_id, split_count))
        # 结束

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

    def check_job_ready_single_party(self, party_id):
        party_dict = self.get_party_info(party_id)
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
            success = self.register_job_single_party(each_party_id, i - 1)
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
        request_body = get_job_registation_request(self.job_id + "_host", data_list, "substring",
                                                   self.get_job_key())  # for test !
        job_reg_url = "http://%s:%s%s" % (party_ip, party_port, action_method_dict["job_reg"])
        self.logger.info("Registering job to party %s su." % job_reg_url)
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

    # 解密信息
    def decrypt_message(self, iv_list, ct_list):
        pt_list = []
        curr_key = self.key.encode()[:16]
        for i in range(len(iv_list)):
            iv_decoded = base64.b64decode(iv_list[i])
            ct_decoded = base64.b64decode(ct_list[i])
            cipher = AES.new(curr_key, AES.MODE_CBC, iv_decoded)
            a = cipher.decrypt(ct_decoded)
            pt = unpad(a, AES.block_size)
            pt_list.append(pt.decode('utf-8')[-self.get_length:])
        return pt_list
