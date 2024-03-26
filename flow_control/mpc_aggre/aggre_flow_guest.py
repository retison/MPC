import argparse
import asyncio
import json
import os
import socket
import sys
import time
from multiprocessing import Process
from config import MPC_IP, MPC_PORT
from flow_control.api_requests import get_job_registation_request, get_job_status_request
from flow_control.flow_controller_base import FlowControllerBase
import mpyc.runtime as runtime
from mpyc.runtime import generate_configs
from utilities.status_code import action_method_dict


# 主要流程包括：
# 1. 找其他参与方注册任务
# 2. 等其他参与方让任务 ready
# 3. 查找自己的空闲端口并且向各个参与方查找空闲端口。
# 4. 根据空闲端口创建子进程使用MPyC自带的库进行连接
# 5. 使用MPyC库进行运算并要求将结果发到本地
# 6. 任务结束，更新任务状态

# 这里需要继承 base
class RSAFlowGuest(FlowControllerBase):
    # 这里就是作为发起方，在 任务 ready 之后
    # 作为回调函数，利用接口完成剩余的内容
    def __init__(self, job_id):
        self.job_id = job_id
        super(RSAFlowGuest, self).__init__(job_id)
        self.intersection_dir = os.path.join(self.job_dir, "intersection")
        self.output_dir = os.path.join(self.job_dir, "mpc_result")

    def run_protocol(self):
        self.parameter_check()
        if len(self.party_list) > 0:
            # 1. 找其他参与方注册任务
            # 1.A 获得其他参与方的 ip + 端口
            self.logger.info("start find other candidate")
            register_job = self.register_job_to_parties()
            if register_job:
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
            for party_id in self.party_list:
                if party_id == 0: continue
                self.logger.info("Start send config")
                self.config_dir = os.path.join(self.job_dir, "config")
                if not os.path.exists(self.config_dir):
                    os.makedirs(self.config_dir)
                self.logger.info(f"Send party {self.party_list} finish")
                self.logger.info(f"Start get party {self.party_list} free port")
                info_list = []
                info_list.append(self.get_party_info(0))
                info_list.append(self.get_party_info(party_id))
                info_list[0]["party_mpc_ip"] = MPC_IP
                info_list[0]["party_mpc_port"] = MPC_PORT
                calculation_port_list = self.get_free_port(info_list)
                if calculation_port_list is None:
                    self.logger.error("a party is busy")
                    return
                self.logger.info(f"finish get party {self.party_list} free port")
                self.logger.info(f"Start send config to party {self.party_list}")
                self.generate_config(calculation_port_list, party_id)
                self.send_config()
                self.logger.info("send config finish！")
                self.logger.info("Start mpc calculation")
                if not self.all_mpc_calcalation(party_id):
                    self.logger.error("mpc calculate error")
                    return
                self.logger.info("MPC calculation finish!")
                self.change_job_status("success")  # 到此结束！

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
        data_list = [json.loads(data_list)[party_index]]
        # 开始注册任务
        party_name = party_dict["party_name"]
        self.logger.info("Registering job to party %s.", party_name)
        party_ip = party_dict['party_mpc_ip']
        party_port = party_dict['party_mpc_port']
        request_body = get_job_registation_request(self.job_id + "_host", data_list, self.mpc_method,
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

    pass

    def generate_config(self, info_list, party_id):
        # 填写config的基本信息
        parser = argparse.ArgumentParser()
        parser.add_argument('-p', '--prefix',
                            help='output filename prefix')
        parser.add_argument('-m', '--parties', dest='m', type=int,
                            help='number of parties')
        parser.add_argument('args', nargs='*')
        parser.set_defaults(m=len(info_list), prefix='party')
        for party in info_list:
            argv_info = party['party_mpc_ip'] + ":" + str(party['party_mpc_port'])
            sys.argv.append(argv_info)
        options = parser.parse_args()
        args = options.args
        if len(args) != options.m:
            self.logger.error('A hostname:port argument required for each party.')
        i = True
        addresses = [arg.split(':', 1) for arg in args]
        configs = generate_configs(options.m, addresses)
        self.logger.info("configs generate")
        # 为每个参与方生成配置文件
        for party, config in enumerate(configs):
            if i:
                filename = f'{self.config_dir}/party_{party_id}_0.ini'
                config.write(open(filename, 'w'))
                i = False
                continue
            else:
                filename = f'{self.config_dir}/party_0_{party_id}.ini'
                config.write(open(filename, 'w'))

    def send_config(self):
        for i in range(len(self.party_list)):
            each_party_id = self.party_list[i]
            if each_party_id == 0: continue  # 对自己略过
            success = self.send_config_single_party(each_party_id, i)
            if success is False:
                return False
        return True

    # 发送config文件
    def send_config_single_party(self, party_id, i):
        filename = f'{self.config_dir}/party_0_{party_id}.ini'
        with open(filename, "r") as f:
            config = f.read()
        f.close()
        party_dict = self.get_party_info(party_id)
        party_ip = party_dict['party_mpc_ip']
        party_port = party_dict['party_mpc_port']
        inter_query_url = "http://{}:{}/1.0/mpc/job/get_config".format(party_ip, party_port)
        self.logger.info(f"send config to {party_id}")
        request_dict = {"job_id": self.job_id + "_host", "config": config}
        success, res = self.send_restful_request(inter_query_url, request_dict)
        if success is False:
            return False
        pass

    # 自己进行MPC运算
    def mine_mpc_calculate(self, party_id):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        sys.argv.append(f"-C../tmp/job/{self.job_id}/config/party_{party_id}_0.ini")
        sys.argv.append("-ssl")
        curr_mpc = runtime.setup()
        self.logger.info("I join to MPC calculation ")
        loop.run_until_complete(self.mpc_calculate(curr_mpc, party_id))
        loop.close()

    # 其它参与方加入MPC运算
    def all_mpc_calcalation(self, party_id):
        try:
            for i in range(len(self.party_list)):
                each_party_id = self.party_list[i]
                if each_party_id == 0:
                    continue  # 对自己略过
                success = self.mpc_calcalation_single_party(each_party_id, i)
                if success is False:
                    return False
                self.logger.info(f"{party_id} join to MPC calculation ")
            th = Process(target=self.mine_mpc_calculate(party_id))
            th.start()
            th.join()
            return True
        except:
            return False

    # TODO 发送信号，告知自己要进行的mpc操作并让它们启动，然后最好将参数保存下来。
    # TODO 对于运算前的数据保存后重新时候还是很成功的
    def mpc_calcalation_single_party(self, party_id, i):
        party_dict = self.get_party_info(party_id)
        party_ip = party_dict['party_mpc_ip']
        party_port = party_dict['party_mpc_port']
        self.logger.info("tell to prepare mpc calculation")
        inter_query_url = "http://{}:{}/1.0/mpc/job/aggre".format(party_ip, party_port)
        request_dict = {"job_id": self.job_id + "_host", "count": False}
        success, res = self.send_restful_request(inter_query_url, request_dict)
        return success

    # TODO 这里会不一样
    async def mpc_calculate(self, curr_mpc, party_id):
        await curr_mpc.start()
        key = self.get_job_key()
        key = int(key, 10)
        secint = curr_mpc.SecFxp(128, 96, key)
        input_res = curr_mpc.input(secint(0), senders=[1])
        operation_result = await curr_mpc.output(input_res[0], receivers=[0])
        await curr_mpc.shutdown()
        with open(f"tmp/job/{self.job_id}/mpc_result_{party_id}.csv", "w") as f:
            f.write(str(operation_result) + '\n')
        f.close()
        self.logger.info("mpc calculation finish")

    def get_free_port(self, info_list):
        calculation_port = []
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("localhost", 0))
            sock.listen(1)
            port = sock.getsockname()[1]
            sock.close()
            calculation_port.append({'party_mpc_ip': info_list[0]['party_mpc_ip'], 'party_mpc_port': port})
        except:
            return None
        for party in info_list[1:]:
            single_free_port_dic = self.get_single_free_port(party)
            if single_free_port_dic is None:
                return None
            self.logger.info(
                f"party ip {single_free_port_dic['party_mpc_ip']} party free port{single_free_port_dic['party_mpc_port']}")
            calculation_port.append(single_free_port_dic)

        return calculation_port

    def get_single_free_port(self, party):
        party_ip = party['party_mpc_ip']
        party_port = party['party_mpc_port']
        self.logger.info("get a free port")
        inter_query_url = "http://{}:{}/1.0/mpc/job/get_port".format(party_ip, party_port)
        request_dict = {"job_id": self.job_id + "_host"}
        # 说明 有 party 离线
        success, res = self.send_restful_request(inter_query_url, request_dict)
        if success is False:
            return None
        return {'party_mpc_ip': party_ip, 'party_mpc_port': res["data"]["port"]}
