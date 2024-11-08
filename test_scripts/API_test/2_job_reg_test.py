# 这个脚本向 mpc service 注册一个任务，等待 ready
# 注册任务只需要向发起方的 service 发送一个注册 request 就行，之后就等待它 ready 
# cmd: 
# ipython -i test_scripts/2_job_reg_test.py 

import time
from pprint import pprint

from config import MPC_IP, MPC_PORT
from utilities.utilities import send_restful_request

job_request_dict = {
    "data_list": ["test_data_1", "test_data_2"],
    "party_list": [0, 2],
    "mpc_method": 1 # 也就是基于 hash 的 MPC 
}

print("\n")
method_url = "/1.0/mpc/job/reg"
url = "http://{}:{}".format(MPC_IP, MPC_PORT) + method_url
success, response = send_restful_request(url, job_request_dict)

job_id = response['data']['job_id']
print("The job_id is %s" % job_id)

# 然后一直查状态，直到 ready 退出
check_method_url = "/1.0/mpc/job/query"
url = "http://{}:{}".format(MPC_IP, MPC_PORT) + check_method_url
check_request_dict = {
     "job_id": job_id
}
success, response = send_restful_request(url, check_request_dict)

job_status = response['data']['status']
print("Job Status is: %s." % job_status)

while job_status != "ready":
    time.sleep(.1)
    success, response = send_restful_request(url, check_request_dict)
    job_status = response['data']['status']
    print("Job Status is: %s." % job_status)
print('\n')
pprint(response)
    




