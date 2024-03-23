import time
from config import MPC_IP, MPC_PORT
from utilities.utilities import send_restful_request

job_request_dict = {
    "data_list": ["test_data_1", "test_data_2"],
    "party_list": [0, 1],
    "mpc_method": "+"
}

print("\n")
method_url = "/1.0/mpc/job/reg"
url = "http://{}:{}".format(MPC_IP, MPC_PORT) + method_url
success, response = send_restful_request(url, job_request_dict)
print(response)
job_id = response['data']['job_id']
print("The job_id is %s" % job_id)

# # 然后一直查状态，直到 ready 退出
# check_method_url = "/1.0/mpc/job/query"
# url = "http://{}:{}".format(MPC_IP, MPC_PORT) + check_method_url
# check_request_dict = {
#     "job_id": job_id
# }
# success, response = send_restful_request(url, check_request_dict)
#
# job_status = response['data']['status']
# print("Job Status is: %s." % job_status)
#
# while job_status != "ready" and job_status != "success":
#     time.sleep(.1)
#     success, response = send_restful_request(url, check_request_dict)
#     job_status = response['data']['status']
#     print("Job Status is: %s." % job_status)
# print('\n')
# print(response)
