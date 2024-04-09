from config import MPC_IP, MPC_PORT
from utilities.utilities import send_restful_request

job_request_dict = {
    "job_id":"20240409154010177_host",
    "data_name":"mpc_result_0"
}

print("\n")
method_url = "/1.0/mpc/job/output/get"
url = "http://{}:{}".format(MPC_IP, 8626) + method_url
print(url)
success, response = send_restful_request(url, job_request_dict)
print(success,response)
url ="http://{}:{}".format(MPC_IP, 8526) + method_url
print(url)
success,response = send_restful_request(url, job_request_dict)
print(success,response)