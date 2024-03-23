from config import MPC_IP, MPC_PORT
from utilities.utilities import send_restful_request

method_url = "/1.0/mpc/crt/get_crt"
url = "http://{}:{}".format(MPC_IP, MPC_PORT) + method_url
success, response = send_restful_request(url, {"party_id": 1})
print(response["data"])
# 需要全部保存到本地
