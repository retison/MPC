# 这个脚本向 mpc service 注册一些数据
# 执行： ipython -i test_scripts/1_data_reg_test.py
# 然后查看 success 和 response 

from config import MPC_IP, MPC_PORT
from utilities.utilities import send_restful_request

# 经过测试，一次最多注册数量为50万条，
# 目前双方都是 50万条，交集应该是 25万条
# 但是我们支持分批注册，后续更多数据的时候，需要分批
test_data_1 = [str(i) for i in list(range(0,  1600   ))]
test_data_2 = [str(i) for i in list(range(0, 3200, 2))]
test_data_3 = [str(i) for i in list(range(0, 4800, 3))]
print("Data Generated.")

# 随机打乱
# random.shuffle(test_data_1)
# random.shuffle(test_data_2)
print("Data Shuffled.")

# 补全成 request dict 
request_dict_1 = {
    "data_id" : "test_data_1",
    "id_list" : test_data_1
}
request_dict_2 = {
    "data_id" : "test_data_2",
    "id_list" : test_data_2
}
request_dict_3 = {
    "data_id" : "test_data_3",
    "id_list" : test_data_3
}
method_url = "/1.0/mpc/data/import"

url = "http://{}:{}".format(MPC_IP, MPC_PORT) + method_url
# 然后发起 request 
# 首先看到如果直接发这么大的数据，是否可以扛得住
success_1, response_1 = send_restful_request(url, request_dict_1)
success_2, response_2 = send_restful_request(url, request_dict_2)
success_3, response_3 = send_restful_request(url, request_dict_3)

