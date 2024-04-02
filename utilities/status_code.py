# Status Code of HE Sandbox 

# 常规状态码
SUCCESS = 0
METHOD_NOT_ALLOWED = 405

# 40xx, mostly because elements not found 
KEY_ERROR = 4003
DATA_NOT_FOUND = 4004

# 50xx, mostly because internal error 
DATA_TYPE_ERROR = 5002
DATA_VALUE_ERROR = 5003
REQUEST_DECODE_ERROR = 5004  # request 的 json decode error
OPERATION_FAILED = 5005

# Status Message of flow-control
status_msg_dict = {}
status_msg_dict[SUCCESS] = "success"
status_msg_dict[METHOD_NOT_ALLOWED] = "method NOT allowed"

# 40xx, 
status_msg_dict[DATA_NOT_FOUND] = "data source not found"
status_msg_dict[KEY_ERROR] = "necessary key not found in request"

# 50xx, 
status_msg_dict[REQUEST_DECODE_ERROR] = "request content decode error, please check the request body"
status_msg_dict[DATA_TYPE_ERROR] = "input data type not correct"
status_msg_dict[DATA_VALUE_ERROR] = 'input data value not correct'
status_msg_dict[OPERATION_FAILED] = 'MPC operation failed'

action_list = [
    'data_import', 'data_del', 'job_reg', 'job_query',"sql_data_import",
    'output_import', 'result_get', 'job_log', 'output_get',
    'config_send', 'interaction',
    'ca_import', 'cert_get',
    'handle_arith', 'get_port', "handle_aggre", "handle_substr",
]

# 记录接口的URL和其他必要信息
action_method_dict = {}

action_method_dict["data_import"] = "/1.0/mpc/data/import"
action_method_dict["data_del"] = "/1.0/mpc/data/delete"
action_method_dict["sql_data_import"] = "/1.0/mpc/data/SQL_import"

action_method_dict["ca_import"] = "/1.0/mpc/crt/gene_crt"
action_method_dict["cert_get"] = "/1.0/mpc/crt/get_crt"

action_method_dict['job_reg'] = "/1.0/mpc/job/reg"
action_method_dict['job_query'] = "/1.0/mpc/job/query"
action_method_dict['job_log'] = "/1.0/mpc/log/get"

action_method_dict['config_send'] = "/1.0/mpc/job/get_config"
action_method_dict["get_port"] = "/1.0/mpc/job/get_port"
action_method_dict["handle_arith"] = "/1.0/mpc/job/arith"
action_method_dict["handle_aggre"] = "/1.0/mpc/job/aggre"

action_method_dict['output_get'] = "/1.0/mpc/job/output/get"

action_method_dict['result_get'] = "/1.0/mpc/result/get"
