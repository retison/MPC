def get_job_registation_request(job_id, data_list, mpc_method, key):
    res = {
        "job_id": job_id,
        "mpc_method": mpc_method,  # 固定 hash 即可
        "party_list": ["NOT_SHOWN_TO_HOST"],  # 这里的  party_list 只能是名字，不是 代号
        "data_list": data_list,  # 这里需要保证 data list 里的名称，都是各个参与方相同的
        "from_machine": True,
        "key": key
    }
    return res


def get_job_status_request(job_id):
    res = {
        "job_id": job_id}
    return res
