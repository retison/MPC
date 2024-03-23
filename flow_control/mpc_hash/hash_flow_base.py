from flow_control.flow_controller_base import FlowControllerBase


# 这里的 queue 可能并不适合
# from config import CPU_COUNT
# CPU_COUNT = 1

# TODO: 还需要更新任务状态，小问题之后处理就行
# 这个 class 属于 基于hash 进行 MPC 的基类
# 增加的是 guest 以及 host 都可能用到的方法
class HashFlowBase(FlowControllerBase):
    def __init__(self, job_id):
        self.job_id = job_id
        super(HashFlowBase, self).__init__(job_id)
        


    
    pass # end of class 


if __name__ == "__main__":
    # for test 
    x = HashFlowBase("20221110171303887.1_da8ed3")
    x.calculate_intersection_f2f("tmp/job/20221110171303887.1_da8ed3/intermediate/intermediate_party_2_1.csv")
    x.calculate_intersection_f2f("tmp/job/20221110171303887.1_da8ed3/intermediate/intermediate_party_2_3.csv")






    
