from flow_control.flow_controller_base import FlowControllerBase


# 这里的 queue 可能并不适合
# from config import CPU_COUNT
# CPU_COUNT = 1

# TODO: 还需要更新任务状态，小问题之后处理就行
class AggreFlowBase(FlowControllerBase):
    def __init__(self, job_id):
        self.job_id = job_id
        super(AggreFlowBase, self).__init__(job_id)
        


    
    pass # end of class 


if __name__ == "__main__":
    # for test 
    x = AggreFlowBase("20221110171303887.1_da8ed3")
    x.calculate_intersection_f2f("tmp/job/20221110171303887.1_da8ed3/output/output_party_2_1.csv")
    x.calculate_intersection_f2f("tmp/job/20221110171303887.1_da8ed3/output/output_party_2_3.csv")






    
