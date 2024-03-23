import random
import numpy as np


class sender_oprf():
    def __init__(self, key_length):
        self.key_length = key_length
        self.select_bit = np.array(self.int_to_bit(random.getrandbits(key_length)))
        self._q = None

    # int变为bit列表
    def int_to_bit(self, num):
        bit_num = []
        while num > 0:
            bit_num.insert(0, num % 2)
            num = int(num / 2)
        now_lenth = len(bit_num)
        if now_lenth < self.key_length:
            for _ in range(self.key_length - now_lenth):
                bit_num.insert(0, 0)
        else:
            bit_num = bit_num[now_lenth - self.key_length:]
        return bit_num

    # OPRF部分根据一个随机数来得到receiver端的随机数矩阵
    def get_F(self, random_bits):
        self._q = []
        count = 0
        for i in self.select_bit:
            pair = random_bits[2 * count + i]
            count += 1
            self._q.append(pair)
        self._q = np.vstack(self._q).T

    # 为了方便调试将计算不经意函数操作写为一个函数
    def eval_op(self, i, data):
        data = np.array(data)
        line = self._q[i, :]
        # print("line\t",line)
        # print("random\t", self.select_bit)
        # print("data\t",data)
        # print("mid_res\t",self.select_bit & data)
        result = line ^ (self.select_bit & data)
        # print("result\t",result)
        return result

    # 计算不经意伪随机函数值
    def oprf_eval(self, position_hash, data):
        bit_data = self.int_to_bit(data)
        res = self.eval_op(position_hash, bit_data)
        return res
