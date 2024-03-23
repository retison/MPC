import random
import numpy as np


class receiver_oprf():
    def __init__(self, key_length) -> None:
        self.seed_place = []
        self.XOR = []
        self.key_length = key_length
        pass

    # 数字变为比特列表
    def int_to_bit(self, num, length):
        bit_num = []
        while num > 0:
            bit_num.insert(0, num % 2)
            num = int(num / 2)
        now_length = len(bit_num)
        if now_length < length:
            for _ in range(length - now_length):
                bit_num.insert(0, 0)
        else:
            bit_num = bit_num[now_length - length:]
        return bit_num

    # 生成种子
    def generate_seed(self, seed_size):
        for i in range(seed_size):
            self.seed_place.append(self.int_to_bit(random.getrandbits(self.key_length), self.key_length))
        # print(len(self.seed_place[0]))
        self.seed_place = np.array(self.seed_place)

    # oprf的send端操作
    def send_oprf(self, keys):
        self.generate_seed(len(keys))
        # 根据数据大小生成比特矩阵
        self.bit_data = np.array([self.int_to_bit(i, self.key_length) for i in keys])
        # 比特矩阵和数据比特矩阵取异或
        for i in range(len(self.bit_data)):
            bit_list = [self.seed_place[i][j] ^ self.bit_data[i][j] for j in
                        range(min(len(self.bit_data[i]), len(self.seed_place[i])))]
            self.XOR.append(bit_list)
        self.XOR = np.array(self.XOR)
        will_send_all = []
        # 将随机比特矩阵和异或矩阵都发送给接收端
        for i in range(self.key_length):
            will_Send = self.seed_place[:, i], self.XOR[:, i]
            will_send_all.append(will_Send)
        return will_send_all
        # print("seed\t",self.seed_place)
        # print("XOR    \t",self.XOR)

    # 不经意函数求值
    def eval(self, i):
        ti = self.seed_place[i, :]
        int_num = int(''.join(str(bit) for bit in ti), 2)

        return int_num

# sender_oprf = sender_oprf()
# sender_oprf.generate_seed(2)
# print(sender_oprf.bit_to_bytes([1,0,2,3]))
