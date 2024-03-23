import hashlib
import random

# 接收数据
from mpc.protocol.OT import cuckoo
from mpc.protocol.OT.receiver_oprf import receiver_oprf


class receiver():
    def __init__(self, key_length, salt, cuckoo) -> None:
        self.cuckoo = cuckoo
        self.cuckoo_rand_bit = []

        self.data_ori = None
        self.key_length = key_length
        self.salt = salt

    def insert_data(self, data_set):
        for i in data_set:
            self.cuckoo.insert(int(i, 16))

    def fill_none(self):
        # 为None搞一个随机值
        for i in range(int(1.2 * self.cuckoo.n)):
            if self.cuckoo.table_full[i] > 0:
                self.cuckoo_rand_bit.append(self.cuckoo.table[i])
            else:
                self.cuckoo_rand_bit.append(random.getrandbits(self.key_length))
        for i in range(self.cuckoo.s):
            if self.cuckoo.stash[i] is not None:
                self.cuckoo_rand_bit.append(self.cuckoo.stash[i])
            else:
                self.cuckoo_rand_bit.append(random.getrandbits(self.key_length))

        print("cuckoo insertion finish")

    def get_res(self, conn):
        # 还没构建cuckoo hash
        if self.cuckoo is None:
            print("please build a cuckoo hash")
            exit(0)
        conn.send(self.cuckoo.n)
        conn.send(self.cuckoo.s)
        oprf = receiver_oprf()
        oprf.send_oprf(keys=self.cuckoo_rand_bit, end=conn)
        tables = [{}, {}, {}]
        stash = {}

        for table in tables:
            while True:
                key = conn.recv()
                if isinstance(key, str):
                    break
                int_num = int(''.join(str(bit) for bit in key), 2)
                table[int_num] = None
        print("get data in bucket finish")

        while True:
            key = conn.recv()
            if isinstance(key, str):
                break
            int_num = int(''.join(str(bit) for bit in key), 2)
            stash[int_num] = None
        print("get data else finish")

        res = []

        # 桶
        for i in range(int(1.2 * self.cuckoo.n)):
            if self.cuckoo.table[i] is not None:
                table = tables[self.cuckoo.table_full[i] - 1]
                local_val = oprf.eval(i)
                # print(key, hash_index,local_val)
                if local_val in table:
                    res.append(self.cuckoo.table[i])
                    conn.send(self.cuckoo.table[i])
        print("classify barrier finish")

        # 对储藏桶做同样操作
        for i, key in enumerate(self.cuckoo.stash):
            if key is not None:
                local_val = oprf.eval(i + int(1.2 * self.cuckoo.n))
                # 找到不经意函数值相同的
                if local_val in stash:
                    res.append(key)
                    conn.send(key)

        print("classify else finish")
        conn.send("ok")


if __name__ == '__main__':
    receiver = receiver()
    receiver.get_data(file_path="data/receiver_data.csv")
    receiver.build_cuckoo()
    print(receiver.cuckoo.table)
