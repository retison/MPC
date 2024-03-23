import random
import hashlib
import cuckoo
# 发送数据
from mpc.protocol.OT.sender_oprf import sender_oprf


class sender():
    def __init__(self) -> None:
        self.data_ori = None
        self.data = []
        self.res = []
        self.string_lenth = 16
        self.n = 0
        self.s = 0
        pass

    # 接收数据
    def get_data(self, file_path):
        f = open(file_path, 'r', encoding="utf-8", )
        self.data_ori = f.read().splitlines()
        f.close()
        # 需要对data进行去重复
        self.data_ori = list(set(self.data_ori))
        for i in self.data_ori:
            hl = hashlib.md5()
            hl.update(str(i).encode(encoding='utf8'))
            md5 = hl.hexdigest()[-8:]
            self.data.append(md5)
        self.data = [int(data, 16) for data in self.data]
        # print(self.data)

    # 基于MPC+OPRF+cuckoo hash,对于在哈希桶的数据很快
    # 但对于储藏桶只能进行OPRF，因此对于过长数据和过大数据集很慢很慢
    # 由于这是对大量数据进行的操作，基于纯OT的密码学加密方式应该删去（感觉也不好加）
    def send(self, conn):
        self.n = conn.recv()
        self.s = conn.recv()
        oprf = sender_oprf()
        oprf.get_F(conn)
        cuckoo_hash = cuckoo.Cuckoo(self.n, self.s)

        # 桶
        for i in range(3):
            words = random.sample(self.data, len(self.data))
            for word in words:
                h = cuckoo_hash.hash_functions[i](word)
                # 计算不经意函数值
                val = oprf.oprf_eval(h, word)
                conn.send(val)
            conn.send("ok")
        print("send data in bucket finish")

        # 储藏桶进行相似的处理
        n = int(1.2 * self.n)
        for i in range(self.s):
            words = random.sample(self.data, len(self.data))
            for word in words:
                val = oprf.oprf_eval(n + i, word)
                conn.send(val)
            print("进度", i, "/", self.s)
        conn.send("ok")
        print("send data else finish")

        # 等待结果的接收
        while True:
            word = conn.recv()
            if word == "ok":
                break
            self.res.append(word)
        print("get result finish")

        f = open("data/result.csv", 'w')
        # 这里弥补了下，在本地将返回的交集和自身数据再求了个交集，再写入的
        for i in self.data_ori:
            hl = hashlib.md5()
            hl.update(str(i).encode(encoding='utf8'))
            i_ori = i
            md5 = hl.hexdigest()[-8:]
            md5_to_int = int(md5, 16)
            if md5_to_int in self.res:
                # print(i_ori)
                f.write(str(i_ori) + '\n')
        f.close()

        # 用来计算准确率,用随机数重复等问题可能会导致少量问题
        print("交集元素个数：", len(self.res))


if __name__ == '__main__':
    sender = sender()
    sender.get_data(file_path="data/sender_data.csv")
    # sender.build_cuckoo()
    # print(sender.cuckoo.table)
    # print(sender.send(1))
