import hashlib
import random


# 这几个哈希函数是否过于不安全了呢？？？
class Cuckoo:
    def __init__(self, n, s, key_length, salt):
        self.n = n
        self._12n = int(1.2 * n)
        self.s = s
        self.key_length = key_length
        self.salt = salt
        # 桶
        self.table = [None] * int(n * 1.2)
        self.table_full = [0] * int(n * 1.2)
        # 储藏桶，在这里仍然是一个哈希桶，允许极低的错误率
        self.stash = [None] * self.s
        self.stash_count = 0
        self.stash_full = [0] * self.s
        # 操作表，似乎为了安全多加会更好？
        self.hash_functions = [self.hash1, self.hash2, self.hash3, self.hash4, self.hash5]

    # 在几个哈希函数的位置(主表3个，副表2个)
    def hash1(self, key):
        data = (1 + key + self.salt).to_bytes(self.key_length, "big")
        length = (self.n.bit_length() + 7) // 8
        h = hashlib.sha256(data).digest()[:length]
        val = int.from_bytes(h, "big")
        return val % self._12n

    def hash2(self, key):
        data = (2 + key + self.salt).to_bytes(self.key_length, "big")
        length = (self.n.bit_length() + 7) // 8
        h = hashlib.sha256(data).digest()[:length]
        val = int.from_bytes(h, "big")
        return val % self._12n

    def hash3(self, key):
        data = (3 + key + self.salt).to_bytes(self.key_length, "big")
        length = (self.n.bit_length() + 7) // 8
        h = hashlib.sha256(data).digest()[:length]
        val = int.from_bytes(h, "big")
        return val % self._12n

    def hash4(self, key):
        data = (4 + key + self.salt).to_bytes(self.key_length, "big")
        length = (self.n.bit_length() + 7) // 8
        h = hashlib.sha256(data).digest()[:length]
        val = int.from_bytes(h, "big")
        return val % self.s

    def hash5(self, key):
        data = (5 + key + self.salt).to_bytes(self.key_length, "big")
        length = (self.n.bit_length() + 7) // 8
        h = hashlib.sha256(data).digest()[:length]
        val = int.from_bytes(h, "big")
        return val % self.s

    # 插入
    def insert(self, key):
        for i in range(500):
            index1 = self.hash1(key)
            if self.table[index1] is None:
                self.table[index1] = key
                self.table_full[index1] = 1
                return
            index2 = self.hash2(key)
            if self.table[index2] is None:
                self.table[index2] = key
                self.table_full[index2] = 2
                return
            index3 = self.hash3(key)
            if self.table[index3] is None:
                self.table[index3] = key
                self.table_full[index3] = 3
                return
            # 三个哈希函数相应位置都满了
            j = random.randrange(3)
            h = [index1, index2, index3][j]
            old_data = self.table[h]
            self.table[h] = key
            self.table_full[h] = j + 1
            key = old_data

        # 进储藏桶更新
        for i in range(500):
            index4 = self.hash4(key)
            if self.stash[index4] is None:
                self.stash[index4] = key
                self.stash_full[index4] = 1
                return
            index5 = self.hash5(key)
            if self.stash[index5] is None:
                self.stash[index5] = key
                self.stash_full[index5] = 2
                return

            # 哈希函数相应位置都满了
            j = random.randrange(2)
            h = [index4, index5][j]
            old_data = self.stash[h]
            self.stash[h] = key
            self.stash_full[h] = j + 1
            key = old_data
        # 不能存入的直接抛弃

    # 查找
    def lookup(self, key):
        index1 = self.hash1(key)
        index2 = self.hash2(key)
        index3 = self.hash3(key)
        index4 = self.hash4(key)
        index5 = self.hash5(key)
        if self.table[index1] == key or self.table[index2] == key or self.table[index3] == key:
            return True
        elif self.stash[index4] == key or self.stash[index5] == key:
            # 在储藏桶吗
            return True
        # 都不在
        return False

# cuckoo = Cuckoo(20,10)
# data = [1, 5, 9, 12, 15, 16, 20, 22, 25, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,40,41,42,42.5,43,44,45,46,47,48,49,50]
# for i in data:
#     cuckoo.insert(i)
# print(cuckoo.lookup(15))
# print(cuckoo.lookup(40))
# print(cuckoo.table)
# print(cuckoo.stash)
