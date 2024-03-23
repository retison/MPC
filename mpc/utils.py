# 以下2个函数可以将字符串转换为long，用于后续的mpc操作
# 比单纯用 ord 方法得到的 long 小很多，但同样准确
import Crypto
from Crypto.Util.number import bytes_to_long, long_to_bytes
from gmpy2 import mpz

# 把字符串转换为整型
def string_to_long(input_str):
    return bytes_to_long(input_str.encode())

# 把字符串转换为字符串，目前我们就支持utf-8编码格式
def long_to_str(input_long):
    return long_to_bytes(input_long).decode("utf-8")

# 将文本读取成 public key 对象，可以直接传入 client 对象
def get_rsa_key(input_key_str):
    return Crypto.PublicKey.RSA.importKey(input_key_str)

def mpz_list_to_string(mpz_list):
    res = []
    for i in mpz_list:
        res.append(str(i))
    return res

def string_to_mpz_list(input_str_list):
    res = []
    for i in input_str_list:
        res.append(mpz(i))
    return res 
