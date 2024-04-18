import configparser
import socket
import sys
from sympy import randprime
import random
import math

arith_operation = [
    "+", "-", "*", "/",
    ">", "<", "=", ">=", "<="
]
aggre_operation = [
    "max", "min", "sum", "avg", "count"
]
other_operation = [
    "group by", "limit", "order by"
]


# 读取.ini文件
def read_ini(path) -> list:
    config = configparser.ConfigParser()
    config.read(path)
    location = []
    for i in config:
        if i != "DEFAULT":
            location.append("-P" + config[i]["host"] + ":" + config[i]["port"])
    return location


def quick_mod(num1, num2, num3):
    result = 1
    while num2 > 0:
        if (num2 & 1) == 1:
            result = (result * num1) % num3
        num1 = (num1 * num1) % num3
        num2 = num2 >> 1
    return result


# 检查是否是素数
def isPrime(m):
    k = 100
    i = 1
    while i <= k:
        a = random.randint(2, m - 2)
        g = math.gcd(a, m)
        r = quick_mod(a, m - 1, m)
        if g != 1:
            return False
        elif r != 1:
            return False
        else:
            i += 1
    if i == k + 1:
        return True


# 生成一个大素数
def generate_prim():
    while True:
        prime_number = randprime(2 ** 256, 2 ** 299 - 1)
        if isPrime(prime_number):
            return prime_number

def get_operator(operator):
    operators = {
        '+': lambda x, y: x + y,
        '-': lambda x, y: x - y,
        '*': lambda x, y: x * y,
        '/': lambda x, y: x / y,
        '%': lambda x, y: x % y,
        '**': lambda x, y: x ** y,
        '==': lambda x, y: x == y,
        '!=': lambda x, y: x != y,
        '>': lambda x, y: x > y,
        '<': lambda x, y: x < y,
        '>=': lambda x, y: x >= y,
        '<=': lambda x, y: x <= y,
        'and': lambda x, y: x and y,
        'or': lambda x, y: x or y,
        'not': lambda x: not x
    }
    if operator in operators:
        return operators[operator]
    else:
        return None


def list_operation(lis, ope):
    operation = get_operator(ope)
    if len(lis) < 2 and ope in ["+", "-", "*", "/"]:
        return "you have to give two lists"
    elif len(lis) != 2 and ope in arith_operation:
        return "your operation only can have two lists"

    result = []
    for i, lst in enumerate(lis):
        for num in lst:
            for other_lst in lis[i + 1:]:
                for other_num in other_lst:
                    result.append(operation(num, other_num))
    return result


def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
        return True

def revert_data(i,key):
    if i < 0:
        i += key
    return i