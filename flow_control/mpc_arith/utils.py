from mpc_handler.utils import arith_operation
from mpyc.runtime import mpc


def get_operator(operator):
    operators = {
        '+': lambda x, y: x + y,
        '-': lambda x, y: x - y,
        '*': lambda x, y: x * y,
        '/': lambda x, y: x / y,
        '**': lambda x, y: x ** y,
        '==': lambda x, y: x == y,
        '!=': lambda x, y: x != y,
        '>': lambda x, y: x > y,
        '<': lambda x, y: x < y,
        '>=': lambda x, y: x >= y,
        '<=': lambda x, y: x <= y
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
