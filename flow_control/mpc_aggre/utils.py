import numpy as np

from mpyc.runtime import mpc


def get_operator(operator):
    operators = {
        'max': lambda x: mpc.max(x),
        'min': lambda x: mpc.min(x),
        'sum': lambda x: mpc.sum(x),
        'count': lambda x: len(x),
        'avg': lambda x: mpc.statistics.mean(x)
    }
    if operator in operators:
        return operators[operator]
    else:
        return None


def list_operation(lis, ope):
    operation = get_operator(ope)
    return operation(lis)
