import numpy as np


def get_operator(operator):
    operators = {
        'max': lambda x: max(x),
        'min': lambda x: min(x),
        'sum': lambda x: sum(x),
        'count': lambda x: len(x),
        'avg': lambda x: np.mean(x)
    }
    if operator in operators:
        return operators[operator]
    else:
        return None


def list_operation(lis, ope):
    operation = get_operator(ope)
    return operation(lis)
