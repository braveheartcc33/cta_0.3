import random


def random_signal(*args):
    """
    随机发出交易信号
    :param df:
    :param now_pos:
    :param avg_price:
    :param para:
    :return:
    """
    r = random.random()
    if r <= 0.25:
        return 1
    elif r <= 0.5:
        return 0
    elif r <= 0.75:
        return -1
    else:
        return None
