from utils import generate_signal_data
from factors.utils import calculate_dema
import numpy as np


def mike_stop_with_bias(*args):
    df = args[0]
    n = args[1]
    # stop = para[1]
    n2 = 4 * 24 * 20  # 20日
    # 计算 mike 指标
    df['typ'] = (df['close'] + df['high'] + df['low']) / 3
    df['hh'] = df['high'].rolling(n, min_periods=1).max()
    df['ll'] = df['low'].rolling(n, min_periods=1).min()

    # 计算 bias 指标
    df['ma'] = df['close'].rolling(window=n2, min_periods=1).mean()
    # df['bias'] = (df['close'] - df['ma']) / df['ma'] * 100

    df['sr'] = df['hh'] * 2 - df['ll']
    df['mr'] = df['typ'] + df['hh'] - df['ll']
    df['wr'] = df['typ'] * 2 - df['ll']

    df['ws'] = df['typ'] * 2 - df['hh']
    df['ms'] = df['typ'] - (df['hh'] - df['ll'])
    df['ss'] = df['ll'] * 2 - df['hh']

    close = [float(x) for x in df['close']]
    df['median'] = calculate_dema(np.array(close), timeperiod=n)

    # 当收盘价在初级支撑线与中级支撑线之间或者突破强力压力线时，平空做多；
    cond1 = (df['close'] < df['ws'].shift(1)) & (df['close'] >
                                                 df['ms'].shift(1))
    cond2 = df['close'] > df['sr'].shift(1)
    df.loc[cond1 | cond2, 'signal_long'] = 1
    # bias大于一定值 平多
    # df.loc[df['bias'] > stop, 'signal_long'] = 0
    # 找出做多平仓信号， 触发条件为 穿中轨 或 回撤超过阈值 二者之一
    condition_sell = (df['close'] < df['median']) & (
        df['close'].shift() >= df['median'].shift())  # k线下穿中轨
    df.loc[condition_sell, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 当收盘价在初级压力线和中间压力之间或者跌破强力支撑线时，平多做空。
    cond3 = (df['close'] > df['wr'].shift(1)) & (df['close'] <
                                                 df['mr'].shift(1))
    cond4 = df['close'] < df['ss'].shift(1)
    df.loc[cond3 | cond4, 'signal_short'] = -1
    # bias小于一定值 平空
    # df.loc[df['bias'] < -stop, 'signal_short'] = 0
    # ===找出做空平仓

    condition_cover = (df['close'] > df['median']) & (
        df['close'].shift() <= df['median'].shift())  # K线上穿中轨
    df.loc[condition_cover, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1,
                                                           min_count=1,
                                                           skipna=True)
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    # 删除无关变量
    df.drop([
        'typ', 'hh', 'll', 'sr', 'mr', 'wr', 'ws', 'ms', 'ss', 'signal_long',
        'signal_short'
    ],
            axis=1,
            inplace=True)

    signal_data = generate_signal_data(df)
    return df, signal_data
