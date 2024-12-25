from utils import generate_signal_data
from factors.utils import calculate_atr, calculate_wma
import numpy as np


def signal_atrbolling_bias_wma(*args):
    df = args[0]
    n = args[1]
    # ----计算atr和std
    df['atr'] = calculate_atr(df, n)
    df['std'] = df['close'].rolling(window=n).std(ddof=0)

    # -----计算中轨以及atr和std的倍数

    # ---中轨
    close = [float(x) for x in df['close']]
    df['median'] = calculate_wma(np.array(close), timeperiod=n)
    # df['median'] = df['close'].rolling(window=n).mean()

    # ---atr，std倍数
    df['atr_J神'] = abs(df['close'] - df['median']) / df['atr']
    df['m_atr'] = df['atr_J神'].rolling(window=n).max().shift(1)
    df['boll_J神'] = abs(df['close'] - df['median']) / df['std']
    df['m_boll'] = df['boll_J神'].rolling(window=n).max().shift(1)

    # ---分别计算atr，布林通道上下轨
    df['upper_atr'] = df['median'] + df['m_atr'] * df['atr']
    df['lower_atr'] = df['median'] - df['m_atr'] * df['atr']

    df['upper_boll'] = df['median'] + df['m_boll'] * df['std']
    df['lower_boll'] = df['median'] - df['m_boll'] * df['std']

    # ----将两个上下轨揉在一起。取MIN开仓太频繁，取MAX开仓太少，最终取mean
    df['upper'] = df[['upper_atr', 'upper_boll']].mean(axis=1)
    df['lower'] = df[['lower_atr', 'lower_boll']].mean(axis=1)

    # 计算bias
    df['bias'] = df['close'] / df['median'] - 1
    # bias_pct 自适应
    df['bias_pct'] = abs(df['bias']).rolling(window=n).max().shift()

    # -----计算开仓

    condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(
        1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2,
           'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # ===找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
    df.loc[condition1 & condition2,
           'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # ===找出做空平仓信号
    condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(
        1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2,
           'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # ===找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
    df.loc[condition1 & condition2,
           'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

    # 合并做多做空信号，去除重复信号
    df['signal_short'].fillna(method='ffill', inplace=True)
    df['signal_long'].fillna(method='ffill', inplace=True)
    df['signal'] = df[['signal_long', 'signal_short'
                       ]].sum(axis=1, min_count=1,
                              skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    df['signal'].fillna(value=0, inplace=True)

    # ===根据bias，修改开仓时间
    df['temp'] = df['signal']

    # 将原始信号做多时，当bias大于阀值，设置为空
    condition1 = (df['signal'] == 1)
    condition2 = (df['bias'] > df['bias_pct'])
    df.loc[condition1 & condition2, 'temp'] = None

    # 将原始信号做空时，当bias大于阀值，设置为空
    condition1 = (df['signal'] == -1)
    condition2 = (df['bias'] < -df['bias_pct'])
    df.loc[condition1 & condition2, 'temp'] = None

    # 原始信号刚开仓，并且大于阀值，将信号设置为0
    condition1 = (df['signal'] != df['signal'].shift(1))
    condition2 = (df['temp'].isnull())
    df.loc[condition1 & condition2, 'temp'] = 0

    # 使用之前的信号补全原始信号
    df['temp'].fillna(method='ffill', inplace=True)
    df['signal'] = df['temp']

    temp = df[['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp
    # 将无关的变量删除
    df.drop(['signal_long', 'signal_short'], axis=1, inplace=True)
    df['median'].fillna(method='bfill', inplace=True)
    df['upper'].fillna(method='bfill', inplace=True)
    df['lower'].fillna(method='bfill', inplace=True)

    signal_data = generate_signal_data(df)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
    ), signal_data
