import numpy as np
import pandas as pd


# ATR 计算
def calculate_atr(df, n):
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=n, min_periods=1).mean()
    return atr


# WMA 计算
def calculate_wma(values, period):
    weights = np.arange(1, period + 1)
    return values.rolling(window=period).apply(
        lambda prices: np.dot(prices, weights) / weights.sum(), raw=True)


def EMA(values, period):
    return values.ewm(span=period, adjust=False).mean()


def calculate_dema(values, period):
    ema1 = EMA(values, period)
    ema2 = EMA(ema1, period)
    return 2 * ema1 - ema2


def keltner_channel_formatter(*args):
    df = args[0]
    n = args[1]
    indicator = args[2]
    '''
    计算KC
    TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-REF(LOW,1)))
    ATR=MA(TR,N)
    Middle=EMA(CLOSE,20)
    自适应转换
    UPPER=MIDDLE+2*ATR
    LOWER=MIDDLE-2*ATR
    '''
    # 基于指标计算KC通道
    df['kc_high'] = df[indicator].rolling(n).max().shift()
    df['kc_low'] = df[indicator].rolling(n).min().shift()

    df['TR'] = np.max([abs(df['kc_high'] - df['kc_low'])], axis=0)
    df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()
    df['median'] = df[indicator].ewm(span=20,
                                     min_periods=1, adjust=False).mean()
    df['z_score'] = abs(df[indicator] - df['median']) / df['ATR']
    df['m'] = df['z_score'].rolling(window=n).max().shift(1)
    df['upper'] = df['median'] + df['ATR'] * df['m']
    df['lower'] = df['median'] - df['ATR'] * df['m']

    # 找出做多信号
    condition1 = (df[indicator] > df['upper'])
    condition2 = (df[indicator].shift() <= df['upper'].shift(1))
    df.loc[condition1 & condition2, 'signal_long'] = 1

    # 找出做多平仓信号
    condition1 = (df[indicator] < df['lower'])
    condition2 = (df[indicator].shift() >= df['lower'].shift())
    df.loc[condition1 & condition2, 'signal_long'] = 0

    # 找出做空信号
    condition1 = (df[indicator] < df['lower'])
    condition2 = (df[indicator].shift(1) >= df['lower'].shift(1))
    df.loc[condition1 & condition2, 'signal_short'] = -1

    # 找出做空平仓信号
    condition1 = (df[indicator] > df['upper'])
    condition2 = (df[indicator].shift() <= df['upper'].shift())
    df.loc[condition1 & condition2, 'signal_short'] = 0

    # ========================= 固定代码 =========================

    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1,
                                                           min_count=1,
                                                           skipna=True)

    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    # ========================= 固定代码 =========================

    # 删除无关变量
    df.drop(['TR', 'ATR', 'kc_high', 'kc_low', 'm',
             'z_score', 'signal_long', 'signal_short'],
            axis=1,
            inplace=True)

    df['median'].fillna(method='bfill', inplace=True)
    df['upper'].fillna(method='bfill', inplace=True)
    df['lower'].fillna(method='bfill', inplace=True)

    return df


def dc_tunnel_formatter(*args):
    # 基础dc通道模板
    df = args[0]
    n = args[1]
    indicator = args[2]

    df['mean'] = df[indicator].rolling(n).mean()
    df['max'] = df[indicator].rolling(n).max().shift()
    df['min'] = df[indicator].rolling(n).min().shift()

    # 做多信号
    condition1 = df[indicator] > df['max']
    condition2 = df[indicator].shift() <= df['max'].shift()
    df.loc[condition1 & condition2, 'signal_long'] = 1  # 1代表做多
    # 平多信号
    condition1 = df[indicator] < df['mean']
    condition2 = df[indicator].shift() >= df['mean'].shift()
    df.loc[condition1 & condition2, 'signal_long'] = 0
    # 做空信号
    condition1 = df[indicator] < df['min']
    condition2 = df[indicator].shift() >= df['min'].shift()
    df.loc[condition1 & condition2, 'signal_short'] = -1
    # 平空信号
    condition1 = df[indicator] > df['mean']
    condition2 = df[indicator].shift() <= df['mean'].shift()
    df.loc[condition1 & condition2, 'signal_short'] = 0

    # ===将long和short合并为signal
    df['signal_short'].fillna(method='ffill', inplace=True)
    df['signal_long'].fillna(method='ffill', inplace=True)
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1)
    df['signal'].fillna(value=0, inplace=True)

    temp = df[['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp
    df['mean'].fillna(method='bfill', inplace=True)
    df['max'].fillna(method='bfill', inplace=True)
    df['min'].fillna(method='bfill', inplace=True)

    return df


def bolling_formatter(*args):
    # 布林通道模板
    df = args[0]
    n = args[1]
    indicator = args[2]
    # 使用自适应 m
    df['median'] = df[indicator].rolling(n, min_periods=1).mean()
    df['std'] = df[indicator].rolling(
        n,
        min_periods=1
        ).std(ddof=0)  # ddof代表标准差自由度
    df['z_score'] = abs(df[indicator] - df['median']) / df['std']
    # df['m'] = df['z_score'].rolling(window=n).max().shift()
    # df['m'] = df['z_score'].rolling(window=n).min().shift()
    df['m'] = df['z_score'].rolling(n, min_periods=1).mean().shift()

    # ===计算指标
    # 计算均线
    # 计算上轨、下轨道
    df['upper'] = df['median'] + df['m'] * df['std']
    df['lower'] = df['median'] - df['m'] * df['std']

    df.fillna(method='backfill', inplace=True)

    # 计算bias
    df['bias'] = df['close'] / df['median'] - 1

    # bias_pct 自适应
    df['bias_pct'] = abs(df['bias']).rolling(window=n,
                                             min_periods=1).max().shift()

    # ===计算原始布林策略信号
    # 找出做多信号
    # 当前K线的收盘价 > 上轨
    condition1 = df[indicator] > df['upper']
    # 之前K线的收盘价 <= 上轨
    condition2 = df[indicator].shift(1) <= df['upper'].shift(1)
    df.loc[condition1 & condition2,
           'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # 找出做多平仓信号
    condition1 = df[indicator] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df[indicator].shift(1) >= df['median'].shift(
        1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2,
           'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 找出做空信号
    # 当前K线的收盘价 < 下轨
    condition1 = df[indicator] < df['lower']
    # 之前K线的收盘价 >= 下轨
    condition2 = df[indicator].shift(1) >= df['lower'].shift(1)
    df.loc[condition1 & condition2,
           'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

    # 找出做空平仓信号
    condition1 = df[indicator] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df[indicator].shift(1) <= df['median'].shift(
        1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2,
           'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # ===将long和short合并为signal
    df['signal_short'].fillna(method='ffill', inplace=True)
    df['signal_long'].fillna(method='ffill', inplace=True)
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1)
    df['signal'].fillna(value=0, inplace=True)
    df['raw_signal'] = df['signal']

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

    # ===将signal中的重复值删除
    temp = df[['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp

    df.drop(
        ['raw_signal', 'std', 'bias', 'temp', 'signal_long', 'signal_short'],
        axis=1,
        inplace=True)
    return df
