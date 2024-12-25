from utils import generate_signal_data


def adapt_bolling(*args):
    df = args[0]
    n = args[1]

    # 使用自适应 m
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    df['std'] = df['close'].rolling(n,
                                    min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['z_score'] = abs(df['close'] - df['median']) / df['std']
    # df['m'] = df['z_score'].rolling(window=n).max().shift()
    # df['m'] = df['z_score'].rolling(window=n).min().shift()
    df['m'] = df['z_score'].rolling(n, min_periods=1).mean().shift()

    # ===计算指标
    # 计算均线
    # 计算上轨、下轨道
    df['upper'] = df['median'] + df['m'] * df['std']
    df['lower'] = df['median'] - df['m'] * df['std']

#     df.fillna(method='backfill', inplace=True)
    df.bfill()

    # 计算bias
    df['bias'] = df['close'] / df['median'] - 1

    # bias_pct 自适应
    df['bias_pct'] = abs(df['bias']).rolling(window=n,
                                             min_periods=1).max().shift()

    # ===计算原始布林策略信号
    # 找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
    df.loc[condition1 & condition2,
           'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # 找出做多平仓信号
    condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(
        1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2,
           'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
    df.loc[condition1 & condition2,
           'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

    # 找出做空平仓信号
    condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(
        1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2,
           'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # ===将long和short合并为signal
#     df['signal_short'].fillna(method='ffill', inplace=True)
#     df['signal_long'].fillna(method='ffill', inplace=True)
    df['signal_short'] = df['signal_short'].ffill()
    df['signal_long'] = df['signal_long'].ffill()

    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1)
#     df['signal'].fillna(value=0, inplace=True)
    df['signal'] = df['signal'].fillna(value=0)
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
#     df['temp'].fillna(method='ffill', inplace=True)
    df['temp'] = df['temp'].ffill()
    df['signal'] = df['temp']

    # ===将signal中的重复值删除
    temp = df[['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp

    df.drop(
        ['raw_signal', 'std', 'bias', 'temp', 'signal_long', 'signal_short'],
        axis=1,
        inplace=True)

    signal_data = generate_signal_data(df)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
    ), signal_data
