from utils import generate_signal_data


def signal_simple_turtle(*args):
    """
    今天收盘价突破过去20天中的收盘价和开盘价中的最高价，做多。今天收盘价突破过去10天中的收盘价的最低价，平仓。
    今天收盘价突破过去20天中的收盘价和开盘价中的的最低价，做空。今天收盘价突破过去10天中的收盘价的最高价，平仓。
    :param para: [参数1, 参数2]
    :param df:
    :return:
    """
    df = args[0]
    n = args[1]

    df['open_close_high'] = df[['open', 'close']].max(axis=1)
    df['open_close_low'] = df[['open', 'close']].min(axis=1)
    # 最近n1日的最高价、最低价
    df['n_high'] = df['open_close_high'].rolling(n, min_periods=1).max()
    df['n_low'] = df['open_close_low'].rolling(n, min_periods=1).min()
    # # 最近n2日的最高价、最低价
    # df['n2_high'] = df['open_close_high'].rolling(n2, min_periods=1).max()
    # df['n2_low'] = df['open_close_low'].rolling(n2, min_periods=1).min()

    # #计算bbi
    # df["ma_low"] = df['open_close_low'].rolling(n1, min_periods=1).mean()
    # df['bbi_low'] = df['ma_low'].rolling(window=n1).mean()

    # df["ma_high"] = df['open_close_high'].rolling(n1, min_periods=1).mean()
    # df['bbi_high'] = df['ma_high'].rolling(window=n1).mean()

    # # dema平仓
    # close = [float(x) for x in df['close']]
    # df['median'] = talib.DEMA(np.array(close), timeperiod=n)

    # ema平仓
    df['median'] = df['close'].ewm(n, adjust=False).mean()
    df.fillna(method='backfill', inplace=True)
    # df['medianstd'] = talib.WMA(np.array(close), timeperiod=n1)
    # df['emastd'] = talib.EMA(np.array(close), timeperiod=n1)

    # dmacd
    # ===找出做多信号
    # 当天的收盘价 > n1日的最高价，做多
    condition = (df['close'] > df['n_high'].shift(1))
    # 将买入信号当天的signal设置为1
    df.loc[condition, 'signal_long'] = 1
    # ===找出做多平仓
    # 当天的收盘价 < n2日的最低价，多单平仓
    # condition = (df['close'] < df['n2_low'].shift(1))

    # # 将卖出信号当天的signal设置为0
    # df.loc[condition, 'signal_long'] = 0

    # 找出做多平仓信号， 触发条件为 穿中轨 或 回撤超过阈值 二者之一
    condition_sell = (df['close'] < df['median']) & (
        df['close'].shift() >= df['median'].shift())  # k线下穿中轨
    df.loc[condition_sell, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # ===找出做空信号
    # 当天的收盘价 < n1日的最低价，做空
    condition = (df['close'] < df['n_low'].shift(1))
    df.loc[condition, 'signal_short'] = -1
    # ===找出做空平仓
    # 当天的收盘价 > n2日的最高价，做空平仓
    # condition = (df['close'] > df['n2_high'].shift(1))

    # # 将卖出信号当天的signal设置为0
    # df.loc[condition, 'signal_short'] = 0

    condition_cover = (df['close'] > df['median']) & (
        df['close'].shift() <= df['median'].shift())  # K线上穿中轨
    df.loc[condition_cover, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short'
                       ]].sum(axis=1, min_count=1,
                              skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    # 将无关的变量删除
    df.drop(
        ['signal_long', 'signal_short', 'open_close_high', 'open_close_low'],
        axis=1,
        inplace=True)
    df['median'].fillna(method='bfill', inplace=True)
    df['n_high'].fillna(method='bfill', inplace=True)
    df['n_low'].fillna(method='bfill', inplace=True)

    signal_data = generate_signal_data(df)

    return df, df['median'].tolist(), df['n_high'].tolist(
    ), df['n_low'].tolist(), signal_data
