from utils import generate_signal_data


def signal_highlow_bolling(*args):
    df = args[0]
    n = args[1]

    indicator = 'close'
    # df['median'] = talib.WMA(df[indicator], timeperiod=n)  # 使用WMA, 综合表现优于其他
    # df['median'] = talib.DEMA(df[indicator], timeperiod=n)
    df['median'] = df[indicator].rolling(n, min_periods=1).mean()

    df['std'] = (df['high'] - df['low']).rolling(n, min_periods=1).mean()
    df['z_score'] = abs(df[indicator] - df['median']) / df['std']
    df['m'] = df['z_score'].rolling(window=n, min_periods=1).mean()
    df['upper'] = df['median'] + df['std'] * df['m']
    df['lower'] = df['median'] - df['std'] * df['m']

    # 为了画图补全一下，不影响实际信号
    df.fillna(method='backfill', inplace=True)

    condition1 = df['close'] > df['upper']
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 1

    condition1 = df['close'] < df['median']
    condition2 = df['close'].shift(1) >= df['median'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 0

    condition1 = df['close'] < df['lower']
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = -1

    condition1 = df['close'] > df['median']
    condition2 = df['close'].shift(1) <= df['median'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = 0

    df['signal_short'].fillna(method='ffill', inplace=True)
    df['signal_long'].fillna(method='ffill', inplace=True)
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1)
    df['signal'].fillna(value=0, inplace=True)
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    df['median'].fillna(method='bfill', inplace=True)
    df['upper'].fillna(method='bfill', inplace=True)
    df['lower'].fillna(method='bfill', inplace=True)
    signal_data = generate_signal_data(df)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
    ), signal_data
