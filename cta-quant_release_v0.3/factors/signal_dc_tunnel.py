from utils import generate_signal_data


def signal_dc_tunnel(*args):
    # 基础dc通道
    df = args[0]
    n = args[1]

    df['mean'] = df['close'].rolling(n).mean()
    df['max'] = df['close'].rolling(n).max().shift()
    df['min'] = df['close'].rolling(n).min().shift()

    factor = "close"

    # 做多信号
    condition1 = df[factor] > df['max']
    condition2 = df[factor].shift() <= df['max'].shift()
    df.loc[condition1 & condition2, 'signal_long'] = 1  # 1代表做多
    # 平多信号
    condition1 = df[factor] < df['mean']
    condition2 = df[factor].shift() >= df['mean'].shift()
    df.loc[condition1 & condition2, 'signal_long'] = 0
    # 做空信号
    condition1 = df[factor] < df['min']
    condition2 = df[factor].shift() >= df['min'].shift()
    df.loc[condition1 & condition2, 'signal_short'] = -1
    # 平空信号
    condition1 = df[factor] > df['mean']
    condition2 = df[factor].shift() <= df['mean'].shift()
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
    signal_data = generate_signal_data(df)
    return df, df['mean'].tolist(), df['max'].tolist(), df['min'].tolist(
    ), signal_data
