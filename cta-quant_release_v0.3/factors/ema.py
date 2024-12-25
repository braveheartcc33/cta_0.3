from utils import generate_signal_data


def ema(*args):
    df = args[0]
    n = args[1]
    df['ema'] = df['close'].ewm(n, adjust=False).mean()

    condition1 = df['close'] > df['ema']
    condition2 = df['close'].shift(1) <= df['ema'].shift(1)
    df.loc[condition1 & condition2, 'signal'] = 1

    condition1 = df['close'] < df['ema']
    condition2 = df['close'].shift(1) >= df['ema'].shift(1)
    df.loc[condition1 & condition2, 'signal'] = -1

    signal_data = generate_signal_data(df)
    return df, df['ema'].tolist(), signal_data
