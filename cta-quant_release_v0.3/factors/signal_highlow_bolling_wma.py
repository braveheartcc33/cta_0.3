from utils import generate_signal_data
from factors.utils import calculate_wma


def signal_highlow_bolling_wma(*args):
    df = args[0]
    n = int(args[1])

    indicator = 'close'
    df['median'] = calculate_wma(df[indicator], timeperiod=n)
    # 使用WMA, 综合表现优于其他

    df['std'] = (df['high'] - df['low']).rolling(n).mean()
    df['z_score'] = abs(df[indicator] - df['median']) / df['std']
    df['m'] = df['z_score'].rolling(window=n).mean()
    df['upper'] = df['median'] + df['std'] * df['m']
    df['lower'] = df['median'] - df['std'] * df['m']

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

    signal_data = generate_signal_data(df)
    df['median'].fillna(method='bfill', inplace=True)
    df['upper'].fillna(method='bfill', inplace=True)
    df['lower'].fillna(method='bfill', inplace=True)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
    ), signal_data
