from utils import generate_signal_data


def signal_dual_thrust(*args):
    # dual thrust
    df = args[0]
    n = args[1]

    df['hh'] = df['high'].rolling(n, min_periods=1).max()
    df['lc'] = df['close'].rolling(n, min_periods=1).min()
    df['hc'] = df['close'].rolling(n, min_periods=1).max()
    df['ll'] = df['low'].rolling(n, min_periods=1).min()

    condition1 = (df['hh'] - df['lc']) > (df['hc'] - df['ll'])
    condition2 = (df['hh'] - df['lc']) <= (df['hc'] - df['ll'])

    df.loc[condition1, 'range'] = df['hh'] - df['lc']
    df.loc[condition2, 'range'] = df['hc'] - df['ll']

    df['upper_open'] = 2 * abs(df['close'] -
                               df['open'].shift()) / df['range'].rolling(
                                   n, min_periods=1).max()
    df['lower_open'] = 2 * abs(df['open'].shift() -
                               df['close']) / df['range'].rolling(
                                   n, min_periods=1).max()

    df['upper'] = df['open'].shift() + df['upper_open'] * df['range']
    df['lower'] = df['open'].shift() - df['lower_open'] * df['range']

    # close >open + upper_open * range  upper_open <(close - open)/range
    # close <open - lower_open*range lower_open< (open -close)/range
    condition = df['close'] > df['upper']

    condition &= (df['upper'].shift() -
                  df['lower'].shift()) / df['upper'].shift() > 0.05

    df.loc[condition, 'signal_long'] = 1

    condition = df['close'] < df['lower']

    condition &= (df['upper'].shift() -
                  df['lower'].shift()) / df['upper'].shift() > 0.05

    df.loc[condition, 'signal_short'] = -1

    condition = (df['upper'].shift() -
                 df['lower'].shift()) < df['high'].shift() - df['low'].shift()
    condition |= (df['upper'].shift() -
                  df['lower'].shift()) / df['upper'].shift() < 0.05

    df.loc[condition, 'signal_short'] = 0
    df.loc[condition, 'signal_long'] = 0

    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1,
                                                           min_count=1,
                                                           skipna=True)

    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]

    df['signal'] = temp['signal']

    signal_data = generate_signal_data(df)
    return df, signal_data
