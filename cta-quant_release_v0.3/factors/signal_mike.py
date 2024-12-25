from utils import generate_signal_data


def signal_mike(*args):
    df = args[0]
    n = args[1]
    df['typ'] = (df['close'] + df['high'] + df['low']) / 3
    df['hh'] = df['high'].rolling(n, min_periods=1).max()
    df['ll'] = df['low'].rolling(n, min_periods=1).min()

    df['sr'] = df['hh'] * 2 - df['ll']
    df['mr'] = df['typ'] + df['hh'] - df['ll']
    df['wr'] = df['typ'] * 2 - df['ll']

    df['ws'] = df['typ'] * 2 - df['hh']
    df['ms'] = df['typ'] - (df['hh'] - df['ll'])
    df['ss'] = df['ll'] * 2 - df['hh']

    condtion1 = (df['close'] < df['ws'].shift()) & (df['close'] >
                                                    df['ms'].shift())
    condtion2 = df['close'] > df['sr'].shift()
    df.loc[(condtion1 | condtion2), 'signal_long'] = 1

    condtion1 = (df['close'] > df['wr'].shift()) & (df['close'] <
                                                    df['mr'].shift())
    condtion2 = df['close'] < df['ss'].shift()
    df.loc[condtion1 | condtion2, 'signal_short'] = -1

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
