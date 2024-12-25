from utils import generate_signal_data
import numpy as np


def signal_adapt_kc(*args):
    df = args[0]
    n = args[1]
    n2 = 3 * n
    df['TR'] = np.max([
        abs(df['high'] - df['low']),
        abs(df['high'] - df['close'].shift(1)),
        abs(df['close'].shift(1) - df['low'].shift(1))
    ],
                      axis=0)
    df['ATR'] = df['TR'].rolling(n2, min_periods=1).mean()
    df['median2'] = df['close'].ewm(span=180, min_periods=1,
                                    adjust=False).mean()
    df['z_score'] = abs(df['close'] - df['median2']) / df['ATR']
    df['m'] = df['z_score'].rolling(window=n2).max().shift()
    df['upper2'] = df['median2'] + df['ATR'] * df['m']
    df['lower2'] = df['median2'] - df['ATR'] * df['m']

    # condition_long = df['close'] > df['upper2']
    # condition_short = df['close'] < df['lower2']
    '''
    计算KC
    TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-REF(LOW,1)))
    ATR=MA(TR,N)
    Middle=EMA(CLOSE,20)
    自适应转换
    UPPER=MIDDLE+2*ATR
    LOWER=MIDDLE-2*ATR
    '''
    # 基于价格因素计算KC通道
    df['TR'] = np.max([
        abs(df['high'] - df['low']),
        abs(df['high'] - df['close'].shift(1)),
        abs(df['close'].shift(1) - df['low'].shift(1))
    ],
                      axis=0)
    df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()
    df['median'] = df['close'].ewm(span=20, min_periods=1, adjust=False).mean()
    df['z_score'] = abs(df['close'] - df['median']) / df['ATR']
    df['m'] = df['z_score'].rolling(window=n).max().shift()
    df['upper'] = df['median'] + df['ATR'] * df['m']
    df['lower'] = df['median'] - df['ATR'] * df['m']

    condition_long = df['upper'] > df['upper2']
    condition_short = df['lower'] < df['lower2']

    # 找出做多信号
    condition1 = (df['close'] > df['upper']) & (df['close'].shift(1) <=
                                                df['upper'].shift(1))
    df.loc[(condition1 & condition_long), 'signal_long'] = 1

    # 找出做多平仓信号
    condition1 = (df['upper'] < df['upper2']) & (df['upper'].shift(1) >=
                                                 df['upper2'].shift(1))
    condition2 = (df['close'] < df['lower']) & (df['close'].shift() >=
                                                df['lower'].shift())
    df.loc[(condition1 | condition2), 'signal_long'] = 0

    # 找出做空信号
    condition1 = (df['close'] < df['lower']) & (df['close'].shift(1) >=
                                                df['lower'].shift(1))
    df.loc[condition1 & condition_short, 'signal_short'] = -1

    # 找出做空平仓信号
    condition1 = (df['lower'] > df['lower2']) & (df['lower'].shift(1) <=
                                                 df['lower2'].shift(1))
    condition2 = (df['close'] > df['upper']) & (df['close'].shift() <=
                                                df['upper'].shift())
    df.loc[condition1 | condition2, 'signal_short'] = 0
    # ========================= 固定代码 =========================

    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1,
                                                           min_count=1,
                                                           skipna=True)

    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    # ========================= 固定代码 =========================
    # 删除无关变量
    df.drop([
        'TR', 'ATR', 'm', 'z_score', 'median2', 'signal_long', 'signal_short'
    ],
            axis=1,
            inplace=True)

    df['median'].fillna(method='bfill', inplace=True)
    df['upper'].fillna(method='bfill', inplace=True)
    df['upper2'].fillna(method='bfill', inplace=True)
    df['lower'].fillna(method='bfill', inplace=True)
    df['lower2'].fillna(method='bfill', inplace=True)

    signal_data = generate_signal_data(df)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
    ), df['upper2'].tolist(), df['lower2'].tolist(), signal_data
