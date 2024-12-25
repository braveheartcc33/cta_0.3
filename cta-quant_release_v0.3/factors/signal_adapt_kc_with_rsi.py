from utils import generate_signal_data
import numpy as np


def signal_adapt_kc_with_rsi(*args):
    df = args[0]
    n = args[1]
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
    df['m'] = df['z_score'].rolling(window=n).max().shift(1)
    df['upper'] = df['median'] + df['ATR'] * df['m']
    df['lower'] = df['median'] - df['ATR'] * df['m']

    # RSI
    # CLOSEUP=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
    df['closeup'] = np.where(df['close'] > df['close'].shift(),
                             df['close'] - df['close'].shift(), 0)
    # # CLOSEDOWN=IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CL OSE,1)),0)
    df['closedown'] = np.where(df['close'] < df['close'].shift(),
                               abs(df['close'] - df['close'].shift()), 0)
    # # CLOSEUP_MA=SMA(CLOSEUP,N,1)
    # df['data'].ewm(alpha=1 / 2, adjust=False).mean()
    df['closeup_ma'] = df['closeup'].ewm(alpha=1 / 2, adjust=False).mean()
    # # CLOSEDOWN_MA=SMA(CLOSEDOWN,N,1)
    df['closedown_ma'] = df['closedown'].ewm(alpha=1 / 2, adjust=False).mean()
    # # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)
    df['rsi'] = 100 * df['closeup_ma'] / (df['closeup_ma'] +
                                          df['closedown_ma'])
    # RSI_MIDDLE=MA(RSI,N)
    # df['rsi_middle'] = df['rsi'].rolling(n, min_periods=1).mean().shift()
    # # RSI_UPPER=RSI_MIDDLE+PARAM*STD(RSI,N)
    # df['z_score'] = abs(df['closeup_ma'] - df['rsi_middle']) / df['rsi']
    # df['m'] = df['z_score'].rolling(window=n).max().shift()
    # df['rsi_std'] = df['rsi'].rolling(n, min_periods=1).std(ddof=0)
    # # RSI_LOWER=RSI_MIDDLE-PARAM*STD(RSI,N)
    # df['rsi_lower'] = df['rsi_middle'] - df['m'] * df['rsi_std']
    # df['rsi_upper'] = df['rsi_middle'] + df['m'] * df['rsi_std']
    # 找出做多信号
    condition1 = (df['close'] > df['upper']) & (
        df['close'].shift(1) <= df['upper'].shift(1)) & (df['rsi'] > 70)
    # df.loc[(condition1 & condition_long), 'signal_long'] = 1
    df.loc[(condition1), 'signal_long'] = 1

    # 找出做多平仓信号
    condition1 = (df['rsi'] < 65)
    condition2 = (df['close'] < df['lower']) & (df['close'].shift() >=
                                                df['lower'].shift())
    df.loc[(condition1 & condition2), 'signal_long'] = 0

    # 找出做空信号
    condition1 = (df['close'] < df['lower']) & (
        df['close'].shift(1) >= df['lower'].shift(1)) & (df['rsi'] < 30)
    # df.loc[condition1 & condition_short, 'signal_short'] = -1
    df.loc[condition1, 'signal_short'] = -1

    # 找出做空平仓信号
    condition1 = (df['rsi'] > 35)
    condition2 = (df['close'] > df['upper']) & (df['close'].shift() <=
                                                df['upper'].shift())
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
    df.drop(['TR', 'ATR', 'm', 'z_score', 'signal_long', 'signal_short'],
            axis=1,
            inplace=True)

    df['median'].fillna(method='bfill', inplace=True)
    df['upper'].fillna(method='bfill', inplace=True)
    df['lower'].fillna(method='bfill', inplace=True)

    signal_data = generate_signal_data(df)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
    ), signal_data
