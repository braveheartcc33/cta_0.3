from utils import generate_signal_data


def signal_mtmbbw_bolling(*args):
    df = args[0]
    n = args[1]

    # 求上、下轨的倍数
    df['diff_c'] = df['close'] / df['close'].shift(n)

    # ===计算指标
    # 计算均线
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    # 计算上轨、下轨道
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)
    # ddof代表标准差自由度
    df['upper'] = df['median'] + df['std'] * (
        df['diff_c'] + df['diff_c'] ** (-1))
    df['lower'] = df['median'] - df['std'] * (
        df['diff_c'] + df['diff_c'] ** (-1))
    df['mouth'] = df['upper'] - df['lower']
    df['mouth_m'] = df['mouth'].rolling(n).mean()

    # ===计算信号
    # 找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    # 之前K线的收盘价 <= 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)
    # 将产生做多信号的那根K线的signal设置为1，1代表做多
    df.loc[condition1 & condition2, 'signal_long'] = 1

    # 找出做多平仓信号
    condition1 = df['mouth'] < df['mouth_m']
    # 之前K线的收盘价 >= 跌幅信号线
    condition2 = df['mouth'].shift(1) >= df['mouth_m'].shift(1)
    # 当前K线的收盘价 < 跌幅信号线
    condition3 = df['close'] < df['median']
    # 之前K线的收盘价 >= 跌幅信号线
    condition4 = df['close'].shift(1) >= df['median'].shift(1)
    df.loc[(condition1 & condition2) | (
        condition3 & condition4), 'signal_long'] = 0
    # 将产生平仓信号当天的signal设置为0，0代表平仓
    # 找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
    df.loc[condition1 & condition2, 'signal_short'] = -1
    # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

    # 找出做空平仓信号
    condition1 = df['mouth'] < df['mouth_m']
    condition2 = df['mouth'].shift(1) >= df['mouth_m'].shift(1)
    # 之前K线的收盘价 >= 跌幅信号线
    condition3 = df['close'] > df['median']  # 当前K线的收盘价 > 涨幅信号线
    condition4 = df['close'].shift(1) <= df['median'].shift(1)
    # 之前K线的收盘价 <= 涨幅信号线
    df.loc[(condition1 & condition2) | (
        condition3 & condition4), 'signal_short'] = 0
    # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(
        axis=1, min_count=1, skipna=True)
    # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    # ===删除无关变量
    df.drop(
        ['std', 'signal_long', 'signal_short',
         'diff_c', 'mouth', 'mouth_m'], axis=1,
        inplace=True)

    df['median'].fillna(method='bfill', inplace=True)
    df['upper'].fillna(method='bfill', inplace=True)
    df['lower'].fillna(method='bfill', inplace=True)

    signal_data = generate_signal_data(df)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
    ), signal_data
