from utils import generate_signal_data
import numpy as np


def signal_dc_flash_with_stop_lose(*args):
    # J神dc闪电侠
    """
    n： 时间窗口参数
    stop_loss_pct： 止损百分比参数
    DC上轨：n天收盘价的最大值
    DC下轨：n天收盘价的最小值
    当收盘价由下向上穿过DC上轨的时候，做多；
    当收盘价由上向下穿过DC下轨的时候，做空；
    flash 平仓。

    :param df:  原始数据
    :param para:  参数，[n, stop_lose]
    :param ma_dict: 均线ma缓存
    :return:
    """
    df = args[0]
    n = args[1]
    ma_dict = {}
    stop_loss_pct = 10

    df['signal'] = np.nan
    holding_times_min = 10

    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    df['flash_stop_win'] = df['median'].copy()
    df['upper'] = df['close'].rolling(window=n).max().shift(1)
    df['lower'] = df['close'].rolling(window=n).min().shift(1)
    df['mtm'] = df['close'] / df['close'].shift(n) - 1

    df['c1'] = df['high'] - df['low']
    df['c2'] = abs(df['high'] - df['close'].shift(1))
    df['c3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=n, min_periods=1).mean()

    condition1 = (df['close'] > df['upper']) & (df['mtm'] > 0)
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 1

    condition1 = df['close'] < df['median']
    condition2 = df['close'].shift(1) >= df['median'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 0

    condition1 = (df['close'] < df['lower']) & (df['mtm'] < 0)
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = -1

    condition1 = df['close'] > df['median']
    condition2 = df['close'].shift(1) <= df['median'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = 0

    info_dict = {
        'pre_signal': 0,
        'stop_lose_price': None,
        'holding_times': 0,
        'stop_win_times': 0,
        'stop_win_price': 0
    }
    df = df.reset_index(drop=True)
    for i in range(df.shape[0]):
        if info_dict['pre_signal'] == 0:
            if df.at[i, 'signal_long'] == 1:
                df.at[i, 'signal'] = 1
                pre_signal = 1
                stop_lose_price = df.at[i, 'close'] * (1 - stop_loss_pct / 100)
                info_dict = {
                    'pre_signal': pre_signal,
                    'stop_lose_price': stop_lose_price,
                    'holding_times': 0,
                    'stop_win_times': 0,
                    'stop_win_price': 0
                }
            elif df.at[i, 'signal_short'] == -1:
                df.at[i, 'signal'] = -1
                pre_signal = -1
                stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100)
                info_dict = {
                    'pre_signal': pre_signal,
                    'stop_lose_price': stop_lose_price,
                    'holding_times': 0,
                    'stop_win_times': 0,
                    'stop_win_price': 0
                }
            else:
                info_dict = {
                    'pre_signal': 0,
                    'stop_lose_price': None,
                    'holding_times': 0,
                    'stop_win_times': 0,
                    'stop_win_price': 0
                }
        elif info_dict['pre_signal'] == 1:
            holding_times = info_dict['holding_times']
            if df.at[i, 'atr'] < df.at[i - 1, 'atr']:
                info_dict['holding_times'] = holding_times + 1
            if df.at[i, 'close'] > df.at[i - 1, 'close']:
                if holding_times > 0:
                    info_dict['holding_times'] = holding_times - 1
                else:
                    info_dict['holding_times'] = 0
            ma_temp = max(n - int(n / 50) * 10 * holding_times,
                          holding_times_min)
            if ma_temp in ma_dict:
                df_ma_temp = ma_dict[ma_temp]
            else:
                df_ma_temp = df['close'].rolling(ma_temp, min_periods=1).mean()
                ma_dict[ma_temp] = df_ma_temp

            df.at[i, 'flash_stop_win'] = df_ma_temp.at[i]

            if df.at[i, 'close'] < df.at[i, 'flash_stop_win']:
                if df.at[i, 'close'] > info_dict[
                        'stop_win_price'] or info_dict['stop_win_times'] == 0:
                    info_dict['stop_win_price'] = df.at[i, 'close']
                    info_dict[
                        'stop_win_times'] = info_dict['stop_win_times'] + 1
                    info_dict['holding_times'] = 0
                else:
                    df.at[i, 'signal_long'] = 0
            if (df.at[i, 'signal_long'] == 0) or (
                    df.at[i, 'close'] < info_dict['stop_lose_price']):
                df.at[i, 'signal'] = 0
                info_dict = {
                    'pre_signal': 0,
                    'stop_lose_price': None,
                    'holding_times': 0,
                    'stop_win_times': 0,
                    'stop_win_price': 0
                }
            if df.at[i, 'signal_short'] == -1:
                df.at[i, 'signal'] = -1
                pre_signal = -1
                stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100)
                info_dict = {
                    'pre_signal': pre_signal,
                    'stop_lose_price': stop_lose_price,
                    'holding_times': 0,
                    'stop_win_times': 0,
                    'stop_win_price': 0
                }
        elif info_dict['pre_signal'] == -1:
            holding_times = info_dict['holding_times']
            if df.at[i, 'atr'] < df.at[i - 1, 'atr']:
                info_dict['holding_times'] = holding_times + 1
            if df.at[i, 'close'] < df.at[i - 1, 'close']:
                if holding_times > 0:
                    info_dict['holding_times'] = holding_times - 1
                else:
                    info_dict['holding_times'] = 0
            ma_temp = max(n - int(n / 50) * 10 * holding_times,
                          holding_times_min)
            if ma_temp in ma_dict:
                df_ma_temp = ma_dict[ma_temp]
            else:
                df_ma_temp = df['close'].rolling(ma_temp, min_periods=1).mean()
                ma_dict[ma_temp] = df_ma_temp
            df.at[i, 'flash_stop_win'] = df_ma_temp.at[i]
            if df.at[i, 'close'] > df.at[i, 'flash_stop_win']:
                if df.at[i, 'close'] < info_dict[
                        'stop_win_price'] or info_dict['stop_win_times'] == 0:
                    info_dict['stop_win_price'] = df.at[i, 'close']
                    info_dict[
                        'stop_win_times'] = info_dict['stop_win_times'] + 1
                    info_dict['holding_times'] = 0
                else:
                    df.at[i, 'signal_short'] = 0

            if (df.at[i, 'signal_short'] == 0) or (
                    df.at[i, 'close'] > info_dict['stop_lose_price']):
                df.at[i, 'signal'] = 0
                info_dict = {
                    'pre_signal': 0,
                    'stop_lose_price': None,
                    'holding_times': 0,
                    'stop_win_times': 0,
                    'stop_win_price': 0
                }
            if df.at[i, 'signal_long'] == 1:
                df.at[i, 'signal'] = 1
                pre_signal = 1
                stop_lose_price = df.at[i, 'close'] * (1 - stop_loss_pct / 100)
                info_dict = {
                    'pre_signal': pre_signal,
                    'stop_lose_price': stop_lose_price,
                    'holding_times': 0,
                    'stop_win_times': 0,
                    'stop_win_price': 0
                }
        else:
            raise ValueError('不可能出现其他的情况，如果出现，说明代码逻辑有误，报错')
    df['pos'] = df['signal'].shift()
    df['pos'].fillna(method='ffill', inplace=True)
    df['pos'].fillna(value=0, inplace=True)
    signal_data = generate_signal_data(df)
    return df, signal_data
