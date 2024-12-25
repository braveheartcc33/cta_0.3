from utils import generate_signal_data
from factors.utils import keltner_channel_formatter


def mtm_keltner_channel(*args):
    df = args[0]
    n = args[1]

    df['mtm'] = (df['close'] / df['close'].shift(n) - 1) * 100
    indicator = "mtm"
    df = keltner_channel_formatter(df, n, indicator)

    signal_data = generate_signal_data(df)
    return df, df['median'].tolist(), df['upper'].tolist(), df['lower'].tolist(
        ), signal_data
