from utils import generate_signal_data
from factors.utils import dc_tunnel_formatter


def mtm_dc_tunnel(*args):
    df = args[0]
    n = args[1]

    df['mtm'] = (df['close'] / df['close'].shift(n) - 1) * 100
    indicator = "mtm"
    df = dc_tunnel_formatter(df, n, indicator)

    signal_data = generate_signal_data(df)
    return df, df['mean'].tolist(), df['max'].tolist(), df['min'].tolist(
        ), signal_data
