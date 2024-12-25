import pandas as pd
import os
import sys
from config_manager import ConfigManager
from database.database_manager import DatabaseManager
from utils import log_print
root_path = os.path.abspath(os.path.dirname(__file__))
root_path = os.path.join(root_path, '..')
sys.path.append(root_path)


if __name__ == '__main__':
    config_path = '/root/cta-quant/heaven.json'
    config_manager = ConfigManager(config_path)
    database_mgr = DatabaseManager(config_manager)
    strategy = config_manager.get('strategy', None)
    trade_type = config_manager.get('trade_type', None)
    is_pm = config_manager.get('is_pm', True)
    # is_running = 0
    try:
        df = pd.read_csv(f'{root_path}/tools/cta.csv')
    except Exception:
        df = pd.DataFrame()

    if df.empty or df is None:
        log_print("strategy.csv is empty", level='error')
    else:
        for index, row in df.iterrows():
            data = {}
            data['strategy'] = strategy
            data['trade_type'] = trade_type
            data['is_pm'] = is_pm
            data['is_running'] = row['is_running']
            data['symbol'] = row['symbol']
            data['interval'] = row['interval']
            data['cta'] = row['cta']
            data['period'] = row['period']
            data['position_amount'] = 0
            data['is_tpsl'] = 0
            # data['profit'] = row['profit']
            data['init_value'] = row['init_value']
            data['net_value'] = row['net_value']
            data['trade_ratio'] = row['trade_ratio']
            data['takeprofit_percentage'] = row['takeprofit_percentage']
            data['stoploss_percentage'] = row['stoploss_percentage']
            data['takeprofit_drawdown_percentage'] = row[
                'takeprofit_drawdown_percentage']
            database_mgr.create_cta_strategy(data)
