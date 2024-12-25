import os
import pytz
from datetime import datetime, timedelta
import pandas as pd
from config_manager import ConfigManager
# from . import binance
from config import (bmac_base_dir,
                    bmac_expire_sec,
                    bmac_min_candle_num,
                    bmac_interval)
from utils import log_print
import time


class DataCenterManager(object):
    '''
    数据中心管理类,用于定义与bmac的数据交互
    TODO
    '''
    def __init__(self, config_manager: ConfigManager) -> None:
        self.config_manager = config_manager
        self.DEFAULT_TZ = pytz.timezone('hongkong')
        # self._init_binance(bmac_base_dir)
        self.candle_data_path = os.path.join(bmac_base_dir,
                                             f'candle_{bmac_interval}')
        self.exg_data_path = os.path.join(bmac_base_dir,
                                          f'exginfo_{bmac_interval}')
        self._init_settings()

    def _init_task(self):
        pass

    def _init_settings(self):
        self._init_task()

        # run_time = datetime.now(self.DEFAULT_TZ).replace(second=0,
        #                                                  microsecond=0)
        # self.fetch_klines("BTCUSDT", "15m", run_time)

    # def _init_binance(self, base_dir):
    #     binance.BinanceDataCenter(base_dir)

    # def _init_okex(self,base_dir):
    #     pass

    def get_klines(self, exchange, symbol, interval, run_time):
        if exchange == 'binance':
            return self.fetch_bmac_klines(symbol, interval, run_time)
        elif exchange == 'okex':
            # return self.fetch_okx_klines(symbol, interval)
            raise RuntimeError('okex not supported yet')

    def format_ready_file_path(self, symbol, run_time):
        '''
        获取 ready file 文件路径, ready file 为每周期 K线文件锁
        ready file 文件名形如 {symbol}_{runtime年月日}_{runtime_时分秒}.ready
        '''
        run_time_str = run_time.strftime('%Y%m%d_%H%M%S')
        name = f'{symbol}_{run_time_str}.ready'
        if symbol == 'exginfo':
            file_path = os.path.join(self.exg_data_path, name)
        else:
            file_path = os.path.join(self.candle_data_path, name)
        return file_path

    def check_ready(self, symbol, interval, run_time):
        '''
        检查 symbol 对应的 ready file 是否存在，并且时间是否满足要求
        '''
        if interval not in ['5m', '15m', '30m', '1h', '4h']:
            raise ValueError("Invalid interval. Allowed intervals: \
                             '5m', '15m', '30m', '1h', '4h'")

        interval_minutes = {
            '5m': 5,
            '15m': 15,
            '30m': 30,
            '1h': 60,
            '4h': 240
        }
        if interval.endswith('h'):
            target_time = run_time.replace(minute=0, second=0, microsecond=0)
        else:
            target_time = run_time.replace(second=0, microsecond=0)
            minute = target_time.minute
            target_time = target_time.replace(minute=(
                minute // interval_minutes[interval]
                ) * interval_minutes[interval])

        if symbol == 'exginfo':
            for file in os.listdir(self.exg_data_path):
                if file.startswith(symbol) and file.endswith('.ready'):
                    file_time_str = file.split(f'{symbol}_')[-1].replace(
                        '.ready', '')
                    file_time = datetime.strptime(
                        file_time_str, '%Y%m%d_%H%M%S'
                        ).replace(tzinfo=self.DEFAULT_TZ)
                    if file_time >= target_time:
                        return True
        else:
            for file in os.listdir(self.candle_data_path):
                if file.startswith(symbol) and file.endswith('.ready'):
                    file_time_str = file.split(f'{symbol}_')[-1].replace(
                        '.ready', '')
                    file_time = datetime.strptime(file_time_str,
                                                  '%Y%m%d_%H%M%S').replace(
                                                      tzinfo=self.DEFAULT_TZ)
                    if file_time >= target_time:
                        return True
        return False

    def read_candle(self, symbol) -> pd.DataFrame:
        '''
        读取 symbol 对应的 K线
        '''
        return pd.read_parquet(os.path.join(self.candle_data_path,
                                            f'{symbol}.pqt'))

    def _fetch_candle_data(self, symbol,
                           interval,
                           run_time,
                           expire_sec,
                           min_candle_num):
        expire_time = run_time + timedelta(seconds=expire_sec)
        now_time = datetime.now(self.DEFAULT_TZ)

        is_ready = self.wait_until_ready(symbol, interval,
                                         run_time, expire_time)

        if not is_ready:
            raise RuntimeError(f'{symbol} not ready at {now_time}')

        df = self.read_candle(symbol)
        resampled_df = self.resample_klines(df, interval)
        if len(resampled_df) < min_candle_num:
            log_print('no enough data', symbol)
            return {}
        resampled_df['symbol'] = symbol
        log_print(f'get {symbol} kline, length= {len(resampled_df)}',
                  level='debug')
        if now_time > expire_time:
            return {}
        return resampled_df

    def fetch_bmac_klines(self, symbol, interval, run_time):
        if run_time.tzinfo is None:
            run_time = run_time.replace(tzinfo=self.DEFAULT_TZ)
        now_time = datetime.now(self.DEFAULT_TZ)
        expire_time = run_time + timedelta(seconds=bmac_expire_sec)
        is_ready = self.wait_until_ready('exginfo',
                                         interval,
                                         run_time,
                                         expire_time)

        if not is_ready:
            raise RuntimeError(f'{symbol} not ready at {now_time}')
        symbol_candle_data = self._fetch_candle_data(symbol,
                                                     interval,
                                                     run_time,
                                                     bmac_expire_sec,
                                                     bmac_min_candle_num)
        return symbol_candle_data

    def get_fundingrate(self, run_time):
        pass
    #     # 从 BMAC 读取资金费
    #     expire_time = run_time + timedelta(seconds=bmac_expire_sec)
    #     bmac_base_dir = os.path.join(bmac_base_dir, 'funding')
    #     is_ready = self.wait_until_ready(exg_mgr, 'funding',
    #                                      run_time, expire_time)

    #     if not is_ready:
    #         raise RuntimeError(f'Funding rate not ready')

    #     return quant.exg_mgr.read_candle('funding')

    def wait_until_ready(self, symbol, interval, run_time, expire_time):
        now_time = datetime.now(self.DEFAULT_TZ)
        while not self.check_ready(symbol, interval, run_time):
            time.sleep(0.01)
            if now_time > expire_time:
                return False
        log_print(
            f'{symbol} {interval} kline is ready',
            level='debug'
        )
        return True

    def round_down_to_nearest_interval(self, timestamp, freq):
        return timestamp.floor(freq)

    def resample_klines(self, df, period):
        if period not in ['5m', '15m', '30m', '1h', '4h']:
            raise ValueError("Invalid period. Allowed periods: \
                             '5m', '15m', '30m', '1h', '4h'")

        resample_rule = {
            '5m': '5min',
            '15m': '15min',
            '30m': '30min',
            '1h': '1h',
            '4h': '4h'
        }

        ohlc_dict = {
            # 'candle_begin_time': 'last',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'quote_volume': 'sum',
            'trade_num': 'sum',
            'taker_buy_base_asset_volume': 'sum',
            'taker_buy_quote_asset_volume': 'sum'
        }
        last_candle_begin_time = df.index.max().floor(resample_rule[period])
        df = df.loc[:last_candle_begin_time]
        resampled_df = df.resample(resample_rule[period]
                                   ).apply(ohlc_dict).dropna(how='any')
        resampled_df = resampled_df.reset_index(
        ).rename(columns={'index': 'candle_end_time'})
        resampled_df['candle_begin_time'] = resampled_df[
            'candle_end_time'] - pd.Timedelta(resample_rule[period])

        resampled_df['candle_begin_time'] = pd.to_datetime(
            resampled_df['candle_begin_time'],
            utc=True).dt.tz_convert(self.DEFAULT_TZ)
        resampled_df['candle_end_time'] = pd.to_datetime(
            resampled_df['candle_end_time'],
            utc=True).dt.tz_convert(self.DEFAULT_TZ)

        return resampled_df
