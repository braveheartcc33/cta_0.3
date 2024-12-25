from config_manager import ConfigManager
from exchange_factory import ExchangeFactory
from database.database_manager import DatabaseManager
from data_center.data_center_manager import DataCenterManager
from config import CTA_EXECUTION_FLAG

from utils import log_print, send_message
# from gevent import monkey
from datetime import datetime
# import pandas as pd
import numpy as np
import importlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


class TradeManager(object):
    def __init__(self, config_manager: ConfigManager,
                 database_manager: DatabaseManager,
                 exchange_adaptor: ExchangeFactory,
                 data_center_manager: DataCenterManager) -> None:
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.exchange_adaptor = exchange_adaptor
        self.data_center_manager = data_center_manager

    def get_current_time(self):
        '''
        获取当前时间
        '''
        current_time = datetime.now().replace(second=0, microsecond=0)
        return current_time

    def get_valid_intervals(self, intervals, current_time):
        '''
        获取有效的策略
        分钟向下取整到5的倍数
        '''
        valid_intervals = []
        minute = (current_time.minute // 5) * 5
        current_time = current_time.replace(minute=minute,
                                            second=0,
                                            microsecond=0)
        for interval in intervals:
            if "m" in interval:
                # 检查分钟间隔
                if current_time.minute % int(interval.split('m')[0]) == 0:
                    valid_intervals.append(interval)
            elif "h" in interval:
                # 检查小时间隔
                if (current_time.minute == 0 and
                        current_time.hour % int(interval.split('h')[0]) == 0):
                    valid_intervals.append(interval)
        return valid_intervals

    def cta_execute(self, pos_infer=False):
        params = self.database_manager.get_all_running_strategy()
        intervals = list(set([param['interval'] for param in params.values()]))
        order_list, update_list = self.generate_order_list(intervals,
                                                           params,
                                                           pos_infer)
        flag = self.config_manager.get(CTA_EXECUTION_FLAG, 0)
        if flag:
            log_print("cta_controller cta executing wait...", level='info')
            while flag:
                time.sleep(5)
                flag = self.config_manager.get(CTA_EXECUTION_FLAG, 0)
        self.config_manager.set(CTA_EXECUTION_FLAG, 1)
        self.update_cta_strategy(update_list)
        self.execute(order_list)
        self.config_manager.set(CTA_EXECUTION_FLAG, 0)

    def update_cta_strategy(self, update_list):
        '''
        更新策略
        '''
        for id, data in update_list.items():
            self.database_manager.update_tradeinfo(id, data)
        log_print("cta_controller update success", level='info')

    def execute(self, order_list):
        '''
        下单执行
        '''
        for symbol, order_amount in order_list.items():
            self.exchange_adaptor.place_taker_order(symbol,
                                                    order_amount,
                                                    "cta")
            send_message(f'cta 策略{symbol} {order_amount}下单成功')
        log_print("cta_controller execute success", level='info')

    def generate_order_list(self, intervals, params, pos_infer=False):
        '''
        生成下单列表
        '''
        run_time = self.get_current_time()
        # 信号检查(仓位推断), 调整时间
        valid_intervals = self.get_valid_intervals(intervals, run_time)
        if pos_infer:
            minute = (run_time.minute // 5) * 5
            run_time = run_time.replace(minute=minute)

        # 信号修正时, 暂不对5m周期的信息进行处理, 可能存在处理时长不足问题
        if pos_infer and '5m' in valid_intervals:
            valid_intervals.remove('5m')
        valid_params = []
        for param in params.values():
            # 信号修正时, 跳过已止盈止损策略
            if pos_infer and param['is_tpsl'] == 1:
                continue
            # interval = param['interval']
            if param['interval'] in valid_intervals:
                valid_params.append(param)

        # 按 symbol 和 interval 进行分组
        grouped_params = {}
        exchange = self.config_manager.get("exchange", "binance")
        klines_data = {}
        trade_info_data = {}
        for param in valid_params:
            if param['interval'] in valid_intervals:
                symbol = param.get('symbol')
                interval = param.get('interval')
                key = (symbol, interval)
                if key not in grouped_params:
                    grouped_params[key] = []
                    grouped_params[key].append(param)
                    symbol_klines = self.data_center_manager.get_klines(
                        exchange, symbol, interval, run_time)

                    symbol_klines = symbol_klines.sort_values(
                        by=['candle_begin_time'])
                    symbol_klines = symbol_klines.drop_duplicates(
                        subset=['candle_begin_time'], keep='last')
                    klines_data[key] = symbol_klines
                    trade_info = self.database_manager.get_cta_trade_info(
                        param['id'])
                    trade_info_data[param['id']] = trade_info
                else:
                    grouped_params[key].append(param)
                    trade_info = self.database_manager.get_cta_trade_info(
                        param['id'])
                    trade_info_data[param['id']] = trade_info

        order_list = {}
        update_list = {}
        # 并行获取 K 线数据并生成信号
        with ThreadPoolExecutor() as executor:
            future_to_key = {
                executor.submit(self._process_symbol_interval_orders,
                                klines_data[key],
                                key,
                                group,
                                trade_info_data,
                                pos_infer): key
                for key, group in grouped_params.items()
            }

            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    orders, updates = future.result()
                    # 合并订单列表和更新列表
                    for symbol, amount in orders.items():
                        if symbol in order_list:
                            order_list[symbol] += amount
                        else:
                            order_list[symbol] = amount
                    update_list.update(updates)
                except Exception as exc:
                    log_print(f'{key} generated an exception: {exc}',
                              level='error')
                    send_message(f'{key} generated order list: {exc}')
        return order_list, update_list

    def _process_symbol_interval_orders(self,
                                        symbol_klines,
                                        key,
                                        params_group,
                                        trade_info_data,
                                        pos_infer):
        '''
        对单个 symbol + interval 组合进行订单生成处理
        '''
        symbol, interval = key
        orders = {}
        updates = {}

        for param in params_group:
            id, symbol, order_amount, data = self._generate_order_list(
                param, symbol_klines, trade_info_data, pos_infer)
            if order_amount == 0 or order_amount is None:
                continue
            else:
                if symbol in orders:
                    orders[symbol] += order_amount
                else:
                    orders[symbol] = order_amount
                updates[id] = data

        return orders, updates

    def _generate_order_list(self,
                             param,
                             symbol_klines,
                             trade_info_data,
                             pos_infer):
        '''
        生成下单列表
        '''
        id = param.get('id')  # param[0]
        # strategy = param[1]
        # trade_type = param[2]
        symbol = param.get('symbol')  # param[4]
        interval = param.get('interval')  # param[5]
        cta = param.get('cta')  # param[6]
        period = param.get('period')  # param[7]
        # position_amount = param[8]
        # is_tpsl = param[9]
        trade_info = trade_info_data.get(id)
        factors = importlib.import_module(f'factors.{cta}')
        function = getattr(factors, cta)
        df, *_ = function(symbol_klines.copy(), int(period))

        order_amount, data = self.generate_cta_signal(df, trade_info, interval,
                                                      period, cta, pos_infer)
        return id, symbol, order_amount, data

    def generate_cta_signal(self, df, trade_info, interval, period, cta,
                            pos_infer):
        '''
        生成cta信号
        '''
        symbol = trade_info['symbol']
        if pos_infer:
            df['signal'] = df['signal'].ffill()
            log_print(f"pos_infer {symbol} {cta} {interval} {period}"
                      f"signal {df.iloc[-1]['signal']}", level='info')

        signal = df.iloc[-1]['signal']
        log_print(f"{symbol} {cta} {interval} {period} 本次信号 {signal}",
                  level='info')
        if trade_info['trade_type'] in ['margin', 'spot'] and signal == -1:
            signal = 0
        if signal is None or np.isnan(signal):
            return None, {}
        elif signal == 1 or signal == -1:
            if trade_info is None:
                log_print(f'{symbol} {cta} {interval} {period}'
                          '获取trade_info执行失败,请修复问题',
                          level='error')
                send_message(f'{cta} 获取trade_info执行失败,请修复问题')
                return None, {}
            elif trade_info['signal'] == signal:
                log_print(
                    f'{symbol} {cta} {interval} {period} 信号未变化,不执行下单',
                    level='info'
                )
                return 0, {}
            elif trade_info['signal'] == 0:
                # 开仓
                net_value = trade_info['net_value']  # 策略当前净值
                init_value = trade_info['init_value']
                trade_ratio = trade_info['trade_ratio']  # 策略杠杆
                position_amount = trade_info['position_amount']  # 策略当前持仓
                min_qty = self.exchange_adaptor.exchange_info[
                    symbol].get('min_qty', 0)
                ticker_price = self.exchange_adaptor.get_book_ticker(

                )[symbol]['askPrice']  # 最新价格
                target_amount = float(net_value) * float(trade_ratio) * float(
                    signal) / float(ticker_price)  # 目标下单量
                target_amount = float(f'{target_amount:.{min_qty}f}')
                # 所需下单量 = 目标下单量 - 当前持仓量
                order_amount = target_amount - float(position_amount)
                order_amount = float(f'{order_amount:.{min_qty}f}')
                log_print(f'{symbol} {cta} {interval} {period}'
                          f'所需下单量={order_amount}', level='info')
                data = {
                    'signal': signal,
                    'signal_time': datetime.now(),
                    'open_price': ticker_price,
                    'profit': net_value - init_value,
                    'net_value': net_value,
                    'position_amount': target_amount,
                    'is_tpsl': 0
                }
                return order_amount, data
            elif trade_info['signal'] != 0:
                # 正负转换
                open_price = float(trade_info['open_price'])  # 策略上次开仓价
                init_value = float(trade_info['init_value'])
                net_value = float(trade_info['net_value'])  # 策略当前净值
                trade_ratio = float(trade_info['trade_ratio'])  # 策略杠杆
                position_amount = float(trade_info['position_amount'])
                min_qty = self.exchange_adaptor.exchange_info[
                    symbol].get('min_qty', 0)
                ticker_price = self.exchange_adaptor.get_book_ticker(
                )[symbol]['askPrice']  # 最新价格
                # 计算最新的net_value，当前价格/开仓价格-1是涨跌幅，
                # 根据上一个signal类型及杠杆确定实际盈亏百分比，
                # 加1之后乘以之前记录的net_value，得到最新的net_value
                net_value = (
                    (float(ticker_price) / open_price - 1) *
                    trade_info['signal'] * trade_ratio + 1
                ) * net_value
                target_amount = net_value * trade_ratio * float(
                    signal) / float(ticker_price)  # 目标下单量
                # 所需下单量 = 目标下单量 - 当前持仓量
                order_amount = target_amount - position_amount
                target_amount = float(f'{target_amount:.{min_qty}f}')
                order_amount = float(f'{order_amount:.{min_qty}f}')
                log_print(f'{symbol} {cta} {interval} {period}'
                          f'所需下单量={order_amount}', level='info')
                data = {
                    'signal': signal,
                    'signal_time': datetime.now(),
                    'open_price': ticker_price,
                    'close_price': ticker_price,
                    'profit': net_value - init_value,
                    'net_value': net_value,
                    'position_amount': target_amount,
                    'is_tpsl': 0
                }
                return order_amount, data
        elif signal == 0:
            # 获取应该平仓的金额及杠杆率及策略当前持仓量
            if trade_info is None:
                log_print(f'{symbol} {cta} {interval} {period}  执行失败，请修复问题')
                send_message(f'{symbol} {cta} {period}  执行失败，请修复问题')
                return None, {}
            elif trade_info['signal'] == 0:
                log_print(f'{symbol} {cta} {interval} {period}'
                          '上次信号为平仓，本次也为平仓，无需操作')
                return 0, {}
            # 需要平仓
            elif trade_info['signal'] != 0:
                open_price = float(trade_info['open_price'])  # 策略上次开仓价
                init_value = float(trade_info['init_value'])
                net_value = float(trade_info['net_value'])  # 策略当前净值
                trade_ratio = float(trade_info['trade_ratio'])  # 策略杠杆
                position_amount = float(trade_info['position_amount'])
                min_qty = self.exchange_adaptor.exchange_info[
                    symbol].get('min_qty', 0)
                ticker_price = self.exchange_adaptor.get_book_ticker()[
                    symbol]['askPrice']  # 最新价格
                net_value = (
                    (float(ticker_price) / open_price - 1) *
                    trade_info['signal'] * trade_ratio + 1
                ) * net_value
                # 计算最新的net_value，当前价格/开仓价格-1是涨跌幅，
                # 根据上一个signal类型及杠杆确定实际盈亏百分比
                # 加1之后乘以之前记录的net_value，得到最新的net_value
                target_amount = 0  # 目标下单量
                order_amount = target_amount - position_amount
                # 所需下单量 = 目标下单量 - 当前持仓量
                target_amount = float(f'{target_amount:.{min_qty}f}')
                order_amount = float(f'{order_amount:.{min_qty}f}')
                log_print(f'{symbol} {cta} {interval} {period}'
                          f'所需下单量={order_amount}')
                data = {
                    'signal': signal,
                    'signal_time': datetime.now(),
                    'close_price': ticker_price,
                    'profit': net_value - init_value,
                    'net_value': net_value,
                    'position_amount': target_amount,
                    'is_tpsl': 0
                }
                return order_amount, data
