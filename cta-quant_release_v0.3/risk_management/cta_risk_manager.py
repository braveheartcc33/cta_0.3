from config_manager import ConfigManager
from database.database_manager import DatabaseManager
from apscheduler.schedulers.background import BackgroundScheduler
from trade_manager import TradeManager
from exchange_factory import ExchangeFactory

from utils import log_print, send_message
import pandas as pd
from decimal import Decimal
from datetime import datetime
import os


# TODO
# 重复策略检查, 是否插入了重复策略, 可通过 cta key
class CtaRiskManager(object):
    '''
        负责各个账户CTA的风险控制
    '''

    def __init__(self, config_manager: ConfigManager,
                 database_manager: DatabaseManager,
                 exchange_adaptor: ExchangeFactory,
                 trade_manager: TradeManager) -> None:
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.scheduler = BackgroundScheduler()
        self.exchange_adaptor = exchange_adaptor
        self.trade_manager = trade_manager
        self.cta_tpsl_time = self.config_manager.get('self.cta_tpsl_time',
                                                     '10s')
        # self.config_manager.get(cta_stoploss_inside_bar, True) # 不指定默认bar内止损
        self.strategy_name = self.config_manager.get("strategy", None)
        self.tpsl_path = exchange_adaptor._storage_path  # tpsl data path
        self._init_settings()
        self.scheduler.start()

    def _init_task(self):
        self.cta_strategy_list = (
            self.database_manager.get_all_need_tpsl_strategy()
        )
        # params = self.database_manager.get_all_need_tpsl_strategy()
        intervals = []
        for param in self.cta_strategy_list.values():
            interval = param.get('interval')
            if interval not in intervals:
                intervals.append(interval)

        if self.cta_tpsl_time.find('m') > 0:
            self.scheduler.add_job(
                id='cta_usdt_tpsl',
                func=self.cta_strategy_tpsl,
                trigger='cron',
                minute=f"*/{self.cta_tpsl_time.split('m')[0]}",
                misfire_grace_time=300,
                max_instances=1)
        elif self.cta_tpsl_time.find('s') > 0:
            self.scheduler.add_job(
                id='cta_usdt_tpsl',
                func=self.cta_strategy_tpsl,
                trigger='cron',
                second=f"*/{self.cta_tpsl_time.split('s')[0]}",
                misfire_grace_time=300)

        # 信号 & 仓位检查
        if self.config_manager.get("pos_infer", True):
            self.scheduler.add_job(id='cta_usdt_signal_check',
                                   func=self.cta_signal_check,
                                   trigger='cron',
                                   minute="3",
                                   misfire_grace_time=300)

        self.scheduler.add_job(
            id='update_cta_tpsl_list',
            func=self._update_cta_tpsl_list,
            trigger='cron',
            second="10",
            minute="*/10",
            misfire_grace_time=300)

    def _update_cta_tpsl_list(self):
        self.cta_strategy_list = (
            self.database_manager.get_all_need_tpsl_strategy()
        )

    def _init_settings(self):
        self._init_task()

    def cta_strategy_tpsl(self):
        log_print('U本位CTA策略止盈止损监测开始', level='debug')
        # 获取当前策略的移动止盈信息
        tpsl_csv_path = os.path.join(
            self.tpsl_path, f'{self.strategy_name}_cta_usdt_tpsl.csv')
        try:
            df = pd.read_csv(tpsl_csv_path)
        except Exception:
            df = pd.DataFrame(columns=['symbol', 'cta_id', 'max_profit_ratio'])

        profit_list = []
        cta_ids = self.cta_strategy_list.keys()
        for cta_id in cta_ids:
            cta_strategy = self.cta_strategy_list[cta_id]
            cta_key = (
                f'{cta_strategy["symbol"]}_{cta_strategy["interval"]}'
                f'_{cta_strategy["cta"]}_{cta_strategy["period"]}'
                f'_{cta_strategy["strategy"]}'
            )

            log_print(f'正在进行策略{cta_key}的止盈止损监测', level='debug')

            trade_info = self.database_manager.get_cta_trade_info(cta_id)
            if trade_info is None:
                log_print(f'{cta_key}止盈止损执行出错，请排查', level='error')
                send_message(f'{cta_key}止盈止损执行出错，请排查')
                continue
            open_tpsl = trade_info['open_tpsl']
            if open_tpsl == 0:
                log_print(f'{cta_key}未开启止盈止损,无需止盈止损', level='debug')
                continue
            signal = trade_info['signal']
            takeprofit_percentage = Decimal(
                trade_info['takeprofit_percentage'])
            cta_takeprofit_drawdown_percentage = Decimal(
                trade_info['takeprofit_drawdown_percentage'])
            stoploss_percentage = Decimal(trade_info['stoploss_percentage'])
            log_print(
                f'{cta_key}止盈比例为{takeprofit_percentage:.2f}'
                f',止损比例为{stoploss_percentage:.2f}',
                level='debug'
            )
            if signal == 0:
                log_print(f'{cta_key}未开仓,无需止盈止损', level='debug')
                continue

            symbol = trade_info['symbol']
            pos_amount = Decimal(trade_info['position_amount'])
            open_price = Decimal(trade_info['open_price'])
            last_price = (float(
                self.exchange_adaptor.get_book_ticker()
                [symbol]["askPrice"]) +
                          float(self.exchange_adaptor.
                                get_book_ticker()[symbol]["bidPrice"])) / 2

            last_price = Decimal(last_price)
            profit_ratio = Decimal(
                f'{signal * (last_price / open_price - 1):.4f}')

            profit_list.append([symbol, cta_id, profit_ratio])

            cta_stoploss_inside_bar = self.config_manager.get(
                'cta_stoploss_inside_bar', True)
            # bar内止损检查
            condition_inside_bar = cta_stoploss_inside_bar or (
                datetime.now().timestamp() %
                int(pd.to_timedelta(trade_info['interval']).total_seconds())
                < max(pd.to_timedelta(self.cta_tpsl_time).seconds, 30))

            # 止损条件
            condition_sl = condition_inside_bar and profit_ratio < 0 and abs(
                profit_ratio
            ) >= stoploss_percentage  # 如果当前盈利为负数，且大于触发百分比，币种进黑名单

            try:
                max_profit_ratio = Decimal(
                    df[(df['symbol'] == symbol)
                       & (df['cta_id'] == cta_id)]['max_profit_ratio'].iloc[0])
            except Exception:
                max_profit_ratio = None

            # 止盈条件
            if max_profit_ratio is None:
                condition_tp = False
            else:
                condition_tp = profit_ratio > 0 \
                    and (max_profit_ratio > takeprofit_percentage) \
                    and (max_profit_ratio - profit_ratio >=
                         cta_takeprofit_drawdown_percentage)

            if float(pos_amount) > 0:
                direction = "做多"
                cta_pos_symbol = f"{cta_key} {direction}{symbol}"
                round_profit_ratio = f"{round(profit_ratio, 4) * 100}"

                if condition_sl:
                    t1 = self._cta_usdt_tpsl_close_order(
                        cta_id, symbol, trade_info, cta_key, last_price)
                    if t1:
                        log_print(
                            f'{cta_pos_symbol}已止损，亏损{round_profit_ratio}%',
                            level='info')
                        send_message(
                            f'{cta_pos_symbol}已止损，亏损{round_profit_ratio}%')
                    else:
                        log_print(f'{cta_pos_symbol}止损失败，请排查', level='error')
                        log_print(f'{cta_pos_symbol}止损失败，请排查', level='error')
                        send_message(f'{cta_pos_symbol}止损失败，请排查')
                if condition_tp:
                    t1 = self._cta_usdt_tpsl_close_order(
                        cta_id, symbol, trade_info, cta_key, last_price)
                    if t1:
                        log_print(
                            f'{cta_pos_symbol}已止盈，盈利{round_profit_ratio}%',
                            level='info')
                        send_message(
                            f'{cta_pos_symbol}已止盈，盈利{round_profit_ratio}%')
                    else:
                        log_print(f'{cta_pos_symbol}止盈失败，请排查', level='error')
                        log_print(f'{cta_pos_symbol}止盈失败，请排查', level='error')
                        send_message(f'{cta_pos_symbol}止盈失败，请排查')

            if float(pos_amount) < 0:
                direction = "做空"
                cta_pos_symbol = f"{cta_key} {direction}{symbol}"
                round_profit_ratio = f"{round(profit_ratio, 4) * 100}"

                if condition_sl:
                    t1 = self._cta_usdt_tpsl_close_order(
                        cta_id, symbol, trade_info, cta_key, last_price)
                    if t1:
                        log_print(
                            f'{cta_pos_symbol}已止损，亏损{round_profit_ratio}%',
                            level='info')
                        send_message(
                            f'{cta_pos_symbol}已止损，亏损{round_profit_ratio}%')
                    else:
                        log_print(
                            f'{cta_pos_symbol}止损失败，请排查',
                            level='error'
                        )
                        send_message(f'{cta_pos_symbol}止损失败，请排查')
                if condition_tp:
                    t1 = self._cta_usdt_tpsl_close_order(
                        cta_id, symbol, trade_info, cta_key, last_price)
                    if t1:
                        log_print(
                            f'{cta_pos_symbol}已止盈，盈利{round_profit_ratio}%',
                            level='info')
                        send_message(
                            f'{cta_pos_symbol}已止盈，盈利{round_profit_ratio}%')
                    else:
                        log_print(
                            f'{cta_pos_symbol}止盈失败，请排查',
                            level='error'
                        )
                        send_message(f'{cta_pos_symbol}止盈失败，请排查')

            log_print(f'策略{cta_key}止盈止损监测完成', level='debug')

        new_df = pd.DataFrame(
            profit_list,
            columns=[
                'symbol',
                'cta_id',
                'max_profit_ratio'
                ]
            )
        new_df = new_df.dropna(axis=1, how='all')
        df = pd.concat([df, new_df], ignore_index=True)

        df = df[df['max_profit_ratio'] > 0]
        df.sort_values('max_profit_ratio', ascending=False, inplace=True)
        df.drop_duplicates(subset=['symbol', 'cta_id'],
                           keep='first',
                           inplace=True)
        df.to_csv(tpsl_csv_path, index=False)
        log_print('U本位CTA策略止盈止损监测完成', level='debug')

    def cta_signal_check(self):
        # 信号修正
        self.trade_manager.cta_execute(pos_infer=True)
        # 仓位修正
        self.cta_position_check()

    def cta_position_check(self):
        # 仓位校准部分
        # 获取开启CTA的strategy, 对每个strategy逐个处理
        params_dict = self.database_manager.get_all_running_strategy()
        # 整理策略持仓
        strategy_info = pd.DataFrame.from_dict(params_dict, orient='index')
        strategy_info = strategy_info[[
            'strategy', 'symbol', 'position_amount'
        ]]

        strategy_info.fillna({'position_amount': 0}, inplace=True)

        symbol_list = list(set(strategy_info['symbol'].to_list()))
        strategy_info_by_symbol = pd.DataFrame(index=symbol_list,
                                               columns=['position_amount'])
        strategy_info_by_symbol['position_amount'] = strategy_info.groupby(
            'symbol')['position_amount'].sum()

        # 整理当前持仓
        position_risk = self.exchange_adaptor.get_um_position_risk()
        # 将原始数据转化为dataframe
        position_risk = pd.DataFrame(position_risk)
        if position_risk.empty or position_risk is None:
            position_risk = pd.DataFrame(columns=['symbol', 'positionAmt'])
        # 整理数据
        position_risk.rename(columns={'positionAmt': '当前持仓量'}, inplace=True)
        position_risk = position_risk[position_risk['当前持仓量'] != 0]  # 只保留有仓位的币种
        position_risk.set_index('symbol', inplace=True)  # 将symbol设置为index
        # 创建symbol_info
        symbol_info = pd.DataFrame(index=symbol_list, columns=['当前持仓量'])
        symbol_info['当前持仓量'] = position_risk['当前持仓量']
        symbol_info.loc[:, '当前持仓量'] = symbol_info['当前持仓量'].fillna(0)

        # 整理待校准结果
        symbol_info['position_amount'] = strategy_info_by_symbol[
            'position_amount']
        symbol_info['下单量'] = symbol_info['position_amount'].astype(
            float) - symbol_info['当前持仓量'].astype(float)
        symbol_info = symbol_info[symbol_info['下单量'] != 0]

        for symbol, row in symbol_info.dropna(subset=['下单量']).iterrows():
            order_amount = row['下单量']

            if self.exchange_adaptor.place_taker_order(
                    symbol, float(order_amount),
                    'pos_check'):
                log_print(f'{symbol}仓位校准下单成功')
                send_message(f'{symbol}仓位校准下单成功')
            else:
                log_print(f'{symbol}仓位校准下单失败', level='error')
                log_print(f'{symbol}仓位校准下单失败', level='error')
                send_message(f'{symbol}仓位校准下单失败')

    def _cta_usdt_tpsl_close_order(self, cta_id, symbol, trade_info, cta_key,
                                   last_price):
        symbol = trade_info['symbol']
        open_price = trade_info['open_price']  # 策略上次开仓价
        init_value = trade_info['init_value']
        net_value = trade_info['net_value']  # 策略当前净值
        trade_ratio = trade_info['trade_ratio']  # 策略杠杆
        position_amount = trade_info['position_amount']  # 策略当前持仓

        # 计算最新的net_value，当前价格/开仓价格-1是涨跌幅，根据上一个signal类型及杠杆确定实际盈亏百分比，
        # 加1之后乘以之前记录的net_value，得到最新的net_value
        if open_price is not None and open_price != Decimal(0):
            net_value = ((Decimal(last_price) / open_price - 1) *
                         trade_info['signal'] * trade_ratio + 1) * net_value
        target_amount = 0  # 目标下单量
        order_amount = target_amount - position_amount  # 所需下单量 = 目标下单量 - 当前持仓量
        log_print(f'标的{symbol}所需下单量={order_amount}', level='info')
        # 下单并更新数据库
        if self.exchange_adaptor.place_taker_order(symbol, float(order_amount),
                                                   'cta_tpsl'):
            log_print(f'{cta_key}下单成功')
            send_message(f'{cta_key}下单成功')
            data = {
                'signal': 0,
                'signal_time': datetime.now(),
                'close_price': last_price,
                'profit': net_value - init_value,
                'net_value': net_value,
                'position_amount': target_amount,
                'is_running': 1,
                'is_tpsl': 1,
            }
            log_print(f'交易信息{data}', level='debug')
            self.database_manager.update_tradeinfo(cta_id, data)
            send_message(f'{cta_key}策略止盈止损成功')
            return True
        else:
            log_print(f'{cta_key}策略止盈止损下单函数执行失败', level='error')
            log_print(f'{cta_key}策略止盈止损下单函数执行失败', level='error')
            send_message(f'{cta_key}策略止盈止损下单函数执行失败')
            return False

    def cta_adl(self):
        # U本位暂不需要实现
        pass
