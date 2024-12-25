from config_manager import ConfigManager
from config import sql_uri

from database.models import CtaStrategy, Base
from utils import log_print, with_session

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class DatabaseManager(object):
    '''
    数据库交互类，与数据库的交互操作
    '''

    def __init__(self, config_manager: ConfigManager) -> None:
        self.config_manager = config_manager
        self._init_settings()

    def _init_task(self):
        pass

    def _init_settings(self):
        self._init_task()
        # 需要定时刷新
        self.engine = create_engine(
            sql_uri,
            pool_size=20,  # 根据需求调整连接池大小
            max_overflow=10
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        # self.session = self.Session()
        # self.session.connection(
        #     execution_options={"isolation_level": "SERIALIZABLE"}
        # )

    @with_session
    def get_all_running_strategy(self, session):
        '''
        获取所有运行中的策略
        '''
        try:
            # param_list = []
            param_dict = {}
            items = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.is_running == 1,
                CtaStrategy.strategy == self.config_manager.get(
                    "strategy", None),
                CtaStrategy.is_pm == self.config_manager.get("is_pm", False),
                CtaStrategy.trade_type == self.config_manager.get(
                    "trade_type", "future"),
                CtaStrategy.is_del == 0
            ).all()
            for item in items:
                # param_list.append([
                #     item.id, item.strategy, item.trade_type,
                #     item.is_pm, item.symbol, item.interval,
                #     item.cta, item.period, item.position_amount, item.is_tpsl
                # ])
                # return param_list
                param_dict[item.id] = {
                    'id': item.id,
                    'strategy': item.strategy,
                    'trade_type': item.trade_type,
                    'is_pm': item.is_pm,
                    'symbol': item.symbol,
                    'interval': item.interval,
                    'cta': item.cta,
                    'period': item.period,
                    'position_amount': item.position_amount,
                    'is_tpsl': item.is_tpsl
                }
            return param_dict
        except Exception as e:
            log_print(e)
            return {}

    @with_session
    def get_all_strategy_id(self, session):
        '''
        获取所有策略的cta_key
        '''
        try:
            ids = []
            items = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.strategy == self.config_manager.get(
                    "strategy", None),
                CtaStrategy.is_pm == self.config_manager.get("is_pm", False),
                CtaStrategy.trade_type == self.config_manager.get(
                    "trade_type", "future"),
                CtaStrategy.is_del == 0
            ).all()
            for item in items:
                ids.append(item.id)
            return ids
        except Exception as e:
            log_print(e)
            return []

    @with_session
    def get_all_need_tpsl_strategy(self, session):
        '''
        获取所有需要止盈止损策略的cta_key
        '''
        try:
            # param_list = []
            param_dict = {}
            items = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.is_running == 1,
                CtaStrategy.strategy == self.config_manager.get(
                    "strategy", None),
                CtaStrategy.is_pm == self.config_manager.get("is_pm", False),
                CtaStrategy.trade_type == self.config_manager.get(
                    "trade_type", "future"),
                CtaStrategy.is_del == 0,
                CtaStrategy.open_tpsl == 1
            ).all()
            for item in items:
                # param_list.append([
                #     item.id, item.strategy, item.trade_type,
                #     item.is_pm, item.symbol, item.interval,
                #     item.cta, item.period, item.position_amount, item.is_tpsl
                # ])
                param_dict[item.id] = {
                    'id': item.id,
                    'strategy': item.strategy,
                    'trade_type': item.trade_type,
                    'is_pm': item.is_pm,
                    'symbol': item.symbol,
                    'interval': item.interval,
                    'cta': item.cta,
                    'period': item.period,
                    'position_amount': item.position_amount,
                    'is_tpsl': item.is_tpsl
                }
            return param_dict
        except Exception as e:
            log_print(e)
            return {}

    @with_session
    def get_cta_trade_info(self, session, id):
        '''
        获取指定cta_key的策略信息
        '''
        try:
            item = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == id
            ).first()
            return {
                'strategy': item.strategy,
                'trade_type': item.trade_type,
                'is_pm': item.is_pm,
                'symbol': item.symbol,
                'signal': item.signal,
                'init_value': item.init_value,
                'net_value': item.net_value,
                'open_price': item.open_price,
                'close_price': item.close_price,
                'trade_ratio': item.trade_ratio,
                'position_amount': item.position_amount,
                'takeprofit_percentage': item.takeprofit_percentage,
                'takeprofit_drawdown_percentage':
                item.takeprofit_drawdown_percentage,
                'stoploss_percentage': item.stoploss_percentage,
                'open_tpsl': item.open_tpsl,
                'interval': item.interval,
            }
        except Exception as e:
            log_print(e)
            return None

    @with_session
    def create_cta_strategy(self, session, data):
        '''
        创建cta策略
        '''
        try:
            strategy = CtaStrategy(
                strategy=data['strategy'],
                trade_type=data['trade_type'],
                is_pm=data['is_pm'],
                symbol=data['symbol'],
                interval=data['interval'],
                cta=data['cta'],
                period=data['period'],
                position_amount=data['position_amount'],
                is_tpsl=data['is_tpsl'],
                is_running=data['is_running'],
                signal=0,
                signal_time=None,
                open_price=0,
                close_price=0,
                init_value=data['init_value'],
                profit=0,
                net_value=data['net_value'],
                trade_ratio=data['trade_ratio'],
                takeprofit_percentage=data['takeprofit_percentage'],
                takeprofit_drawdown_percentage=data[
                    'takeprofit_drawdown_percentage'],
                stoploss_percentage=data['stoploss_percentage'],
                open_tpsl=0,
            )
            session.add(strategy)
            session.commit()
            return {'status': 0, 'msg': 'create strategy success'}
        except Exception as e:
            log_print(e, level='error')
            return {'status': 500, 'msg': str(e)}

    @with_session
    def delete_cta_strategy(self, session, id):
        '''
        删除cta策略
        '''
        try:
            strategy = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == id
            ).first()
            strategy.is_del = 1
            session.commit()
            return {'status': 0, 'msg': 'delete strategy success'}
        except Exception as e:
            log_print(e)
            return {'status': 500, 'msg': str(e)}

    @with_session
    def update_cta_strategy(self, session, data):
        '''
        更新cta策略
        '''
        try:
            strategy = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == data['id']
            ).first()
            strategy.strategy = data['strategy']
            strategy.trade_type = data['trade_type']
            strategy.is_pm = data['is_pm']
            strategy.trade_ratio = data['trade_ratio']
            strategy.takeprofit_percentage = data['takeprofit_percentage']
            strategy.stoploss_percentage = data['stoploss_percentage']
            session.commit()
        except Exception as e:
            log_print(e)
            return {'status': 500, 'msg': str(e)}

    @with_session
    def start_cta_strategy(self, session, id):
        '''
        开启cta策略
        '''
        try:
            strategy = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == id
            ).first()
            strategy.is_running = 1
            session.commit()
            return {'status': 0, 'msg': 'start strategy success'}
        except Exception as e:
            log_print(e)
            return {'status': 500, 'msg': str(e)}

    @with_session
    def stop_cta_strategy(self, session, id):
        '''
        停止cta策略
        '''
        try:
            strategy = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == id
            ).first()
            strategy.is_running = 0
            session.commit()
            return {'status': 0, 'msg': 'stop strategy success'}
        except Exception as e:
            log_print(e)
            return {'status': 500, 'msg': str(e)}

    @with_session
    def start_cta_strategy_tpsl(self, session, id):
        '''
        开启cta策略止盈止损
        '''
        try:
            strategy = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == id
            ).first()
            strategy.open_tpsl = 1
            session.commit()
            return {'status': 0, 'msg': 'start tpsl success'}
        except Exception as e:
            log_print(e)
            return {'status': 500, 'msg': str(e)}

    @with_session
    def stop_cta_strategy_tpsl(self, session, id):
        '''
        停止cta策略止盈止损
        '''
        try:
            strategy = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == id
            ).first()
            strategy.open_tpsl = 0
            session.commit()
            return {'status': 0, 'msg': 'stop tpsl success'}
        except Exception as e:
            log_print(e)
            return {'status': 500, 'msg': str(e)}

    @with_session
    def update_tradeinfo(self, session, id, data):
        '''
        更新cta策略
        '''
        try:
            strategy = session.query(
                CtaStrategy
            ).filter(
                CtaStrategy.id == id
            ).first()
            for key, value in data.items():
                setattr(strategy, key, value)
            session.commit()
            log_print(f'strategy {id} 交易信息写入成功', level='info')
        except Exception as e:
            log_print(f'strategy {id} 交易信息写入失败', level='error')
            log_print(e)
            # return {'status': 500, 'msg': str(e)}
