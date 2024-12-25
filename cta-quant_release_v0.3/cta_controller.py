from config_manager import ConfigManager
from exchange_factory import ExchangeFactory
from database.database_manager import DatabaseManager
from data_center.data_center_manager import DataCenterManager
from risk_management.cta_risk_manager import CtaRiskManager
from trade_manager import TradeManager

from utils import log_print
from apscheduler.schedulers.background import BackgroundScheduler
# from gevent import monkey
import time
# monkey.patch_all()


class CTAController(object):
    '''
    CTA 程序主流程控制
    '''

    def __init__(self, config_path) -> None:
        self.scheduler = BackgroundScheduler()

        self.config_manager = ConfigManager(config_path)
        self.database_manager = DatabaseManager(self.config_manager)
        self.data_center_manager = DataCenterManager(
                            self.config_manager)
        self.exchange_adaptor = ExchangeFactory.create_adaptor(
                            self.config_manager)
        self.trade_manager = TradeManager(self.config_manager,
                                          self.database_manager,
                                          self.exchange_adaptor,
                                          self.data_center_manager)

        self.risk_manager = CtaRiskManager(
            self.config_manager, self.database_manager,
            self.exchange_adaptor, self.trade_manager
        )
        self._init_settings()
        self._init_task()
        self.scheduler.start()
        self.loop()

    def _init_settings(self):
        pass

    def _init_task(self):
        self.scheduler.add_job(
            id='cta_strategy',
            func=self.trade_manager.cta_execute,
            trigger='cron',
            minute='*/5',
            misfire_grace_time=300,
            max_instances=1)
        log_print("cta_controller init task success", level='info')

    def loop(self):
        while True:
            time.sleep(60)

    def start_cta_strategy(self, id):
        self.database_manager.start_cta_strategy(id)

    def stop_cta_strategy(self, id):
        self.database_manager.stop_cta_strategy(id)

    def stop_all_cta_strategy(self):
        ids = self.database_manager.get_all_running_strategy()
        for id in ids:
            self.stop_cta_strategy(id)

    def start_cta_strategy_tpsl(self, id):
        self.database_manager.start_cta_strategy_tpsl(id)

    def stop_cta_strategy_tpsl(self, id):
        self.database_manager.stop_cta_strategy_tpsl(id)

    def delete_cta_strategy(self, id):
        self.database_manager.delete_cta_strategy(id)
