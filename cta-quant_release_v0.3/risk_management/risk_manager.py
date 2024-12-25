from config_manager import ConfigManager
from exchange_factory import ExchangeFactory


class RiskManager(object):
    '''
    账户基本风险控制
    '''

    def __init__(self, config_manager: ConfigManager,
                 exchange_adaptor: ExchangeFactory) -> None:
        self.config_manager = config_manager
        self.exchange_adaptor = exchange_adaptor

        self._init_settings()

    def _init_task(self):
        pass

    def _init_settings(self):
        self._init_task()
        pass

    # TODO
    # unimmr 监控
    def monitor_unimmr(self):
        # self.config_manager.get("exchange", "binance")
        unimmr = self.exchange_adaptor.get_unimmr()
        if unimmr < 2:
            pass
