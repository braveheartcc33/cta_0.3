import ccxt
import ccxt.pro
from config import (
    proxy)
from exchange_adaptor import ExchangeAdaptor


class BinanceFutureAdaptor(ExchangeAdaptor):

    def __init__(self, config_manager) -> None:
        super().__init__(config_manager)

    def _init_task(self):
        super()._init_task()
        self.scheduler.add_job(
            id="bnb_transfer",
            func=self._bnb_transfer,
            args=[],
            trigger="cron",
            minute="*/30",
            misfire_grace_time=60,
            max_instances=1,
        )

    def _create_adaptor(self):
        exchange = ccxt.binance(
            {
                "apiKey": self.config_manager.get("api"),
                "secret": self.config_manager.get("secret"),
                "timeout": 30000,
                "rateLimit": 10,
                "enableRateLimit": False,
                "options": {
                    "adjustForTimeDifference": True,
                    "recvWindow": 10000,
                },
            }
        )

        if self.config_manager.get("need_proxy", False):
            exchange.proxies = proxy

        return exchange
