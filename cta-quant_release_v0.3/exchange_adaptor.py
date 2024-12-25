import os
from apscheduler.schedulers.background import BackgroundScheduler

from config import exchange_path
from config_manager import ConfigManager
from utils import log_print


class ExchangeAdaptor(object):
    '''
    交易所适配器基类
    定义接口
    '''
    def __init__(self, config_manager: ConfigManager) -> None:
        self.config_manager = config_manager
        self.scheduler = BackgroundScheduler()

        self.exchange = self._create_adaptor()

        self._init_settings()
        self._init_exchange_info()
        self._init_task()
        self.scheduler.start()

    def _init_settings(self):
        strategy_id = self.config_manager.get("strategy", None)
        if not strategy_id:
            raise ValueError("strategy_id need to be set in config")

        self._storage_path = os.path.join(exchange_path, strategy_id)
        if not os.path.exists(self._storage_path):
            os.mkdir(self._storage_path)
        self.set_position_side()

    def _init_task(self):
        self.scheduler.add_job(
            id="update_exchange_info",
            func=self._update_exchange_info,
            args=[],
            trigger="cron",
            minute="*/5",
            misfire_grace_time=60,
            max_instances=1,
        )
        self.scheduler.add_job(
            id="bnb_commission",
            func=self._bnb_transfer,
            args=[],
            trigger="cron",
            minute="*/5",
            misfire_grace_time=60,
            max_instances=1,
        )

    def get_exchange(self):
        return self.exchange

    def _init_exchange_info(self):
        self._update_exchange_info()

    def _update_exchange_info(self):
        self.exchange_info = self.get_exchange_info()
        strategy_id = self.config_manager.get("strategy", None)
        log_print(f"{strategy_id} update exchange_info success")

    def _get_rest_book_ticker(self):
        raise NotImplementedError("Must complete get_rest_book_ticker method")

    def get_book_ticker(self):
        raise NotImplementedError("Must complete get_book_ticker method")

    def _get_book_ticker(self):
        raise NotImplementedError("Must complete _get_book_ticker method")

    def _create_adaptor(self):
        raise NotImplementedError("Must complete create_adaptor method")

    def place_taker_order(self, symbol, order_amount, clientId):
        raise NotImplementedError(
            "Must complete spot_place_maker_order method")

    def get_exchange_info(self):
        raise NotImplementedError("Must complete get_exchange_info method")

    def get_account(self):
        raise NotImplementedError("Must complete get_account method")

    def get_balance(self):
        raise NotImplementedError("Must complete get_balance method")

    def get_actual_balance(self):
        raise NotImplementedError("Must complete get_actual_balance method")

    def get_position(self):
        raise NotImplementedError("Must complete get_positions method")

    def _set_leverage(self, leverage):
        raise NotImplementedError("Must complete set_leverage method")

    def get_unimmr(self):
        raise NotImplementedError("Must complete get_unimmr method")

    def set_position_side(self):
        raise NotImplementedError("Must complete set_position_side method")

    def _get_position_side(self):
        raise NotImplementedError("Must complete _get_position_side method")

    def set_multi_asset_margin(self):
        raise NotImplementedError(
            "Must complete set_multi_asset_margin method")

    def _get_multi_asset_margin(self):
        raise NotImplementedError(
            "Must complete _get_multi_asset_margin method")

    def set_bnb_burn(self):
        raise NotImplementedError("Must complete set_bnb_burn method")

    def _get_bnb_burn(self):
        raise NotImplementedError("Must complete _get_bnb_burn method")

    def _bnb_transfer(self):
        raise NotImplementedError("Must complete bnb_transfer method")

    def auto_collection(self):
        raise NotImplementedError("Must complete auto_collection method")

    def post_asset_dust(self):
        raise NotImplementedError("Must complete post_asset_dust method")
