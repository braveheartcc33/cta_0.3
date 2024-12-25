from config_manager import ConfigManager
from exchange.binance_pm_margin import BinancePmMarginAdaptor
from exchange.binance_pm_future import BinancePmFutureAdaptor


class ExchangeFactory:
    '''
    交易所工厂
    将每个交易所的特定交易类型作为一个类
    如 BinanceSpot, BinanceFuture, BinanceDerivatives, BinancePmMargin,
    BinancePmFuture, BinancePmDerivatives, OkexSpot, OkexFuture...

    '''
    @staticmethod
    def create_adaptor(config_manager: ConfigManager):

        exchange_name = config_manager.get("exchange", None)
        is_pm = config_manager.get("is_pm", False)
        trade_type = config_manager.get("trade_type", None)

        adaptors = {
            "binance": {
                # "normal": {
                #     "spot": BinanceSpotAdaptor,
                #     "future": BinanceFutureAdaptor,
                #     "derivatives": BinanceDerivativesAdaptor,
                # },
                "pm": {
                    "margin": BinancePmMarginAdaptor,
                    "future": BinancePmFutureAdaptor,
                    # "derivatives": BinancePmDerivativesAdaptor,
                }
            },
            # "okex": {
            #     "normal": {
            #         "spot": OkexSpotAdaptor,
            #         "future": OkexFutureAdaptor,
            #         "derivatives": OkexDerivativesAdaptor,
            #     },
            #     "pm": {
            #         "margin": OkexPmMarginAdaptor,
            #         "future": OkexPmFutureAdaptor,
            #         "derivatives": OkexPmDerivativesAdaptor,
            #     }
            # },
        }

        exchange_adaptors = adaptors.get(exchange_name.lower())
        if not exchange_adaptors:
            raise ValueError(f"Unknown exchange: {exchange_name}")

        # 根据是否是pm获取适当的子字典
        trade_adaptors = exchange_adaptors.get("pm" if is_pm else "normal")
        if not trade_adaptors:
            raise ValueError(f"Unsupported PM setting: {is_pm} "
                             f"for exchange: {exchange_name}")

        # 获取具体交易类型的适配器
        adaptor_class = trade_adaptors.get(trade_type.lower())
        if not adaptor_class:
            raise ValueError(f"Unsupported trade type: {trade_type} "
                             f"for exchange: {exchange_name}")

        return adaptor_class(config_manager)
