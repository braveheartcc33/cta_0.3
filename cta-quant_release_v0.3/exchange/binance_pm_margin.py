import ccxt
import ccxt.pro
import math
import time
import pandas as pd
from datetime import datetime

from config import (
    proxy)
from utils import robust, log_print
from exchange_adaptor import ExchangeAdaptor


class BinancePmMarginAdaptor(ExchangeAdaptor):

    def __init__(self, config_manager) -> None:
        super().__init__(config_manager)

    def _init_task(self):
        super()._init_task()

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

    def get_exchange_info(self):
        exchange_info = robust(
            self.exchange.publicGetExchangeInfo,
            func_name="publicGetExchangeInfo"
        )
        return exchange_info["symbols"]

    def get_position(self):
        res = robust(self.exchange.papiGetBalance, func_name="papiGetBalance")
        return res

    def _bnb_transfer(self):
        res = robust(self.exchange.papiGetBalance, func_name="papiGetBalance")
        margin = 0
        for asset in res:
            if asset["asset"] == "BNB":
                margin = float(asset["crossMarginFree"])
                break
            else:
                continue

        log_print(f"scan current bnb, margin: {margin}", level="info")

        quantity = 0
        bnb_commission_amount = self.config_manager.get("bnb_commission", 0.05)

        if margin < bnb_commission_amount:
            quantity += bnb_commission_amount - margin

        info = self.exchange_info.get("BNBUSDT")
        min_qty = info.get("min_qty")
        quantity = float(f"{quantity:.{min_qty}f}")
        if quantity == 0:
            return
        quantity = max(quantity, 0.01)
        if quantity > 0:
            params = {
                "symbol": "BNBUSDT",
                "side": "BUY",
                "type": "MARKET",
                "quantity": quantity,
            }
            res = robust(
                self.exchange.papiPostMarginOrder,
                params=params,
                func_name="papiPostMarginOrder",
            )
            log_print(f"{quantity} bnb buy success", level="info")

    def get_book_ticker(self):
        ticker_mode = self.config_manager.get("ticker_type", "restapi")
        if ticker_mode == "restapi":
            return self._get_rest_book_ticker()
        elif ticker_mode == "websocket":
            pass
        else:
            raise ValueError("ticker type need to be set in config")

    def _get_rest_book_ticker(self):
        book_ticker = self._get_book_ticker()
        df = pd.DataFrame(book_ticker)
        df["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        book_dict = df.set_index("symbol")[
            ["bidPrice", "bidQty", "askPrice", "askQty", "time"]
        ].to_dict("index")
        return book_dict

    def _set_leverage(self, leverage):
        target_leverage = int(leverage)
        position_risk = robust(
            self.exchange.papiGetUmPositionRisk,
            func_name="papiGetUmPositionRisk"
        )
        leverage_info = dict(
            [(row["symbol"], int(row["leverage"])) for row in position_risk]
        )
        for symbol, leverage in leverage_info.items():
            if leverage != target_leverage:
                params = {"symbol": symbol, "leverage": target_leverage}
                robust(
                    self.exchange.papiPostUmLeverage,
                    params,
                    func_name="papiPostUmLeverage",
                )

        strategy_id = self.config_manager.get("strategy", None)
        log_print(f"{strategy_id} set leverage = {leverage} success")

    def get_balance(self):
        balances = robust(self.exchange.papiGetAccount,
                          func_name="papiGetAccount")
        balance = float(balances["accountEquity"])
        return balance

    def get_actual_balance(self):
        balances = robust(self.exchange.papiGetAccount,
                          func_name="papiGetAccount")
        actual_balance = float(balances["actualEquity"])
        return actual_balance

    def get_unimmr(self):
        balances = robust(self.exchange.papiGetAccount,
                          func_name="papiGetAccount")
        unimmr = float(balances["uniMMR"])
        return unimmr

    def auto_collection(self):
        params = {"asset": "USDT"}
        robust(
            self.exchange.papiPostAssetCollection,
            params=params,
            func_name="papiPostAssetCollection",
        )
        log_print("execute asset_collection")

    def place_taker_order(self, symbol, order_amount, clientId):
        twap_amount = self.config_manager.get("twap_amount")
        side = "BUY" if order_amount > 0 else "SELL"
        price = (
            float(self.book_ticker[symbol]["askPrice"]) * 1.03
            if side == "BUY"
            else float(self.book_ticker[symbol]["bidPrice"]) * 0.97
        )
        price_precision = self.exchange_info[symbol].get("price_precision", 0)
        min_notional = self.exchange_info[symbol].get("min_notional", 10)
        min_qty = self.exchange_info[symbol].get("min_qty", 0)
        price = float(f"{price:.{price_precision}f}")
        reduce_only = True if order_amount < 0 else False

        twap_order_num = math.floor(order_amount * price / twap_amount)
        for i in range(0, twap_order_num):
            if order_amount * price < twap_amount + min_notional:
                log_print(f"{symbol} 不需要进一步拆单，可直接下单")
                break
            log_print(f"{symbol} twap下单,正在进行第 {i+1} 次下单")
            quantity = twap_amount / price
            quantity = float(f"{quantity:.{min_qty}f}")
            log_print(f"本次下单量 = {quantity}")

            params = {
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "price": price,
                "quantity": abs(quantity),
                "timeInForce": "GTC",
                "reduceOnly": reduce_only,
                "newClientOrderId": clientId,
            }

            try:
                res = robust(
                    self.exchange.papiPostMarginOrder,
                    params=params,
                    func_name="papiPostMarginOrder",
                )
                log_print(
                    f'spot taker open order: symbol={res["symbol"]}, '
                    f'side={side}, amount={quantity}',
                    level="info",
                )
                log_print(f"{symbol} twap下单,正在进行第 {i+1} 次下单成功")
            except Exception as e:
                log_print(
                    f"spot_place_taker_order {type(e).__name__}, {str(e)}",
                    level="error",
                )

            order_amount -= quantity
            order_amount = float(f"{order_amount:.{min_qty}f}")
            log_print(f"剩余下单量 = {order_amount}")
            time.sleep(2)

        order_amount = float(f"{order_amount:.{min_qty}f}")
        log_print(f"残单处理, symbol = {symbol}, 残单量 = {order_amount}")
        if order_amount == 0 or abs(order_amount) * price < min_notional:
            log_print(
                f"残单下单量 = {order_amount} 或价值小于 {min_notional}U, 无需下单"
            )
            return

        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "price": price,
            "quantity": abs(order_amount),
            "timeInForce": "GTC",
            "reduceOnly": reduce_only,
            "newClientOrderId": clientId,
        }

        try:
            res = robust(
                self.exchange.papiPostMarginOrder,
                params=params,
                func_name="papiPostMarginOrder",
            )
            log_print(
                f'spot taker open order: symbol={res["symbol"]}, '
                f'side={side}, amount={order_amount}',
                level="info",
            )
            log_print(f"{symbol} 残单下单成功")
        except Exception as e:
            log_print(
                f"spot_place_taker_order {type(e).__name__}, {str(e)}",
                level="error",
            )

    def post_asset_dust(self, asset):
        params = {"asset": asset, "accountType": "MARGIN"}
        res = self.exchange.sapiPostAssetDust(params=params)
        return res
