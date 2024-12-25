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


class BinancePmFutureAdaptor(ExchangeAdaptor):

    def __init__(self, config_manager) -> None:
        super().__init__(config_manager)
        self._init_settings()

    def _init_settings(self):
        super()._init_settings()

    def _init_task(self):
        super()._init_task()
        self.scheduler.add_job(
            id="auto_collection",
            func=self.auto_collection,
            args=[],
            trigger="cron",
            hour="*",
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

    def get_exchange_info(self):
        _exchange_info = robust(
            self.exchange.fapiPublicGetExchangeInfo,
            func_name="fapiPublicGetExchangeInfo",
        )
        _symbol_list = [
            x['symbol'] for x in _exchange_info['symbols']
            if x['status'] == 'TRADING'
        ]
        symbol_list = [
            symbol for symbol in _symbol_list
            if (symbol.endswith('USDT') or
                symbol.endswith('USDC') or
                symbol == 'ETHBTC')
        ]
        min_qty = {}
        price_precision = {}
        min_notional = {}
        exchange_info = {}

        for x in _exchange_info['symbols']:
            _symbol = x['symbol']
            if _symbol not in symbol_list:
                continue
            else:
                for _filter in x['filters']:
                    if _filter['filterType'] == 'PRICE_FILTER':
                        price_precision[_symbol] = int(
                            math.log(float(_filter['tickSize']), 0.1))
                    elif _filter['filterType'] == 'LOT_SIZE':
                        min_qty[_symbol] = int(math.log(
                            float(_filter['minQty']), 0.1))
                    elif _filter['filterType'] == 'MIN_NOTIONAL':
                        min_notional[_symbol] = float(_filter['notional'])
                exchange_info[_symbol] = {
                    "min_qty": min_qty[_symbol],
                    "price_precision": price_precision[_symbol],
                    "min_notional": min_notional[_symbol],
                }

        return exchange_info

    def get_position(self):
        res = robust(self.exchange.papiGetBalance, func_name="papiGetBalance")
        return res

    def _bnb_transfer(self):
        res = robust(self.exchange.papiGetBalance, func_name="papiGetBalance")
        um = 0
        margin = 0
        for asset in res:
            if asset["asset"] == "BNB":
                um = float(asset["umWalletBalance"])
                margin = float(asset["crossMarginFree"])
            elif asset["asset"] == "USDT":
                usdt = float(asset['crossMarginFree'])
            else:
                continue

        log_print(
            f"scan current bnb, um: {um}, margin: {margin}",
            level="info"
        )
        log_print(
            f"current usdt: {usdt}",
            level="info"
        )
        quantity = 0
        bnb_commission_amount = self.config_manager.get(
            "bnb_commission", 0.05)
        if um < bnb_commission_amount:
            quantity += bnb_commission_amount - um - margin

        info = self.exchange_info.get("BNBUSDT")
        min_qty = info.get("min_qty")
        quantity = float(f"{quantity:.{min_qty}f}")
        if quantity <= 0 and um >= bnb_commission_amount:
            log_print(f'um has sufficient bnb, um: {um}',
                      level='info')
            return
        elif quantity <= 0 and margin > 0:
            log_print(f'margin has sufficient bnb, margin: {margin}',
                      level='info')
        elif quantity <= 0 and margin == 0:
            log_print(f'margin has no bnb, margin: {margin},'
                      f'but no need to buy.',
                      level='info')
            return
        elif quantity <= 0 and um >= bnb_commission_amount:
            log_print(f'um has sufficient bnb, um: {um}',
                      level='info')
            return
        elif quantity > 0 and usdt > 0:
            quantity = max(quantity, 0.01)
            ticker_price = self.get_book_ticker()["BNBUSDT"]["askPrice"]
            if usdt < float(quantity) * float(ticker_price):
                log_print(f"usdt is not enough, usdt: {usdt}", level="info")
                return
            else:
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
        res = robust(self.exchange.papiGetBalance, func_name="papiGetBalance")
        margin = 0
        for asset in res:
            if asset["asset"] == "BNB":
                margin = float(asset["crossMarginFree"])
                break
            else:
                continue
        if margin > 0:
            res = self.exchange.papiPostBnbTransfer(
                params={"amount": margin, "transferSide": "TO_UM"}
            )
            log_print(
                f"{margin} bnb transfer to um success",
                level="info"
            )

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

    def _get_book_ticker(self):
        res = robust(
            self.exchange.fapiPublicGetTickerBookTicker,
            func_name="fapiPublicGetTickerBookTicker",
        )
        return res

    def _set_leverage(self, leverage):
        target_leverage = int(leverage)
        position_risk = robust(
            self.exchange.papiGetUmPositionRisk,
            func_name="papiGetUmPositionRisk"
        )
        leverage_info = dict(
            [(row["symbol"], int(row["leverage"])) for row in position_risk]
        )
        for symbol, leverage in leverage_info.items():  # TODO 可能会出现symbol异常
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

    def place_taker_order(self,
                          symbol,
                          order_amount,
                          clientId="place_taker_order"):
        timestamp = time.time()
        dt = datetime.fromtimestamp(timestamp)
        formatted_time = dt.strftime("%Y%m%d_%H%M%S") +\
            f"{int((timestamp % 1) * 1000):03d}"
        clientId = f'{clientId}_{formatted_time}'
        twap_amount = self.config_manager.get("twap_amount")
        side = "BUY" if order_amount > 0 else "SELL"
        price = (
            float(self.get_book_ticker()[symbol]["askPrice"]) * 1.03
            if side == "BUY"
            else float(self.get_book_ticker()[symbol]["bidPrice"]) * 0.97
        )

        pos = self.get_um_position().get(symbol, 0)
        if (
            (pos < 0 and order_amount > abs(pos)) or
            (pos > 0 and order_amount < -abs(pos))
        ):
            # 如果当前仓位是空头，且下单量超过了当前的空头仓位，或者
            # 当前仓位是多头，且下单量超过了当前的多头仓位（即将反向持仓），
            # 则这是一个增加仓位的操作，应设置 reduce_only 为 False
            reduce_only = False
        else:
            # 其他情况按照原逻辑处理
            reduce_only = abs(order_amount + pos) <= abs(pos)

        price_precision = self.exchange_info[symbol].get("price_precision", 0)
        min_notional = self.exchange_info[symbol].get("min_notional", 10)
        min_qty = self.exchange_info[symbol].get("min_qty", 0)
        price = float(f"{price:.{price_precision}f}")

        twap_order_num = math.floor(order_amount * price / twap_amount)
        for i in range(0, twap_order_num):
            if order_amount * price < twap_amount + min_notional:
                log_print(f"{symbol} 不需要进一步拆单，可直接下单")
                break
            log_print(f"{symbol} twap下单，正在进行第 {i+1} 次下单",
                      level="debug")
            quantity = twap_amount / price
            quantity = float(f"{quantity:.{min_qty}f}")
            log_print(f"本次下单量 = {quantity}", level="debug")

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
                    self.exchange.papiPostUmOrder,
                    params=params,
                    func_name="papiPostUmOrder",
                )
                log_print(
                    f'futures taker open order: symbol={res["symbol"]},'
                    f'side={side}, amount={quantity}',
                    level="info",
                )
                log_print(f"{symbol} twap下单,正在进行第 {i+1} 次下单成功",
                          level="debug")
            except Exception as e:
                log_print(
                    f"futures_place_taker_order {type(e).__name__}, {str(e)}",
                    level="error",
                )
                # TODO
                # return False
                # return 返回值布尔, send message

            order_amount -= quantity
            order_amount = float(f"{order_amount:.{min_qty}f}")
            log_print(f"剩余下单量 = {order_amount}", level="debug")
            time.sleep(2)

        order_amount = float(f"{order_amount:.{min_qty}f}")
        log_print(f"残单处理，残单量 = {order_amount}", level="debug")
        if order_amount == 0 or (
            not reduce_only and abs(order_amount) * price < min_notional
        ):
            log_print(
                f"{symbol} 残单下单量 = {order_amount} 或价值小于 {min_notional}U,无需下单",
                level="info",
            )
            return True

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
                self.exchange.papiPostUmOrder,
                params=params,
                func_name="papiPostUmOrder",
            )
            log_print(
                f'futures taker open order: symbol={res["symbol"]}, '
                f'side={side}, amount={order_amount}',
                level="info",
            )
            log_print(f"{symbol} 残单下单成功", level="debug")
            return True
        except Exception as e:
            log_print(
                f"futures_place_taker_order {type(e).__name__}, {str(e)}",
                level="error",
            )
            return False

    def set_um_positionSide(self):
        res = robust(self.exchange.papiGetUmPositionSideDual)

        if res["dualSidePosition"]:
            params = {"dualSidePosition": False}
            robust(self.exchange.papiPostUmPositionSideDual, params=params)

    def get_um_position_risk(self):
        position_risk = robust(
            self.exchange.papiGetUmPositionRisk,
            func_name="papiGetUmPositionRisk"
        )
        return position_risk

    def get_um_position(self):
        positions = self.get_um_position_risk()
        res = {}
        for position in positions:
            res[position['symbol']] = float(position['positionAmt'])
        return res

    def set_position_side(self):
        res = self._get_position_side()

        if res["dualSidePosition"]:
            params = {"dualSidePosition": False}
            robust(self.exchange.papiPostUmPositionSideDual, params=params)

    def _get_position_side(self):
        res = robust(self.exchange.papiGetUmPositionSideDual)
        return res
