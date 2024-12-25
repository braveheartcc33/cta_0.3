import asyncio

from data_center.bmac import bmac


class Bmac:
    """
    Binance Marketdata Async Client
    """

    def start(self, base_dir):
        asyncio.run(bmac.main(base_dir))
