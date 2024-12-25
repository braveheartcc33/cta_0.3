import asyncio
from config import bmac_base_dir
from data_center.bmac import bmac


class Bmac:
    """
    Binance Marketdata Async Client
    """

    def start(self, base_dir):
        asyncio.run(bmac.main(base_dir))


if __name__ == '__main__':

    Bmac().start(bmac_base_dir)
