import asyncio
import logging
from itertools import islice

import aiohttp


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session


async def async_retry_getter(func, max_times=5, **kwargs):
    sleep_seconds = 1
    while True:
        try:
            return await func(**kwargs)
        except Exception as e:
            if max_times == 0:
                raise e
            else:
                logging.warning('Error occurred, %s, %d times retry left',
                                str(e), max_times)

            await asyncio.sleep(sleep_seconds)
            max_times -= 1
            sleep_seconds *= 2


def batched(iterable, n):
    """
    batched('ABCDEFG', 3) --> ABC DEF G
    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def get_loop():
    """
    check if there is an event loop in the current thread, if not create one
    https://github.com/sammchardy/python-binance/blob/master/binance/helpers.py
    """
    try:
        loop = asyncio.get_event_loop()
        return loop
    except RuntimeError as e:
        if str(e).startswith("There is no current event loop in thread"):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop
        else:
            raise


STABLECOINS = {
    'BKRWUSDT', 'USDCUSDT', 'USDPUSDT', 'TUSDUSDT', 'BUSDUSDT', 'FDUSDUSDT',
    'DAIUSDT', 'EURUSDT', 'GBPUSDT',
    'USBPUSDT', 'SUSDUSDT', 'PAXGUSDT', 'AEURUSDT', 'USDSUSDT', 'USDSBUSDT'
}


def is_leverage_token(x: str):
    if x.endswith(('UPUSDT', 'DOWNUSDT', 'BEARUSDT', 'BULLUSDT')
                  ) and x != 'JUPUSDT':
        return True
    return False


def filter_symbols(symbols):
    lev_symbols = {x for x in symbols if is_leverage_token(x)}
    not_usdt_symbols = {x for x in symbols if not x.endswith('USDT')}

    excludes = set.union(not_usdt_symbols, lev_symbols,
                         STABLECOINS).intersection(symbols)

    symbols_filtered = sorted(set(symbols) - excludes)
    return symbols_filtered
