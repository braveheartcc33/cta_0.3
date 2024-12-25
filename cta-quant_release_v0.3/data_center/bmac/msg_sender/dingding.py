import base64
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import quote_plus

import aiohttp

from data_center.bmac.util import async_retry_getter


def retry_getter(func,
                 retry_times=5,
                 sleep_seconds=1,
                 default=None,
                 raise_err=True):
    for i in range(retry_times):
        try:
            return func()
        except Exception as e:
            logging.warn(f'An error occurred {str(e)}')
            if i == retry_times - 1 and raise_err:
                raise e
            time.sleep(sleep_seconds)
            sleep_seconds *= 2
    return


class DingDingSender:

    def __init__(self, aiohttp_session, secret, access_token):
        self.secret = secret
        self.access_token = access_token
        self.session: aiohttp.ClientSession = aiohttp_session

    def generate_post_url(self):
        secret = self.secret
        access_token = self.access_token
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc,
                             digestmod=hashlib.sha256).digest()
        sign = quote_plus(base64.b64encode(hmac_code))
        url = (
            f'https://oapi.dingtalk.com/robot/send?'
            f'access_token={access_token}&'
            f'timestamp={timestamp}&sign={sign}'
        )
        return url

    async def send_message(self, msg):
        post_url = self.generate_post_url()
        headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
        req_json_str = json.dumps({"msgtype": "text",
                                   "text": {"content": msg}})
        await async_retry_getter(lambda: self.session.post(
            url=post_url,
            data=req_json_str,
            headers=headers))
