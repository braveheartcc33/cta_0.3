import sys
import time
import json
import logging
import pandas as pd
import numpy as np
from functools import wraps
from datetime import datetime
import requests
from config import environment, corp_id, corp_secret, agent_id, proxy
eps = 1e-8

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)  # 最多显示数据的行数


# 自定义 Formatter 类，以包含微秒
class MicrosecondFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
            return "%s.%03d" % (s, record.msecs)
        else:
            t = time.strftime(self.default_time_format, ct)
            return self.default_msec_format % (t, record.msecs)


logger = logging.getLogger(__name__)
if environment == "PROD":
    logger.setLevel(logging.INFO)  # 设置最低日志级别
else:
    logger.setLevel(logging.DEBUG)  # 设置最低日志级别

for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# 定义向sys.stdout输出的处理程序
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.addFilter(
    lambda record: record.levelno < logging.ERROR
)  # 只处理低于ERROR级别的日志
stdout_format = MicrosecondFormatter(
    "[%(asctime)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
stdout_handler.setFormatter(stdout_format)

# 定义向sys.stderr输出的处理程序
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)  # 只处理ERROR及以上级别的日志
stderr_format = MicrosecondFormatter(
    "[%(asctime)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
stderr_handler.setFormatter(stderr_format)

# 将这两个处理程序添加到日志记录器
logger.addHandler(stdout_handler)
logger.addHandler(stderr_handler)


# debug, info, warning, error, critical
def log_print(*objects, level="info", sep=" ", end="\n", flush=False):
    msg = sep.join(map(str, objects)) + end
    if level == "info":
        logger.info(msg)
    elif level == "error":
        logger.error(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "debug":
        logger.debug(msg)
    elif level == "critical":
        logger.critical(msg)
    else:
        logger.info(msg)

    if flush:
        for handler in logger.handlers:
            if hasattr(handler, "flush"):
                handler.flush()


def robust(func, params={}, func_name="", retry_times=10, sleep_seconds=1):
    for _ in range(retry_times):
        try:
            return func(params=params)
        except Exception as e:
            print(str(e))
            msg = str(e).replace("binance", "").strip()
            error_code = None
            if not msg:
                log_print(
                    f"{func_name} occured a Error. Error message is empty."
                    f"Original error: {e}",
                    level="error",
                )
            try:
                error_code = json.loads(msg)["code"]
                if error_code in (
                    -2022,
                    -2025,
                    -2011,
                    -2013,
                    -4015,
                    -4169,
                    -4171,
                    -5022,
                ):
                    raise e
            except json.decoder.JSONDecodeError:
                log_print(
                    f"{func_name} occured a Error. "
                    f"Error message is not valid JSON. Original error: {e}",
                    level="error",
                )
            if _ == (retry_times - 1):
                log_print(
                    f"{func_name} occured a Error. "
                    f"出现意料之外的报错，重试{retry_times}次未成功，error={e}",
                    level="error",
                )
                raise e
            time.sleep(sleep_seconds)


def generate_signal_data(df):
    # 画买卖点
    df['ctime'] = df['candle_begin_time'].apply(str)
    signal_data = df[~np.isnan(df['signal'])][['ctime', 'high', 'signal']]

    def get_act(x):  # 通过signal判断仓位方向
        if x > 0:
            return {'formatter': '多'}
        elif x < 0:
            return {'formatter': '空'}
        else:
            return {'formatter': '平'}

    def set_color(x):  # 设置不同操作的颜色
        if x > 0:
            return {'color': 'rgb(214,18,165)'}
        elif x < 0:
            return {'color': 'rgb(0,0,255)'}
        else:
            return {'color': 'rgb(224,136,11)'}

    signal_data.loc[:, 'label'] = np.vectorize(get_act, otypes=[str]
                                               )(signal_data['signal'])
    signal_data.loc[:, 'itemStyle'] = np.vectorize(set_color, otypes=[str]
                                                   )(signal_data['signal'])
    del signal_data['signal']
    signal_data.columns = ['xAxis', 'yAxis', 'label', 'itemStyle']
    signal_data = signal_data.to_dict('records')
    return signal_data


# =====企业微信机器人推送消息
def send_message(message, max_try_count=5):
    try_count = 0
    while True:
        try_count += 1
        try:
            access_token = get_access_token()
            msg = {
                "touser": "@all",
                "msgtype": "text",
                "agentid": agent_id,
                "text": {
                    "content":
                    message + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")
                }
            }
            headers = {"Content-Type": "application/json;charset=utf-8"}
            base_url = 'https://qyapi.weixin.qq.com/cgi-bin/message/send'
            url = f'{base_url}?access_token={access_token}'
            response = requests.post(url,
                                     json=msg,
                                     headers=headers,
                                     proxies=proxy if proxy else None).json()
            if response['errcode'] != 0:
                raise Exception(response['errmsg'])
            log_print('企业微信已发送')
            break
        except Exception as e:
            if try_count > max_try_count:
                log_print("发送企业微信失败：", e)
                break
            else:
                log_print("发送企业微信报错，重试：", e)


def get_access_token():
    url = (
        'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s'
        % (corp_id, corp_secret)
    )
    response = requests.get(url, proxies=proxy if proxy else None)
    access_token = response.json()['access_token']
    return access_token


def with_session(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        session = self.Session()  # 每个任务独立创建 session
        try:
            # 将 session 传递给被装饰的函数
            result = func(self, session, *args, **kwargs)
            # session.commit()  # 成功时提交事务
            return result
        except Exception:
            session.rollback()  # 出错时回滚事务
            raise
        finally:
            session.close()  # 确保关闭 session
    return wrapper
