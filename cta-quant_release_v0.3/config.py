import os

root_path = os.path.abspath(os.path.dirname(__file__))
data_path = os.path.join(root_path, "data")
if not os.path.exists(data_path):
    os.mkdir(data_path)
exchange_path = os.path.join(data_path, "exchange")
if not os.path.exists(exchange_path):
    os.mkdir(exchange_path)
log_path = os.path.join(root_path, "logs")
if not os.path.exists(log_path):
    os.mkdir(log_path)
order_path = os.path.join(data_path, "order")
if not os.path.exists(order_path):
    os.mkdir(order_path)
bmac_base_dir = os.path.join(data_path, "bmac_data")
if not os.path.exists(bmac_base_dir):
    os.mkdir(bmac_base_dir)
proxy = {}

# 链接mysql的uri
sql_uri = 'mysql+pymysql://user:passwd@ip:port/table'

bmac_expire_sec = 30
bmac_min_candle_num = 100
bmac_interval = '5m'

CTA_EXECUTION_FLAG = "cta_placing_order"

# 企业微信企业id
corp_id = 'wwf12345678'

corp_secret = 'YymKOH579WFP_x-'

agent_id = 1000001
# 当前环境, PROD or DEV
environment = "PROD"
