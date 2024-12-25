# Cta量化平台

Cta量化平台

## Getting started

conda create -n cta_quant python==3.10.9

pip install -r requirements-dev.txt


## 代码风格检查

follow 牛马船队代码组规范 pep8 check

pip install -r requirements-dev.txt

pre-commit install

正常代码commit后会自动进行检查，按照规则修复后提交至代码库

flake8 文档链接 https://flake8.pycqa.org/en/latest/index.html#quickstart


## 运行说明

1. pm2 启动bmac, 5m k线预热大约需要30min
2. cta程序入口main.py, 可通过pm2添加配置启动
3. 配置xxx.json 及 config.py启动程序
