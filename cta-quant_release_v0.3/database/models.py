# coding: utf-8
from sqlalchemy import Column, TIMESTAMP, text, DateTime
from sqlalchemy.dialects.mysql import DECIMAL, INTEGER, TINYINT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class CtaStrategy(Base):
    __tablename__ = 'cta_strategy'

    def to_dict(self):
        return {
            c.name: getattr(self, c.name, None)
            for c in self.__table__.columns
        }

    id = Column(INTEGER, primary_key=True, comment='ID')
    strategy = Column(
        VARCHAR(255),
        nullable=False,
        server_default=text("''"),
        comment='策略账户名称'
    )
    trade_type = Column(
        VARCHAR(255),
        nullable=False,
        server_default=text("''"),
        comment='策略类型, spot, margin, future, delivery'
    )
    is_pm = Column(
        INTEGER,
        nullable=False,
        server_default=text("'0'"),
        comment='是否统一账户'
    )
    is_running = Column(
        INTEGER,
        nullable=False,
        server_default=text("'0'"),
        comment='是否正在运行'
    )
    symbol = Column(
        VARCHAR(255),
        nullable=False,
        server_default=text("''"),
        comment='交易对'
    )
    interval = Column(
        VARCHAR(255),
        nullable=False,
        server_default=text("''"),
        comment='时间间隔'
    )
    cta = Column(
        VARCHAR(255),
        nullable=False,
        server_default=text("''"),
        comment='cta策略名称'
    )
    period = Column(
        VARCHAR(255),
        nullable=False,
        server_default=text("''"),
        comment='cta参数'
    )
    signal = Column(
        INTEGER,
        nullable=False,
        server_default=text("'0'"),
        comment='当前信号'
    )
    signal_time = Column(DateTime, comment='当前信号产生时间')
    open_price = Column(DECIMAL(20, 6), comment='策略开仓价')
    close_price = Column(DECIMAL(20, 6), comment='策略上次平仓价')
    position_amount = Column(
        DECIMAL(20, 5),
        nullable=False,
        server_default=text("'0.00000'"),
        comment='当前仓位'
    )
    init_value = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'0.00'"),
        comment='策略初始开仓金额'
    )
    profit = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'0.00'"),
        comment='策略盈利'
    )
    net_value = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'1.00'"),
        comment='策略当前净值'
    )
    trade_ratio = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'1.00'"),
        comment='策略杠杆'
    )
    takeprofit_percentage = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'0.30'"),
        comment='止盈比例'
    )
    takeprofit_drawdown_percentage = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'0.05'"),
        comment='吊灯止盈回调比例'
    )
    stoploss_percentage = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'0.10'"),
        comment='止损比例'
    )
    open_tpsl = Column(
        TINYINT,
        nullable=False,
        server_default=text("'1'"),
        comment='是否开启止盈止损'
    )
    is_tpsl = Column(
        TINYINT,
        nullable=False,
        server_default=text("'0'"),
        comment='是否已触发止盈止损'
    )
    is_del = Column(
        TINYINT,
        nullable=False,
        server_default=text("'0'"),
        comment='是否软删除'
    )
    update_time = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        comment='上次更新时间'
    )


class Strategy(Base):
    __tablename__ = 'strategy'

    id = Column(INTEGER, primary_key=True, comment='ID')
    strategy = Column(VARCHAR(255), nullable=False, comment='策略名称')
    account = Column(VARCHAR(255), nullable=False, comment='币安邮箱')
    apikey = Column(VARCHAR(255), nullable=False, comment='币安API')
    secret = Column(VARCHAR(255), nullable=False, comment='币安密钥')
    is_pm = Column(INTEGER, nullable=False, server_default=text("'0'"),
                   comment='是否统一账户')
    trade_ratio = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'1.00'"),
        comment='策略杠杆'
    )
    takeprofit_percentage = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'0.30'"),
        comment='止盈比例'
    )
    stoploss_percentage = Column(
        DECIMAL(10, 2),
        nullable=False,
        server_default=text("'0.10'"),
        comment='止损比例'
    )
    is_del = Column(
        TINYINT,
        nullable=False,
        server_default=text("'0'"),
        comment='是否软删除'
    )
    update_time = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        comment='上次更新时间'
    )
    is_main = Column(
        TINYINT,
        nullable=False,
        server_default=text("'0'"),
        comment='是否主账户'
    )
