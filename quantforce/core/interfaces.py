"""
QuantForce Core Interfaces — 系统宪法
定义 Bar / Signal / Order / Strategy 标准接口
所有模块必须遵守，禁止在此文件外修改接口定义
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# ── 品种类型 ──────────────────────────────────────────
ASSET_TYPES = ('stock', 'etf', 'fut', 'opt', 'cash', 'crypto')
ORDER_TYPES  = ('MKT', 'LMT', 'BRACKET', 'TWAP')
DIRECTIONS   = ('BUY', 'SELL', 'FLAT')


@dataclass
class Bar:
    """行情数据单元 — 由 ib_insync 行情订阅写入"""
    symbol:       str
    timestamp:    datetime
    asset_type:   str        # stock / etf / fut / opt
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       float
    vwap:         float
    rvol:         float      # 相对成交量（当日成交量 / 20日均量）
    dollar_volume: float     # close × volume，用于 Russell3000 动态排名


@dataclass
class Signal:
    """标准信号工单 — 策略插件的唯一输出格式"""
    symbol:      str
    direction:   str         # BUY / SELL / FLAT
    confidence:  float       # 0.0 ~ 1.0
    strategy_id: str         # 插件唯一ID，例如 tech_v2 / news_v4
    asset_type:  str
    reason:      str         # 可读理由
    timestamp:   datetime
    is_primary:  bool = True # True=主策略 False=次要条件（新闻）
    priority:    int  = 0    # 共振评分权重
    meta:        dict = field(default_factory=dict)  # 扩展字段


@dataclass
class Order:
    """执行订单 — Risk Gate 输出，ib_executor 消费"""
    signal:      Signal
    qty:         int         # 风控计算得出的股数
    order_type:  str = 'MKT' # MKT / LMT / BRACKET / TWAP
    limit_price: float = 0.0
    stop_price:  float = 0.0
    tif:         str = 'DAY' # DAY / GTC / IOC


class Strategy(ABC):
    """策略插件基类 — 所有策略必须继承此类"""
    strategy_id:  str        # 全局唯一，注册用
    asset_types:  list       # 支持的品种类型列表
    is_primary:   bool = True
    priority:     int  = 0

    @abstractmethod
    def on_bar(self, bar: Bar) -> Optional[Signal]:
        """每根 Bar 调用一次，返回 Signal 或 None"""
        pass

    def on_signal(self, signal: Signal) -> None:
        """可选：收到其他策略信号时的响应"""
        pass
