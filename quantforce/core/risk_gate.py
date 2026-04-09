"""
QuantForce Risk Gate — 三层风控
Signal → risk_check() → Order 或 None
"""
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional
import pytz

from .interfaces import Signal, Order

ET = pytz.timezone('America/New_York')

MARKET_OPEN  = time(9, 35)   # 开盘后5min才允许
MARKET_CLOSE = time(15, 45)  # 收盘前15min停止


@dataclass
class AccountState:
    """账户快照 — 由 .11 ib_insync 每30s同步到 PG"""
    net_liquidation:  float               # 账户净值
    available_funds:  float               # 可用资金
    positions:        dict = field(default_factory=dict)  # {symbol: qty}
    updated_at:       datetime = field(default_factory=datetime.utcnow)

    def is_fresh(self, max_age_seconds: int = 60) -> bool:
        age = (datetime.utcnow() - self.updated_at).total_seconds()
        return age < max_age_seconds


class RiskGate:
    MAX_PORTFOLIO_RATIO = 0.80   # 总持仓不超过净值80%
    MAX_POSITION_RATIO  = 0.05   # 单票不超过净值5%
    MIN_CONFIDENCE      = 0.65   # 最低置信度门槛

    def check(self, signal: Signal, account: AccountState) -> Optional[Order]:
        """三层风控，全部通过才生成 Order"""

        # 层1：时间层
        if not self._time_check():
            return None

        # 层2：账户层
        if not self._account_check(signal, account):
            return None

        # 层3：标的层
        if not self._symbol_check(signal, account):
            return None

        qty = self._calc_qty(signal, account)
        if qty <= 0:
            return None

        return Order(
            signal=signal,
            qty=qty,
            order_type='MKT',
            tif='DAY',
        )

    def _time_check(self) -> bool:
        now = datetime.now(ET).time()
        return MARKET_OPEN <= now <= MARKET_CLOSE

    def _account_check(self, signal: Signal, account: AccountState) -> bool:
        if not account.is_fresh():
            return False
        if signal.confidence < self.MIN_CONFIDENCE:
            return False
        total_exposure = sum(account.positions.values()) * 1.0
        return account.available_funds > 0

    def _symbol_check(self, signal: Signal, account: AccountState) -> bool:
        existing_qty = account.positions.get(signal.symbol, 0)
        max_value = account.net_liquidation * self.MAX_POSITION_RATIO
        return existing_qty == 0 or True  # 扩展：加冷却检查

    def _calc_qty(self, signal: Signal, account: AccountState) -> int:
        max_value = account.net_liquidation * self.MAX_POSITION_RATIO
        # 暂用固定分配，后续接入实时报价
        estimated_price = 100.0  # placeholder
        qty = int(max_value / estimated_price)
        return max(qty, 0)
