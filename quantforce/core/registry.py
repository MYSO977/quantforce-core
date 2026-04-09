"""
QuantForce Strategy Registry — 策略插件注册表
新增策略只需 register()，无需修改任何核心代码
"""
from typing import Optional
from .interfaces import Bar, Signal, Strategy


class StrategyRegistry:
    _plugins: dict[str, Strategy] = {}

    @classmethod
    def register(cls, strategy: Strategy) -> None:
        """注册策略插件"""
        cls._plugins[strategy.strategy_id] = strategy
        print(f"[Registry] 已注册: {strategy.strategy_id} (primary={strategy.is_primary})")

    @classmethod
    def unregister(cls, strategy_id: str) -> None:
        """注销策略插件"""
        cls._plugins.pop(strategy_id, None)

    @classmethod
    def run_all(cls, bar: Bar) -> list[Signal]:
        """对一根 Bar 运行所有已注册策略，返回信号列表"""
        signals = []
        # 主策略先跑
        for s in sorted(cls._plugins.values(), key=lambda x: (not x.is_primary, -x.priority)):
            if bar.asset_type in s.asset_types:
                sig = s.on_bar(bar)
                if sig:
                    signals.append(sig)
        return signals

    @classmethod
    def run_secondary(cls, bar: Bar, primary_signals: list[Signal]) -> list[Signal]:
        """次要条件：仅在主策略有信号时才运行（节省算力）"""
        if not primary_signals:
            return []
        signals = []
        for s in cls._plugins.values():
            if not s.is_primary and bar.asset_type in s.asset_types:
                sig = s.on_bar(bar)
                if sig:
                    signals.append(sig)
        return signals

    @classmethod
    def list_plugins(cls) -> list[str]:
        return list(cls._plugins.keys())
