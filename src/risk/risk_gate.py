#!/usr/bin/env python3
"""
QuantForce-Labs — risk_gate.py  v2.0
节点: center (.18)
变更:
  M3: SMA200 + VIX双重大盘过滤（原EMA20）
  M2: 连续亏损计数 + 周熔断
  M6: 资金再平衡（原disabled）
  M1: 加滑点估算
  M4: trailing激活门槛1.5%，atr_multiplier 3.0
"""

import threading
import time
import logging
import json
import sqlite3
import os
import socket
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, Tuple

import yaml
import yfinance as yf

def _setup_logger(cfg):
    log_cfg  = cfg.get("logging", {})
    level    = getattr(logging, log_cfg.get("level", "INFO"))
    log_file = log_cfg.get("log_file", os.path.expanduser("~/logs/risk_gate.log"))
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger("risk_gate")
    logger.setLevel(level)
    if logger.handlers:
        return logger
    fmt = logging.Formatter("%(asctime)s [RISK] %(levelname)s %(message)s")
    sh  = logging.StreamHandler(); sh.setFormatter(fmt); logger.addHandler(sh)
    fh  = logging.FileHandler(log_file); fh.setFormatter(fmt); logger.addHandler(fh)
    logger.propagate = False  # 阻止消息冒泡到root logger
    return logger

class RiskDecision:
    def __init__(self, approved, reason, signal=None):
        self.approved = approved
        self.reason   = reason
        self.signal   = signal
    def __repr__(self):
        return f"RiskDecision(approved={self.approved}, reason={self.reason})"

class RiskGate:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.cfg = yaml.safe_load(f)
        self.log = _setup_logger(self.cfg)

        # ── M2: 熔断状态 ──────────────────────────────
        self.circuit_open        = False
        self.weekly_halt         = False          # 周熔断
        self.daily_pnl           = 0.0
        self.weekly_pnl          = 0.0
        self.consecutive_losses  = 0              # 连续亏损计数
        self._today              = date.today()
        self._week_start         = date.today() - timedelta(days=date.today().weekday())
        self._circuit_lock       = threading.Lock()

        # ── M3: 大盘状态 ──────────────────────────────
        self._market_safe        = True
        self._vix_level          = 0.0            # 当前VIX值
        self._size_multiplier    = 1.0            # VIX引发的仓位缩放
        self._market_cache_ts    = 0.0
        self._market_lock        = threading.Lock()

        # ── M5: 持仓与净值 ────────────────────────────
        self._positions          = {}
        self._equity             = 100_000.0
        self._peak_equity        = 100_000.0      # M6用
        self._base_equity        = 100_000.0      # M6用
        self._risk_pct_override  = None           # M6动态覆盖
        self._pos_lock           = threading.Lock()

        # ── M1: ATR缓存 ───────────────────────────────
        self._atr_cache          = {}
        self._atr_lock           = threading.Lock()

        # ── M4: Trailing Stop ─────────────────────────
        self._trailing           = {}
        self._trail_lock         = threading.Lock()
        self._stop_event         = threading.Event()

        self._start_background_threads()
        self.log.info(
            f"RiskGate v2.0 启动 | "
            f"daily_loss={self.cfg['circuit_breaker']['daily_loss_limit_pct']}% | "
            f"weekly_loss={self.cfg['circuit_breaker']['weekly_loss_limit_pct']}% | "
            f"consec_limit={self.cfg['circuit_breaker']['consecutive_loss_limit']} | "
            f"vix_block={self.cfg['market_regime']['vix_hard_block']} | "
            f"vix_half={self.cfg['market_regime']['vix_half_size']}"
        )

    # ══════════════════════════════════════════════════
    #  主入口
    # ══════════════════════════════════════════════════
    def evaluate(self, signal):
        ticker = signal.get("ticker", "UNKNOWN")
        action = signal.get("action", "")
        self._log_event("SIGNAL_IN", ticker, {"action": action, "raw_size": signal.get("size")})

        # T-014: 时间层
        from datetime import time as dtime
        from zoneinfo import ZoneInfo
        _ET = ZoneInfo("America/New_York")
        import datetime as _dt
        _now = _dt.datetime.now(tz=_ET).time()
        if not (dtime(9,30) <= _now < dtime(16,0)):
            return self._reject(ticker, "TIME", f"市场未开盘或已收盘 ({_now.strftime('%H:%M')} ET)")
        if _now < dtime(9,35):
            return self._reject(ticker, "TIME", "开盘缓冲期，09:35 ET后允许入场")
        if _now >= dtime(15,45):
            return self._reject(ticker, "TIME", "收盘前保护窗口，15:45 ET后停止入场")
                # M2: 熔断检查
        ok, reason = self._check_circuit_breaker()
        if not ok:
            return self._reject(ticker, "CIRCUIT_BREAKER", reason)

        # M3: 大盘避险
        ok, reason = self._check_market_regime()
        if not ok:
            return self._reject(ticker, "MARKET_REGIME", reason)

        # SELL直通（不受开仓限制）
        if action == "SELL":
            self._log_event("APPROVED_SELL", ticker, {})
            return RiskDecision(approved=True, reason="SELL_PASSTHROUGH", signal=signal)

        # M5: 分仓上限
        ok, reason, max_value = self._check_position_cap(ticker)
        if not ok:
            return self._reject(ticker, "POSITION_CAP", reason)

        # M1: ATR仓位计算
        size, reason = self._calc_position_size(ticker, max_value)
        if size is None:
            return self._reject(ticker, "ATR_SIZING_FAIL", reason)

        # M3: VIX引发的仓位缩放
        with self._market_lock:
            multiplier = self._size_multiplier
        if multiplier < 1.0:
            size = max(1, int(size * multiplier))
            self.log.info(f"VIX缩仓: {ticker} multiplier={multiplier:.2f} → size={size}")

        adjusted = dict(signal)
        adjusted["size"]   = size
        adjusted["equity"] = self._equity
        self._log_event("APPROVED", ticker, {"final_size": size, "vix_mult": multiplier})
        return RiskDecision(approved=True, reason="ALL_GATES_PASSED", signal=adjusted)

    # ══════════════════════════════════════════════════
    #  M2: 熔断
    # ══════════════════════════════════════════════════
    def _check_circuit_breaker(self):
        with self._circuit_lock:
            # 周熔断优先
            if self.weekly_halt:
                return False, f"weekly_halt | weekly_pnl={self.weekly_pnl:.2f}"
            if self.circuit_open:
                return False, f"daily circuit open | daily_pnl={self.daily_pnl:.2f}"

            cfg       = self.cfg["circuit_breaker"]
            # 日亏损金额
            day_limit = self._equity * cfg["daily_loss_limit_pct"] / 100.0
            if self.daily_pnl < -day_limit:
                self.circuit_open = True
                self.log.warning(f"🔴 日熔断(金额) daily_pnl={self.daily_pnl:.2f} limit=-{day_limit:.2f}")
                self._log_event("CIRCUIT_DAILY_AMOUNT", "ALL",
                                {"daily_pnl": self.daily_pnl, "limit": -day_limit})
                return False, f"daily pnl={self.daily_pnl:.2f} < -{day_limit:.2f}"
            # 连续亏损次数
            consec_limit = cfg.get("consecutive_loss_limit", 3)
            if self.consecutive_losses >= consec_limit:
                self.circuit_open = True
                self.log.warning(f"🔴 日熔断(连亏) consecutive={self.consecutive_losses} >= {consec_limit}")
                self._log_event("CIRCUIT_DAILY_STREAK", "ALL",
                                {"consecutive_losses": self.consecutive_losses})
                return False, f"consecutive_losses={self.consecutive_losses} >= {consec_limit}"
            # 周熔断
            week_limit = self._equity * cfg["weekly_loss_limit_pct"] / 100.0
            if self.weekly_pnl < -week_limit:
                self.weekly_halt = True
                self.log.warning(f"🔴 周熔断 weekly_pnl={self.weekly_pnl:.2f} limit=-{week_limit:.2f}")
                self._log_event("CIRCUIT_WEEKLY", "ALL",
                                {"weekly_pnl": self.weekly_pnl, "limit": -week_limit})
                return False, f"weekly pnl={self.weekly_pnl:.2f} < -{week_limit:.2f}"
        return True, "ok"

    def update_pnl(self, realized_pnl_delta, ticker=""):
        """每笔平仓后调用，更新PNL和连续亏损计数"""
        with self._circuit_lock:
            self.daily_pnl  += realized_pnl_delta
            self.weekly_pnl += realized_pnl_delta
            if realized_pnl_delta < 0:
                self.consecutive_losses += 1
            else:
                self.consecutive_losses = 0   # 盈利则重置连续亏损
        self.log.info(
            f"PNL更新 | {ticker} delta={realized_pnl_delta:+.2f} | "
            f"daily={self.daily_pnl:.2f} weekly={self.weekly_pnl:.2f} "
            f"streak={self.consecutive_losses}"
        )
        # 主动触发熔断检查（不等下次evaluate）
        self._trigger_circuit_if_needed()
        # 同步触发M6再平衡检查
        self._check_rebalance()

    def _trigger_circuit_if_needed(self):
        """update_pnl后主动检查，无需等evaluate才熔断"""
        cfg = self.cfg["circuit_breaker"]
        with self._circuit_lock:
            # 周熔断优先检查（累计多日，比日熔断更严重）
            week_limit = self._equity * cfg["weekly_loss_limit_pct"] / 100.0
            if not self.weekly_halt and self.weekly_pnl < -week_limit:
                self.weekly_halt  = True
                self.circuit_open = True   # 周熔断同时锁定日熔断
                self.log.warning(
                    f"🔴 周熔断 weekly_pnl={self.weekly_pnl:.2f} "
                    f"limit=-{week_limit:.2f}"
                )
                return
            if self.circuit_open:
                return
            # 日熔断：金额
            day_limit = self._equity * cfg["daily_loss_limit_pct"] / 100.0
            if self.daily_pnl < -day_limit:
                self.circuit_open = True
                self.log.warning(
                    f"🔴 日熔断(金额) daily_pnl={self.daily_pnl:.2f} "
                    f"limit=-{day_limit:.2f}"
                )
                return
            # 日熔断：连续亏损
            consec_limit = cfg.get("consecutive_loss_limit", 3)
            if self.consecutive_losses >= consec_limit:
                self.circuit_open = True
                self.log.warning(
                    f"🔴 日熔断(连亏) consecutive={self.consecutive_losses} "
                    f">= {consec_limit}"
                )

    def _reset_daily(self):
        with self._circuit_lock:
            prev = self.daily_pnl
            prev_streak = self.consecutive_losses
            self.circuit_open       = False
            self.daily_pnl          = 0.0
            self.consecutive_losses = 0
            self._today             = date.today()
        self.log.info(f"🟢 日熔断重置 | prev_pnl={prev:.2f} prev_streak={prev_streak}")

    def _reset_weekly(self):
        with self._circuit_lock:
            prev = self.weekly_pnl
            self.weekly_halt  = False
            self.weekly_pnl   = 0.0
            self._week_start  = date.today() - timedelta(days=date.today().weekday())
        self.log.info(f"🟢 周熔断重置 | prev_weekly_pnl={prev:.2f}")

    def _circuit_reset_loop(self):
        cfg   = self.cfg["circuit_breaker"]
        hh, mm = map(int, cfg.get("reset_time", "08:30").split(":"))
        while not self._stop_event.is_set():
            now = datetime.now()
            # 日重置
            if now.date() > self._today and now.hour == hh and now.minute >= mm:
                self._reset_daily()
            # 周重置（周一）
            if now.weekday() == 0 and now.date() > self._week_start and now.hour == hh and now.minute >= mm:
                self._reset_weekly()
            time.sleep(30)

    # ══════════════════════════════════════════════════
    #  M3: 大盘择时 + VIX
    # ══════════════════════════════════════════════════
    def _check_market_regime(self):
        with self._market_lock:
            if not self._market_safe:
                return False, f"market AVOID | SPY+QQQ below SMA200 | VIX={self._vix_level:.1f}"
            return True, "ok"

    def _refresh_market_regime(self):
        try:
            cfg        = self.cfg["market_regime"]
            period     = cfg.get("data_period", "250d")
            sma200_n   = cfg.get("sma200_period", 200)
            ema20_n    = cfg.get("ema20_period", 20)
            vix_block  = cfg.get("vix_hard_block", 30)
            vix_half   = cfg.get("vix_half_size", 20)

            # 下载SPY/QQQ + VIX
            tickers_all = cfg["tickers"] + [cfg.get("vix_ticker", "^VIX")]
            raw = yf.download(tickers_all, period=period,
                              interval="1d", auto_adjust=True, progress=False)["Close"]

            results = {}
            for t in cfg["tickers"]:
                if t not in raw.columns:
                    continue
                s       = raw[t].dropna()
                sma200  = s.rolling(sma200_n).mean()
                ema20   = s.ewm(span=ema20_n, adjust=False).mean()
                last    = float(s.iloc[-1])
                results[t] = {
                    "close":        last,
                    "sma200":       float(sma200.iloc[-1]),
                    "ema20":        float(ema20.iloc[-1]),
                    "below_sma200": last < float(sma200.iloc[-1]),
                    "below_ema20":  last < float(ema20.iloc[-1]),
                }

            # VIX
            vix_col = cfg.get("vix_ticker", "^VIX")
            vix_val = 0.0
            if vix_col in raw.columns:
                vix_val = float(raw[vix_col].dropna().iloc[-1])

            spy_down = results.get("SPY", {}).get("below_sma200", False)
            qqq_down = results.get("QQQ", {}).get("below_sma200", False)

            # 大盘安全判断：SPY或QQQ任一在SMA200上方 AND VIX未硬封顶
            if cfg.get("require_both_down", True):
                trend_bad = spy_down and qqq_down   # 两者都跌破才危险
            else:
                trend_bad = spy_down or qqq_down

            vix_block_triggered = vix_val >= vix_block
            safe = not trend_bad and not vix_block_triggered

            # VIX半仓逻辑
            if vix_val >= vix_block:
                size_mult = 0.0    # 完全禁止
            elif vix_val >= vix_half:
                size_mult = 0.5    # 减半
            else:
                size_mult = 1.0    # 正常

            with self._market_lock:
                self._market_safe     = safe
                self._vix_level       = vix_val
                self._size_multiplier = size_mult
                self._market_cache_ts = time.time()

            self.log.info(
                f"大盘更新: {'SAFE' if safe else 'AVOID'} | "
                f"SPY_below_SMA200={spy_down} QQQ_below_SMA200={qqq_down} | "
                f"VIX={vix_val:.1f} (block≥{vix_block} half≥{vix_half}) | "
                f"size_mult={size_mult:.1f}"
            )
            # hedge mode: 收紧现有trailing stop
            if not safe:
                self._tighten_all_stops()

        except Exception as e:
            self.log.error(f"大盘数据更新失败: {e}")

    def _tighten_all_stops(self):
        """进入避险模式：将所有trailing stop收紧到1x ATR"""
        with self._trail_lock:
            for ticker, state in self._trailing.items():
                old_stop = state["trailing_stop"]
                new_stop = state["highest_price"] - state["atr"] * 1.0
                if new_stop > old_stop:
                    state["trailing_stop"] = new_stop
                    self.log.warning(
                        f"⚠️  避险收紧止损: {ticker} "
                        f"{old_stop:.2f} → {new_stop:.2f}"
                    )

    def _market_regime_loop(self):
        self._refresh_market_regime()
        cfg    = self.cfg["market_regime"]
        ttl    = cfg.get("cache_ttl_minutes", 60) * 60
        hh, mm = map(int, cfg.get("update_time", "08:45").split(":"))
        _forced = None
        while not self._stop_event.is_set():
            time.sleep(60)
            now = datetime.now()
            if now.hour == hh and now.minute == mm and now.date() != _forced:
                self._refresh_market_regime(); _forced = now.date()
            elif time.time() - self._market_cache_ts > ttl:
                self._refresh_market_regime()

    # ══════════════════════════════════════════════════
    #  M5: 分仓上限
    # ══════════════════════════════════════════════════
    def _check_position_cap(self, ticker):
        max_pct = self.cfg["position_cap"]["max_single_pct"]
        with self._pos_lock:
            current_val = self._positions.get(ticker, 0.0)
            equity      = self._equity
        current_pct   = (current_val / equity * 100) if equity > 0 else 0
        cap_value     = equity * max_pct / 100.0
        remaining_val = cap_value - current_val
        if current_pct >= max_pct:
            reason = f"{ticker} at {current_pct:.1f}% >= cap {max_pct}%"
            self.log.info(f"分仓拦截: {reason}")
            return False, reason, 0.0
        self.log.info(f"分仓通过: {ticker} current={current_pct:.1f}% remaining={remaining_val:.0f}")
        return True, "ok", remaining_val

    # ══════════════════════════════════════════════════
    #  M1: ATR仓位计算（含滑点）
    # ══════════════════════════════════════════════════
    def _calc_position_size(self, ticker, max_value):
        try:
            atr = self._get_atr(ticker)
            if atr is None or atr <= 0:
                return None, f"ATR无效: {atr}"

            cfg            = self.cfg["atr_sizing"]
            # M6: 取动态覆盖或配置值
            with self._pos_lock:
                risk_pct = self._risk_pct_override or cfg["risk_per_trade_pct"]
            risk_amount    = self._equity * risk_pct / 100.0
            stop_distance  = atr * cfg["atr_multiplier"]
            slippage_pct   = cfg.get("slippage_pct", 0.05) / 100.0
            price          = self._get_last_price(ticker) or 0
            slippage_amt   = price * slippage_pct if price > 0 else 0
            atr_shares     = risk_amount / (stop_distance + slippage_amt)

            if price > 0:
                cap_shares   = max_value / price
                final_shares = min(atr_shares, cap_shares)
            else:
                final_shares = atr_shares

            final_shares = max(int(final_shares), cfg.get("min_shares", 1))
            if cfg.get("round_to_lot", False):
                final_shares = (final_shares // 100) * 100
                if final_shares < 100:
                    return None, "round_to_lot: shares < 100"

            reason = (f"ATR={atr:.4f} stop={stop_distance:.4f} "
                      f"slip={slippage_amt:.4f} risk={risk_amount:.2f} "
                      f"risk_pct={risk_pct}% shares={final_shares}")
            self.log.info(f"ATR sizing: {ticker} | {reason}")
            return final_shares, reason
        except Exception as e:
            return None, f"ATR sizing exception: {e}"

    def _get_atr(self, ticker):
        with self._atr_lock:
            cached = self._atr_cache.get(ticker)
            if cached and (time.time() - cached[1]) < 3600:
                return cached[0]
        try:
            import pandas as pd
            cfg = self.cfg["atr_sizing"]
            df  = yf.download(ticker, period=cfg.get("data_period", "60d"),
                              interval="1d", auto_adjust=True, progress=False)
            if df.empty or len(df) < cfg["atr_period"]:
                return None
            tr  = pd.concat([
                df["High"] - df["Low"],
                (df["High"] - df["Close"].shift()).abs(),
                (df["Low"]  - df["Close"].shift()).abs()
            ], axis=1).max(axis=1)
            atr = float(tr.ewm(span=cfg["atr_period"], adjust=False).mean().iloc[-1])
            with self._atr_lock:
                self._atr_cache[ticker] = (atr, time.time())
            self.log.info(f"ATR: {ticker}={atr:.4f}")
            return atr
        except Exception as e:
            self.log.error(f"ATR获取失败: {ticker} {e}")
            return None

    def _get_last_price(self, ticker):
        try:
            df = yf.download(ticker, period="2d", interval="1d",
                             auto_adjust=True, progress=False)
            if df.empty:
                return None
            val = df["Close"].iloc[-1]
            return float(val.iloc[0]) if hasattr(val, "iloc") else float(val)
        except Exception:
            return None

    # ══════════════════════════════════════════════════
    #  M4: Trailing Stop
    # ══════════════════════════════════════════════════
    def register_position(self, ticker, entry_price, atr=None):
        if atr is None:
            atr = self._get_atr(ticker) or (entry_price * 0.02)
        cfg  = self.cfg["trailing_stop"]
        stop = entry_price - atr * cfg["atr_multiplier"]
        with self._trail_lock:
            self._trailing[ticker] = {
                "entry_price":   entry_price,
                "highest_price": entry_price,
                "trailing_stop": stop,
                "atr":           atr,
                "activated":     False,
            }
        self.log.info(f"Trailing注册: {ticker} entry={entry_price:.2f} stop={stop:.2f} atr={atr:.4f}")

    def _trailing_stop_loop(self):
        cfg = self.cfg["trailing_stop"]
        while not self._stop_event.is_set():
            time.sleep(cfg.get("poll_interval_sec", 60))
            if not cfg.get("enabled", True):
                continue
            with self._trail_lock:
                tickers = list(self._trailing.keys())
            for ticker in tickers:
                try:
                    self._check_trailing(ticker)
                except Exception as e:
                    self.log.error(f"Trailing检查异常: {ticker} {e}")

    def _check_trailing(self, ticker):
        price = self._get_last_price(ticker)
        if price is None:
            return
        cfg        = self.cfg["trailing_stop"]
        min_profit = cfg.get("min_profit_to_activate_pct", 1.5) / 100.0
        with self._trail_lock:
            state = self._trailing.get(ticker)
            if not state:
                return
            atr   = state["atr"]
            entry = state["entry_price"]
            if not state["activated"]:
                if price >= entry * (1 + min_profit):
                    state["activated"] = True
                    self.log.info(f"Trailing激活: {ticker} price={price:.2f} entry={entry:.2f} profit={((price/entry)-1)*100:.2f}%")
                else:
                    return
            if price > state["highest_price"]:
                state["highest_price"] = price
                state["trailing_stop"] = price - atr * cfg["atr_multiplier"]
                self.log.info(f"Trailing更新: {ticker} high={price:.2f} stop={state['trailing_stop']:.2f}")
            stop = state["trailing_stop"]
        if price < stop:
            self.log.warning(f"🔴 Trailing触发: {ticker} price={price:.2f} < stop={stop:.2f}")
            self._log_event("TRAILING_TRIGGERED", ticker, {"price": price, "stop": stop, "entry": entry})
            self._push_sell(ticker, price)
            with self._trail_lock:
                self._trailing.pop(ticker, None)

    def _push_sell(self, ticker, price):
        try:
            import zmq
            ctx  = zmq.Context.instance()
            sock = ctx.socket(zmq.PUSH)
            sock.connect("tcp://192.168.0.11:5557")   # 直连executor，不绕dispatcher
            sock.send_json({
                "ticker": ticker, "action": "SELL",
                "price": price, "source": "risk_gate_trailing", "score": 0
            })
            sock.close()
            self.log.info(f"Trailing SELL → executor直推: {ticker} @ {price:.2f}")
        except Exception as e:
            self.log.error(f"Trailing SELL推送失败: {ticker} {e}")

    # ══════════════════════════════════════════════════
    #  M6: 资金再平衡
    # ══════════════════════════════════════════════════
    def _check_rebalance(self):
        cfg = self.cfg.get("rebalancing", {})
        if not cfg.get("enabled", False):
            return
        with self._pos_lock:
            equity      = self._equity
            peak        = self._peak_equity
            base        = self._base_equity
            orig_risk   = self.cfg["atr_sizing"]["risk_per_trade_pct"]

        up_thr   = cfg.get("rebalance_up_pct",   10.0) / 100.0
        down_thr = cfg.get("rebalance_down_pct",  8.0) / 100.0
        rec_thr  = cfg.get("min_recovery_pct",   98.0) / 100.0

        # 扩仓：净值增长超阈值
        if (equity - base) / base >= up_thr:
            with self._pos_lock:
                self._base_equity        = equity
                self._peak_equity        = max(peak, equity)
                self._risk_pct_override  = None   # 恢复正常风险比例
            self.log.info(f"📈 M6 扩仓再平衡 | equity={equity:.0f} new_base={equity:.0f}")
            return

        # 缩仓：从峰值回撤超阈值
        if peak > 0 and (peak - equity) / peak >= down_thr:
            mult = equity / peak
            new_risk = round(orig_risk * mult, 4)
            with self._pos_lock:
                self._risk_pct_override = new_risk
            self.log.warning(
                f"📉 M6 缩仓再平衡 | peak={peak:.0f} equity={equity:.0f} "
                f"drawdown={(peak-equity)/peak*100:.1f}% | "
                f"risk_pct {orig_risk}% → {new_risk}%"
            )
            return

        # 恢复满仓：接近峰值
        if self._risk_pct_override is not None and equity >= peak * rec_thr:
            with self._pos_lock:
                self._risk_pct_override = None
            self.log.info(f"✅ M6 仓位恢复 | equity={equity:.0f} peak={peak:.0f}")

    # ══════════════════════════════════════════════════
    #  状态接口
    # ══════════════════════════════════════════════════
    def set_equity(self, equity):
        with self._pos_lock:
            self._equity      = equity
            self._peak_equity = max(self._peak_equity, equity)
        self.log.info(f"净值更新: equity={equity:.2f} peak={self._peak_equity:.2f}")
        self._check_rebalance()

    def set_position(self, ticker, market_value):
        with self._pos_lock:
            self._positions[ticker] = market_value
        self.log.info(f"持仓更新: {ticker} market_value={market_value:.2f}")

    def status(self):
        with self._circuit_lock:
            cb = {
                "open":               self.circuit_open,
                "weekly_halt":        self.weekly_halt,
                "daily_pnl":          self.daily_pnl,
                "weekly_pnl":         self.weekly_pnl,
                "consecutive_losses": self.consecutive_losses,
                "equity":             self._equity,
            }
        with self._market_lock:
            mr = {
                "safe":         self._market_safe,
                "vix":          self._vix_level,
                "size_mult":    self._size_multiplier,
            }
        with self._pos_lock:
            rb = {
                "base_equity":       self._base_equity,
                "peak_equity":       self._peak_equity,
                "risk_pct_override": self._risk_pct_override,
            }
            pos = dict(self._positions)
        with self._trail_lock:
            trail = {k: dict(v) for k, v in self._trailing.items()}
        return {
            "circuit_breaker": cb,
            "market_regime":   mr,
            "rebalancing":     rb,
            "positions":       pos,
            "trailing_stops":  trail,
        }

    # ══════════════════════════════════════════════════
    #  内部工具
    # ══════════════════════════════════════════════════
    def _start_background_threads(self):
        for name, target in [
            ("circuit_reset", self._circuit_reset_loop),
            ("market_regime", self._market_regime_loop),
            ("trailing_stop", self._trailing_stop_loop),
        ]:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self.log.info(f"后台线程启动: {name}")

    def stop(self):
        self._stop_event.set()
        self.log.info("RiskGate 停止")

    def _reject(self, ticker, gate, reason):
        self.log.info(f"❌ REJECT | gate={gate} | ticker={ticker} | {reason}")
        self._log_event("REJECTED", ticker, {"gate": gate, "reason": reason})
        return RiskDecision(approved=False, reason=f"{gate}: {reason}")

    def _log_event(self, event, ticker, data):
        record = {
            "ts": datetime.utcnow().isoformat(),
            "event": event, "ticker": ticker,
            "node": socket.gethostname(), **data
        }
        self.log.debug(f"EVENT: {json.dumps(record)}")
