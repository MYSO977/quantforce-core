#!/usr/bin/env python3
"""
QuantForce-Labs — risk_gate.py
节点: center (.18)
职责: 信号前置风控拦截 + 仓位管理
      熔断 → 大盘避险 → 分仓 → ATR sizing → 跟随止盈(后台线程)
用法: from risk_gate import RiskGate
"""

import threading
import time
import logging
import json
import sqlite3
import os
import socket
from datetime import datetime, date
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
        self.circuit_open   = False
        self.daily_pnl      = 0.0
        self.opening_equity = None
        self._circuit_lock  = threading.Lock()
        self._today         = date.today()
        self._market_safe   = True
        self._market_cache_ts = 0.0
        self._market_lock   = threading.Lock()
        self._positions     = {}
        self._equity        = 100_000.0
        self._pos_lock      = threading.Lock()
        self._atr_cache     = {}
        self._atr_lock      = threading.Lock()
        self._trailing      = {}
        self._trail_lock    = threading.Lock()
        self._stop_event    = threading.Event()
        self._start_background_threads()
        self.log.info(
            f"RiskGate 启动 | "
            f"daily_loss_limit={self.cfg['circuit_breaker']['daily_loss_limit_pct']}% | "
            f"max_single_pct={self.cfg['position_cap']['max_single_pct']}% | "
            f"risk_per_trade={self.cfg['atr_sizing']['risk_per_trade_pct']}%"
        )

    def evaluate(self, signal):
        ticker = signal.get("ticker", "UNKNOWN")
        action = signal.get("action", "")
        self._log_event("SIGNAL_IN", ticker, {"action": action, "raw_size": signal.get("size")})

        ok, reason = self._check_circuit_breaker()
        if not ok:
            return self._reject(ticker, "CIRCUIT_BREAKER", reason)

        ok, reason = self._check_market_regime()
        if not ok:
            return self._reject(ticker, "MARKET_REGIME", reason)

        if action == "SELL":
            self._log_event("APPROVED_SELL", ticker, {})
            return RiskDecision(approved=True, reason="SELL_PASSTHROUGH", signal=signal)

        ok, reason, max_value = self._check_position_cap(ticker)
        if not ok:
            return self._reject(ticker, "POSITION_CAP", reason)

        size, reason = self._calc_position_size(ticker, max_value)
        if size is None:
            return self._reject(ticker, "ATR_SIZING_FAIL", reason)

        adjusted = dict(signal)
        adjusted["size"]   = size
        adjusted["equity"] = self._equity
        self._log_event("APPROVED", ticker, {"final_size": size, "reason": reason})
        return RiskDecision(approved=True, reason="ALL_GATES_PASSED", signal=adjusted)

    def _check_circuit_breaker(self):
        with self._circuit_lock:
            if self.circuit_open:
                return False, f"circuit already open | daily_pnl={self.daily_pnl:.2f}"
            limit_pct = self.cfg["circuit_breaker"]["daily_loss_limit_pct"]
            limit_val = self._equity * limit_pct / 100.0
            if self.daily_pnl < -limit_val:
                self.circuit_open = True
                self.log.warning(f"🔴 熔断触发！daily_pnl={self.daily_pnl:.2f} limit=-{limit_val:.2f}")
                self._log_event("CIRCUIT_TRIGGERED", "ALL", {
                    "daily_pnl": self.daily_pnl, "limit": -limit_val, "equity": self._equity})
                return False, f"daily_pnl={self.daily_pnl:.2f} breached limit=-{limit_val:.2f}"
        return True, "ok"

    def update_pnl(self, realized_pnl_delta, ticker=""):
        with self._circuit_lock:
            self.daily_pnl += realized_pnl_delta
        self.log.info(f"PNL更新 | ticker={ticker} delta={realized_pnl_delta:+.2f} | daily_pnl={self.daily_pnl:.2f}")

    def _reset_circuit(self):
        with self._circuit_lock:
            prev = self.daily_pnl
            self.circuit_open = False
            self.daily_pnl    = 0.0
            self._today       = date.today()
        self.log.info(f"🟢 熔断重置 | prev_daily_pnl={prev:.2f} | date={self._today}")

    def _check_market_regime(self):
        with self._market_lock:
            return self._market_safe, ("ok" if self._market_safe else "SPY+QQQ both < EMA20")

    def _refresh_market_regime(self):
        try:
            cfg    = self.cfg["market_regime"]
            data   = yf.download(cfg["tickers"], period=cfg.get("data_period","60d"),
                                 interval="1d", auto_adjust=True, progress=False)["Close"]
            results = {}
            for t in cfg["tickers"]:
                if t not in data.columns:
                    continue
                s = data[t].dropna()
                ema = s.ewm(span=cfg["ema_period"], adjust=False).mean()
                results[t] = {"close": float(s.iloc[-1]), "ema20": float(ema.iloc[-1]),
                              "below": float(s.iloc[-1]) < float(ema.iloc[-1])}
            spy_down = results.get("SPY", {}).get("below", False)
            qqq_down = results.get("QQQ", {}).get("below", False)
            safe = not (spy_down and qqq_down) if cfg.get("require_both_down", True) else not (spy_down or qqq_down)
            with self._market_lock:
                self._market_safe     = safe
                self._market_cache_ts = time.time()
            self.log.info(f"大盘状态: {'SAFE' if safe else 'AVOID'} | SPY={spy_down} QQQ={qqq_down}")
        except Exception as e:
            self.log.error(f"大盘数据更新失败: {e}")

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

    def _calc_position_size(self, ticker, max_value):
        try:
            atr = self._get_atr(ticker)
            if atr is None or atr <= 0:
                return None, f"ATR无效: {atr}"
            cfg           = self.cfg["atr_sizing"]
            risk_amount   = self._equity * cfg["risk_per_trade_pct"] / 100.0
            stop_distance = atr * cfg["atr_multiplier"]
            atr_shares    = risk_amount / stop_distance
            price         = self._get_last_price(ticker)
            if price and price > 0:
                cap_shares   = max_value / price
                final_shares = min(atr_shares, cap_shares)
            else:
                final_shares = atr_shares
            final_shares = max(int(final_shares), cfg.get("min_shares", 1))
            if cfg.get("round_to_lot", False):
                final_shares = (final_shares // 100) * 100
                if final_shares < 100:
                    return None, "round_to_lot: shares < 100"
            reason = f"ATR={atr:.4f} stop={stop_distance:.4f} risk={risk_amount:.2f} shares={final_shares}"
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
            df  = yf.download(ticker, period=cfg.get("data_period","60d"),
                              interval="1d", auto_adjust=True, progress=False)
            if df.empty or len(df) < cfg["atr_period"]:
                return None
            tr  = pd.concat([df["High"]-df["Low"],
                             (df["High"]-df["Close"].shift()).abs(),
                             (df["Low"] -df["Close"].shift()).abs()], axis=1).max(axis=1)
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
            df = yf.download(ticker, period="2d", interval="1d", auto_adjust=True, progress=False)
            return float(df["Close"].iloc[-1].iloc[0]) if not df.empty else None
        except Exception:
            return None

    def register_position(self, ticker, entry_price, atr=None):
        if atr is None:
            atr = self._get_atr(ticker) or (entry_price * 0.02)
        cfg   = self.cfg["trailing_stop"]
        stop  = entry_price - atr * cfg["atr_multiplier"]
        with self._trail_lock:
            self._trailing[ticker] = {
                "entry_price": entry_price, "highest_price": entry_price,
                "trailing_stop": stop, "atr": atr, "activated": False}
        self.log.info(f"Trailing注册: {ticker} entry={entry_price:.2f} stop={stop:.2f}")

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
        min_profit = cfg.get("min_profit_to_activate_pct", 0.5) / 100.0
        with self._trail_lock:
            state = self._trailing.get(ticker)
            if not state:
                return
            atr   = state["atr"]
            entry = state["entry_price"]
            if not state["activated"]:
                if price >= entry * (1 + min_profit):
                    state["activated"] = True
                    self.log.info(f"Trailing激活: {ticker} price={price:.2f}")
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
            sock.connect("tcp://127.0.0.1:5556")
            sock.send_json({"ticker": ticker, "action": "SELL", "price": price, "source": "risk_gate_trailing", "score": 0})
            sock.close()
            self.log.info(f"Trailing SELL推送: {ticker}")
        except Exception as e:
            self.log.error(f"Trailing SELL推送失败: {ticker} {e}")

    def set_equity(self, equity):
        with self._pos_lock:
            self._equity = equity
        self.log.info(f"净值更新: equity={equity:.2f}")

    def set_position(self, ticker, market_value):
        with self._pos_lock:
            self._positions[ticker] = market_value
        self.log.info(f"持仓更新: {ticker} market_value={market_value:.2f}")

    def status(self):
        with self._circuit_lock:
            cb = {"open": self.circuit_open, "daily_pnl": self.daily_pnl, "equity": self._equity}
        with self._market_lock:
            mr = {"safe": self._market_safe}
        with self._pos_lock:
            pos = dict(self._positions)
        with self._trail_lock:
            trail = {k: dict(v) for k, v in self._trailing.items()}
        return {"circuit_breaker": cb, "market_regime": mr, "positions": pos, "trailing_stops": trail}

    def _start_background_threads(self):
        for name, target in [
            ("circuit_reset", self._circuit_reset_loop),
            ("market_regime", self._market_regime_loop),
            ("trailing_stop", self._trailing_stop_loop),
        ]:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self.log.info(f"后台线程: {name}")

    def _circuit_reset_loop(self):
        hh, mm = map(int, self.cfg["circuit_breaker"].get("reset_time","08:30").split(":"))
        while not self._stop_event.is_set():
            now = datetime.now()
            if now.date() > self._today and now.hour == hh and now.minute >= mm:
                self._reset_circuit()
            time.sleep(30)

    def _market_regime_loop(self):
        self._refresh_market_regime()
        cfg = self.cfg["market_regime"]
        ttl = cfg.get("cache_ttl_minutes", 60) * 60
        hh, mm = map(int, cfg.get("update_time","08:45").split(":"))
        _forced = None
        while not self._stop_event.is_set():
            time.sleep(60)
            now = datetime.now()
            if now.hour == hh and now.minute == mm and now.date() != _forced:
                self._refresh_market_regime(); _forced = now.date()
            elif time.time() - self._market_cache_ts > ttl:
                self._refresh_market_regime()

    def stop(self):
        self._stop_event.set()
        self.log.info("RiskGate 停止")

    def _reject(self, ticker, gate, reason):
        self.log.info(f"❌ REJECT | gate={gate} | ticker={ticker} | {reason}")
        self._log_event("REJECTED", ticker, {"gate": gate, "reason": reason})
        return RiskDecision(approved=False, reason=f"{gate}: {reason}")

    def _log_event(self, event, ticker, data):
        record = {"ts": datetime.utcnow().isoformat(), "event": event,
                  "ticker": ticker, "node": socket.gethostname(), **data}
        self.log.debug(f"EVENT: {json.dumps(record)}")
