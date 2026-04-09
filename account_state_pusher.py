"""
account_state_pusher.py  —  QuantForce Labs
节点: Dell (.11)
功能: 每 30 秒从 IB Gateway 拉取账户状态，写入 PG account_state 表
"""

import logging
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PUSHER] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("account_state_pusher")

ET             = ZoneInfo("America/New_York")
PUSH_INTERVAL  = 30
IB_HOST        = "127.0.0.1"
IB_PORT        = 4002
IB_CLIENT_ID   = 12

_IB_TAGS = ",".join([
    "NetLiquidation",
    "GrossPositionValue",
    "TotalCashValue",
    "BuyingPower",
    "DailyPnL",
])

class _AccountWrapper(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self._data: dict = {}
        self._done = False

    def accountSummary(self, reqId, account, tag, value, currency):
        self._data[tag] = float(value) if value else 0.0

    def accountSummaryEnd(self, reqId):
        self._done = True

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in (2104, 2106, 2158):
            log.warning(f"IB error {errorCode}: {errorString}")


def fetch_account_from_ib() -> dict | None:
    app = _AccountWrapper()
    try:
        app.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
        app.reqAccountSummary(9001, "All", _IB_TAGS)
        deadline = time.time() + 8
        while not app._done and time.time() < deadline:
            app.run()
            time.sleep(0.05)
        app.disconnect()
        if not app._done:
            log.warning("IB 账户快照超时")
            return None
        return app._data
    except Exception as e:
        log.error(f"IB 连接失败: {e}")
        return None


def _get_conn():
    return psycopg2.connect(os.environ["QUANT_PG_DSN"])


def _get_equity_start(conn) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT equity_start FROM account_state
            WHERE DATE(updated_at AT TIME ZONE 'America/New_York') = CURRENT_DATE
              AND equity_start IS NOT NULL
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return float(row[0]) if row else None


def push_to_db(conn, ib_data: dict, equity_start: float):
    nav            = ib_data.get("NetLiquidation", 0.0)
    total_exposure = ib_data.get("GrossPositionValue", 0.0)
    cash           = ib_data.get("TotalCashValue", 0.0)
    buying_power   = ib_data.get("BuyingPower", 0.0)
    day_pnl        = ib_data.get("DailyPnL", 0.0)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO account_state
                (nav, equity_start, day_pnl, total_exposure, cash, buying_power,
                 net_liquidation, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT ((TRUE)) DO UPDATE SET
                nav            = EXCLUDED.nav,
                net_liquidation= EXCLUDED.net_liquidation,
                day_pnl        = EXCLUDED.day_pnl,
                total_exposure = EXCLUDED.total_exposure,
                cash           = EXCLUDED.cash,
                buying_power   = EXCLUDED.buying_power,
                updated_at     = EXCLUDED.updated_at
            """,
            (nav, equity_start, day_pnl, total_exposure, cash, buying_power, nav),
        )
    conn.commit()
    log.info(
        f"NAV={nav:,.0f}  exposure={total_exposure:,.0f} ({total_exposure/nav*100:.1f}% NAV)"
        f"  day_pnl={day_pnl:+,.0f}  cash={cash:,.0f}"
    ) if nav > 0 else log.info("NAV=0, IB 数据异常")


def main():
    log.info("account_state_pusher 启动")
    equity_start_cache: float | None = None

    while True:
        try:
            conn    = _get_conn()
            ib_data = fetch_account_from_ib()

            if ib_data is None:
                log.warning("跳过本次推送（IB 数据不可用）")
                conn.close()
                time.sleep(PUSH_INTERVAL)
                continue

            nav = ib_data.get("NetLiquidation", 0.0)

            if equity_start_cache is None:
                equity_start_cache = _get_equity_start(conn)
            if equity_start_cache is None:
                equity_start_cache = nav
                log.info(f"equity_start 初始化: {equity_start_cache:,.0f}")

            push_to_db(conn, ib_data, equity_start_cache)
            conn.close()

        except Exception as e:
            log.error(f"推送失败: {e}")

        time.sleep(PUSH_INTERVAL)


if __name__ == "__main__":
    main()
