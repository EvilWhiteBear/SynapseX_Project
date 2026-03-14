# -*- coding: utf-8 -*-
"""
portfolio.py — Trade Journal + Portfolio
─────────────────────────────────────────
• Хранение сделок в SQLite (signals.db)
• Авто-запись при сигнале AI
• Фоновый мониторинг TP/SL через MEXC цены
• Авто-удаление записей старше 60 дней
• Equity curve за 30 дней
"""
import sqlite3
import threading
import time
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger("PORTFOLIO")

# ── Инициализация таблиц ──────────────────────────────────────────────────────

def init_portfolio_db(db_path: str):
    """Создаёт таблицы portfolio и trades если не существуют."""
    with sqlite3.connect(db_path) as conn:
        # Портфолио пользователя (баланс)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio (
                uid           TEXT PRIMARY KEY,
                balance       REAL DEFAULT 10000.0,
                initial_bal   REAL DEFAULT 10000.0,
                updated_at    TEXT
            )
        """)
        # Сделки журнала
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                uid         TEXT    NOT NULL,
                asset       TEXT    NOT NULL,
                direction   TEXT    NOT NULL,
                entry       REAL    NOT NULL,
                exit_price  REAL    DEFAULT 0,
                size        REAL    NOT NULL,
                leverage    INTEGER DEFAULT 1,
                tp1         REAL    DEFAULT 0,
                tp2         REAL    DEFAULT 0,
                sl          REAL    DEFAULT 0,
                pnl         REAL    DEFAULT 0,
                status      TEXT    DEFAULT 'OPEN',
                result      TEXT    DEFAULT '',
                note        TEXT    DEFAULT '',
                opened_at   TEXT    NOT NULL,
                closed_at   TEXT    DEFAULT '',
                signal_id   INTEGER DEFAULT 0
            )
        """)
        conn.commit()
    log.info("[PORTFOLIO] БД инициализирована")


# ── Portfolio (баланс) ────────────────────────────────────────────────────────

def get_portfolio(db_path: str, uid: str) -> dict:
    """Возвращает портфолио пользователя. Создаёт если не существует."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM portfolio WHERE uid=?", (uid,)
        ).fetchone()
        if not row:
            return {"uid": uid, "balance": 10000.0,
                    "initial_bal": 10000.0, "updated_at": ""}
        return dict(row)


def set_initial_balance(db_path: str, uid: str, balance: float) -> dict:
    """Устанавливает начальный баланс (только если нет открытых позиций)."""
    ts = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        open_count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE uid=? AND status='OPEN'", (uid,)
        ).fetchone()[0]
        if open_count > 0:
            return {"error": "CLOSE_POSITIONS_FIRST"}
        conn.execute("""
            INSERT INTO portfolio (uid, balance, initial_bal, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                balance=excluded.balance,
                initial_bal=excluded.initial_bal,
                updated_at=excluded.updated_at
        """, (uid, balance, balance, ts))
        conn.commit()
    return {"status": "OK", "balance": balance}


def _update_balance(db_path: str, uid: str, delta: float):
    """Изменяет баланс на delta (положительное = прибыль, отрицательное = убыток)."""
    ts = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO portfolio (uid, balance, initial_bal, updated_at)
            VALUES (?, 10000+?, 10000.0, ?)
            ON CONFLICT(uid) DO UPDATE SET
                balance = balance + ?,
                updated_at = ?
        """, (uid, delta, ts, delta, ts))
        conn.commit()


# ── Trades ────────────────────────────────────────────────────────────────────

def open_trade(db_path: str, uid: str, asset: str, direction: str,
               entry: float, size: float, leverage: int,
               tp1: float, tp2: float, sl: float,
               note: str = "", signal_id: int = 0) -> dict:
    """Открывает новую сделку."""
    ts = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("""
            INSERT INTO trades
                (uid, asset, direction, entry, size, leverage,
                 tp1, tp2, sl, status, opened_at, note, signal_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?)
        """, (uid, asset, direction, entry, size, leverage,
              tp1, tp2, sl, ts, note, signal_id))
        conn.commit()
        trade_id = cur.lastrowid
    log.info(f"[PORTFOLIO] Открыта сделка #{trade_id} {asset} {direction} @{entry}")
    return {"status": "OK", "trade_id": trade_id}


def close_trade(db_path: str, trade_id: int, uid: str,
                exit_price: float, result: str = "") -> dict:
    """Закрывает сделку, считает PnL, обновляет баланс."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        trade = conn.execute(
            "SELECT * FROM trades WHERE id=? AND uid=? AND status='OPEN'",
            (trade_id, uid)
        ).fetchone()
        if not trade:
            return {"error": "TRADE_NOT_FOUND"}
        trade = dict(trade)

        # Считаем PnL
        direction = 1 if trade['direction'] == 'LONG' else -1
        coins     = (trade['size'] * trade['leverage']) / trade['entry']
        pnl       = direction * coins * (exit_price - trade['entry'])
        pnl       = round(pnl, 4)

        # Определяем результат если не передан
        if not result:
            result = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "BE"

        ts = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE trades SET
                exit_price=?, pnl=?, status='CLOSED',
                result=?, closed_at=?
            WHERE id=? AND uid=?
        """, (exit_price, pnl, result, ts, trade_id, uid))
        conn.commit()

    # Обновляем баланс
    _update_balance(db_path, uid, pnl)
    log.info(f"[PORTFOLIO] Закрыта сделка #{trade_id} PnL={pnl:+.2f}")
    return {"status": "OK", "pnl": pnl, "result": result}


def get_trades(db_path: str, uid: str, status: str = "",
               limit: int = 100) -> list:
    """Возвращает сделки пользователя."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if status:
            rows = conn.execute(
                "SELECT * FROM trades WHERE uid=? AND status=? ORDER BY id DESC LIMIT ?",
                (uid, status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trades WHERE uid=? ORDER BY id DESC LIMIT ?",
                (uid, limit)
            ).fetchall()
    return [dict(r) for r in rows]


def get_equity_curve(db_path: str, uid: str, days: int = 30) -> list:
    """
    Возвращает equity curve за N дней.
    Каждый элемент — (date_str, cumulative_pnl).
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("""
            SELECT date(closed_at) as d, SUM(pnl) as day_pnl
            FROM trades
            WHERE uid=? AND status='CLOSED' AND closed_at >= ?
            GROUP BY date(closed_at)
            ORDER BY d ASC
        """, (uid, since)).fetchall()

    portfolio = get_portfolio(db_path, uid)
    base = portfolio.get('initial_bal', 10000.0)
    curve, cumulative = [], base
    for row in rows:
        cumulative += row[1]
        curve.append({"date": row[0], "equity": round(cumulative, 2)})
    return curve


# ── Авто-мониторинг TP/SL ─────────────────────────────────────────────────────

class TPSLMonitor:
    """
    Фоновый поток — каждые 30 секунд проверяет открытые позиции.
    Если цена достигла TP1/TP2/SL — автоматически закрывает сделку.
    """
    def __init__(self, db_path: str, get_price_fn):
        self.db_path      = db_path
        self.get_price_fn = get_price_fn   # функция(symbol) → float
        self._stop        = threading.Event()
        self._thread      = None

    def start(self):
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="TPSLMonitor"
        )
        self._thread.start()
        log.info("[TPSL] Монитор запущен (интервал 30с)")

    def stop(self):
        self._stop.set()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self._check_all()
            except Exception as e:
                log.error(f"[TPSL] Ошибка проверки: {e}")
            self._stop.wait(30)

    def _check_all(self):
        """Проверяет все открытые позиции."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                open_trades = conn.execute(
                    "SELECT * FROM trades WHERE status='OPEN' AND (tp1>0 OR sl>0)"
                ).fetchall()
        except Exception:
            return

        if not open_trades:
            return

        # Группируем по активу чтобы не запрашивать одну цену много раз
        prices = {}
        for trade in open_trades:
            asset = trade['asset']
            if asset not in prices:
                try:
                    prices[asset] = self.get_price_fn(asset)
                except Exception:
                    prices[asset] = 0.0

        for trade in open_trades:
            trade   = dict(trade)
            price   = prices.get(trade['asset'], 0.0)
            if not price:
                continue

            uid      = trade['uid']
            trade_id = trade['id']
            direction = trade['direction']
            tp1 = trade['tp1'] or 0
            tp2 = trade['tp2'] or 0
            sl  = trade['sl']  or 0

            hit_tp = False
            hit_sl = False

            if direction == 'LONG':
                if tp1 > 0 and price >= tp1:
                    hit_tp = True
                elif sl > 0 and price <= sl:
                    hit_sl = True
            else:  # SHORT
                if tp1 > 0 and price <= tp1:
                    hit_tp = True
                elif sl > 0 and price >= sl:
                    hit_sl = True

            if hit_tp:
                exit_price = tp1
                result     = "WIN"
                log.info(f"[TPSL] TP достигнут: #{trade_id} {trade['asset']} @{price:.4f}")
                close_trade(self.db_path, trade_id, uid, exit_price, result)

            elif hit_sl:
                exit_price = sl
                result     = "LOSS"
                log.info(f"[TPSL] SL достигнут: #{trade_id} {trade['asset']} @{price:.4f}")
                close_trade(self.db_path, trade_id, uid, exit_price, result)


# ── Авто-очистка старых записей ───────────────────────────────────────────────

def cleanup_old_trades(db_path: str, days: int = 60):
    """Удаляет закрытые сделки старше N дней."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.execute(
                "DELETE FROM trades WHERE status='CLOSED' AND closed_at < ?",
                (cutoff,)
            )
            conn.commit()
            if cur.rowcount:
                log.info(f"[PORTFOLIO] Удалено {cur.rowcount} сделок старше {days} дней")
    except Exception as e:
        log.error(f"[PORTFOLIO] cleanup error: {e}")


def start_cleanup_scheduler(db_path: str, days: int = 60):
    """Запускает ежедневную очистку старых сделок."""
    def _scheduler():
        while True:
            cleanup_old_trades(db_path, days)
            time.sleep(86400)  # раз в сутки
    threading.Thread(target=_scheduler, daemon=True, name="CleanupScheduler").start()
    log.info(f"[PORTFOLIO] Автоочистка запущена (хранить {days} дней)")
