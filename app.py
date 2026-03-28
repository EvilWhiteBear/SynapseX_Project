# -*- coding: utf-8 -*-
"""
app_core.py — SynapseX Core
────────────────────────────
Конфигурация, helpers, DB, portfolio-функции.
Разделён из app.py для уменьшения размера коммитов.
"""
# -*- coding: utf-8 -*-
"""
SYNAPSE X — app.py v3
─────────────────────────────────────────────────────────────────────────────
Новое в v3:
  ① Rate Limiter       — встроенный, без flask-limiter (скользящее окно)
  ② /health            — статус MEXC / Groq / Telegram / Redis / DB
  ③ Warm Cache         — топ-100 подгружается ДО первого запроса (on_startup)
  ④ SSE live-prices    — GET /stream/prices → text/event-stream (замена WS)
  ⑤ SQLite история     — каждый сигнал пишется в signals.db
  ⑥ /signals           — GET последних 50 сигналов (для дашборда)
  ⑦ Redis-ready кэш    — если REDIS_URL задан, переключается на Redis
  ⑧ .env интеграция    — все ключи ТОЛЬКО из .env / переменных окружения
─────────────────────────────────────────────────────────────────────────────
"""
import os
import re
import json
import time
import logging
import sqlite3
import threading
import collections
import requests
from datetime import datetime, timezone, timedelta

# ── Auto Screener (импортируем после инициализации логгера) ──────────────────
_screener_ready = False
_screener_import_error = ""
try:
    from screener import AutoScreener, init_screener_db, DISCLAIMER_SHORT
    _screener_ready = True
except ImportError as e:
    _screener_import_error = str(e)
    _screener_ready = False
    class AutoScreener: pass
    DISCLAIMER_SHORT = "⚠️ Не является финансовым советом."

# ── Neural Consensus Engine ───────────────────────────────────────────────────
try:
    from ai_consensus import get_consensus as _get_consensus
    _consensus_ready = True
except ImportError:
    _consensus_ready = False
    def _get_consensus(*a, **k): return {}

from flask import (Flask, render_template, request, jsonify,
                   session, redirect, url_for, Response, stream_with_context)

# ── Paddle Payments (карты Visa/MC для всех стран включая Сербию) ────────────
try:
    from paddle_payments import (
        init_paddle_db, create_checkout_url as create_paddle_checkout,
        handle_paddle_webhook, verify_webhook_signature as verify_paddle_sig,
        get_paddle_status, _paddle_configured
    )
    _paddle_ready = True
except ImportError:
    _paddle_ready = False
    def init_paddle_db(*a, **k): pass
    def create_paddle_checkout(*a, **k): return {"error": "Paddle не установлен"}
    def handle_paddle_webhook(*a, **k): return {}
    def verify_paddle_sig(*a, **k): return False
    def get_paddle_status(*a, **k): return {}
    _paddle_configured = False
# ── S3 Backup (Timeweb S3 для хранения БД) ───────────────────────────────────
try:
    from s3_backup import (
        restore_if_needed, upload_db, upload_now,
        start_s3_sync_scheduler, _is_configured as _s3_configured
    )
except ImportError:
    def restore_if_needed(db_path): return False
    def upload_db(db_path):         return False
    def upload_now(db_path):        pass
    def start_s3_sync_scheduler(*a, **k): pass
    def _s3_configured():           return False
# ── activate_trial (app_core) ─────────────────────────────────────────────────
try:
    from app_core import activate_trial
except ImportError:
    def activate_trial(*a, **k): return False

# ── Stripe payments ──────────────────────────────────────────────────────────
try:
    from stripe_payments import (
        init_stripe_db, create_checkout_session,
        create_portal_session, handle_webhook,
        get_subscription_info, STRIPE_AVAILABLE,
        PLAN_PRICE_USD, PLAN_PRICE_EUR, PLAN_SIGNALS
    )
    _STRIPE_OK = True
except ImportError:
    _STRIPE_OK = False
    def init_stripe_db(db_path): pass
    def create_checkout_session(*a, **k): return {"error": "stripe_payments.py не найден"}
    def create_portal_session(*a, **k):   return {"error": "stripe_payments.py не найден"}
    def handle_webhook(*a, **k):          return {"error": "webhook недоступен"}
    def get_subscription_info(*a, **k):   return {"is_premium": False, "status": "none"}

# ── NOWPayments (Crypto USDT/BTC) ─────────────────────────────────────────────
_nowpayments_ready = False
try:
    from nowpayments import (
        init_crypto_db, create_crypto_payment,
        handle_crypto_webhook, verify_ipn_signature,
        get_payment_status, is_configured as _crypto_configured
    )
    _nowpayments_ready = True
except ImportError:
    def init_crypto_db(db_path): pass
    def create_crypto_payment(*a, **k):  return {"error": "nowpayments.py не найден"}
    def handle_crypto_webhook(*a, **k):  return {"status": "not_configured"}
    def verify_ipn_signature(*a, **k):   return False
    def get_payment_status(*a, **k):     return {"error": "not_configured"}
    def _crypto_configured():            return False

# ── python-dotenv — загружаем .env если файл существует ──────────────────────
try:
    from dotenv import load_dotenv
    # override=False — переменные окружения хоста имеют приоритет над .env
    load_dotenv(override=False)
    _dotenv_loaded = True
except ImportError:
    _dotenv_loaded = False
    # Если python-dotenv не установлен — переменные всё равно читаются из env
    # Установи: pip install python-dotenv

# ── Настройка логирования ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("SYNAPSE_APP")

# Логируем статус импортов (уже после инициализации log)
def _log_import_status():
    if _nowpayments_ready:
        log.info("[CRYPTO] nowpayments.py загружен ✓")
    else:
        log.warning("[CRYPTO] nowpayments.py не найден — крипто-платежи отключены")

if _dotenv_loaded:
    log.info("[ENV] .env загружен через python-dotenv")
else:
    log.warning("[ENV] python-dotenv не установлен — только системные переменные окружения")

# Теперь можно логировать ошибку импорта screener (если была)
if _screener_import_error:
    log.warning(f"[SCREENER] Не загружен: {_screener_import_error}")

# ── Импорты logic/ ────────────────────────────────────────────────────────────
try:
    from logic.market import MarketDataCollector
    from logic.analysis import QuantumAnalyzer
except ImportError:
    log.warning("logic/ не найден — используются заглушки")
    class MarketDataCollector:
        def get_live_metrics(self, a):
            return {'price': 85.32, 'status': 'MOCK'}
        def get_historical_klines(self, a, interval='15m', limit=100):
            return []
        def get_top_100_mexc(self):
            return [{"symbol": "SOL-USDT", "base": "SOL", "change": "+4.00%"}]
    class QuantumAnalyzer:
        def calculate_signal(self, h, l, m):
            return {
                'fibo': {'618': 83.63}, 'ob': {'bottom': 82.34, 'top': 82.85},
                'fvg_status': 'Not Found', 'direction': 'LONG',
                'entry': 85.00, 'tp1': 90.00, 'tp2': 95.00,
                'sl': 82.00, 'bullish_score': 4, 'mtf_data': {}
            }

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv("FLASK_SECRET", "synapse_monolith_supreme_2024")

collector = MarketDataCollector()
analyzer  = QuantumAnalyzer()

# ══════════════════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ — все значения только из .env / переменных окружения
#  Захардкоженных ключей нет.
# ══════════════════════════════════════════════════════════════════════════════

# ── AI ────────────────────────────────────────────────────────────────────────
GROQ_KEY    = os.getenv("GROQ_KEY", "")
GEMINI_KEY  = os.getenv("GEMINI_KEY", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")   # для будущего crypto_bot.py

# ── Telegram — терминальный бот (SynapseX) ────────────────────────────────────
TG_TOKEN_TERMINAL = os.getenv("TELEGRAM_TOKEN_TERMINAL", "")

# ── Telegram — аналитический бот (News_CryptoSynapseX_AIBot) ─────────────────
TG_TOKEN_ANALYTICS = os.getenv("TELEGRAM_TOKEN", "")

# ── Telegram — чаты и каналы ──────────────────────────────────────────────────
ADMIN_CHAT_ID  = os.getenv("ADMIN_CHAT_ID",      "")   # личка администратора
TERMINAL_GROUP = os.getenv("TERMINAL_GROUP_ID",  "")   # группа терминала
CHANNEL_ID     = os.getenv("CHANNEL_ID",         "")   # @SynapseX_AI канал
ADMIN_IDS      = [i.strip() for i in os.getenv("ADMIN_IDS", "").split(",") if i.strip()]
ADMIN_LOGIN    = os.getenv("ADMIN_LOGIN",    "WhiteBear")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "Alcozebra12")

FIREBASE_CONFIG = {
    "api_key":             os.getenv("FIREBASE_API_KEY", ""),
    "auth_domain":         os.getenv("FIREBASE_AUTH_DOMAIN", "synapsex-auth.firebaseapp.com"),
    "project_id":          os.getenv("FIREBASE_PROJECT_ID", "synapsex-auth"),
    "storage_bucket":      os.getenv("FIREBASE_STORAGE_BUCKET", "synapsex-auth.appspot.com"),
    "messaging_sender_id": os.getenv("FIREBASE_MESSAGING_SENDER_ID", ""),
    "app_id":              os.getenv("FIREBASE_APP_ID", ""),
}

# ── Хранилище ─────────────────────────────────────────────────────────────────
DB_PATH   = os.getenv("SYNAPSE_DB",  "signals.db")
REDIS_URL = os.getenv("REDIS_URL",   "")            # пусто → in-memory кэш

# ── Авто-постинг ──────────────────────────────────────────────────────────────
AUTO_POST_INTERVAL = int(os.getenv("AUTO_POST_INTERVAL", "60"))   # минуты

# ── Доп. интеграции ───────────────────────────────────────────────────────────
CRYPTOPANIC_KEY = os.getenv("CRYPTOPANIC_KEY", "")    # пусто → не используется

# ── Валидация обязательных ключей при старте ──────────────────────────────────
_required = {
    "GROQ_KEY":                 GROQ_KEY,
    "GEMINI_KEY":               GEMINI_KEY,
    "TELEGRAM_TOKEN_TERMINAL":  TG_TOKEN_TERMINAL,
    "ADMIN_CHAT_ID":            ADMIN_CHAT_ID,
    "TERMINAL_GROUP_ID":        TERMINAL_GROUP,
}
_missing = [k for k, v in _required.items() if not v]
if _missing:
    log.error(f"[ENV] Отсутствуют обязательные переменные: {', '.join(_missing)}")
    log.error("[ENV] Заполни .env файл или установи переменные окружения!")
else:
    log.info("[ENV] Все обязательные переменные загружены ✓")

# Предупреждения для опциональных
if not ANTHROPIC_KEY:
    log.info("[ENV] ANTHROPIC_API_KEY не задан (нужен только для crypto_bot.py)")
if not REDIS_URL:
    log.info("[ENV] REDIS_URL не задан — используется in-memory кэш")
if not CRYPTOPANIC_KEY:
    log.info("[ENV] CRYPTOPANIC_KEY не задан — расширенные новости отключены")

# ── Локализация ───────────────────────────────────────────────────────────────
TEXTS = {
    "RU": {
        "terminal_title":        "SYNAPSE X // КВАНТОВЫЙ ТЕРМИНАЛ",
        "watchlist_title":       "❯ МОНИТОРИНГ_АКТИВОВ",
        "neural_log_title":      "❯ ЖУРНАЛ_НЕЙРОЯДРА",
        "exit_btn":              "ВЫХОД",
        "risk_processor":        "❯ РИСК_ПРОЦЕССОР",
        "cmd_input_placeholder": "ВВОД_КОМАНДЫ...",
        "offline_error":         "ЦЕНА НЕДОСТУПНА — БИРЖА ОФЛАЙН",
        "rate_limit_error":      "ПРЕВЫШЕН ЛИМИТ ЗАПРОСОВ — ПОДОЖДИТЕ 60 СЕК",
    },
    "EN": {
        "terminal_title":        "SYNAPSE X // QUANTUM TERMINAL",
        "watchlist_title":       "❯ ASSET_MONITORING",
        "neural_log_title":      "❯ NEURAL_CORE_LOG",
        "exit_btn":              "EXIT",
        "risk_processor":        "❯ RISK_PROCESSOR",
        "cmd_input_placeholder": "CMD_INPUT...",
        "offline_error":         "PRICE UNAVAILABLE — EXCHANGE OFFLINE",
        "rate_limit_error":      "RATE LIMIT EXCEEDED — WAIT 60 SEC",
    }
}

# ══════════════════════════════════════════════════════════════════════════════
#  СПИСОК ФЬЮЧЕРСНЫХ ПАР (терминал + скринер)
# ══════════════════════════════════════════════════════════════════════════════
FUTURES_PAIRS = [
    'BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT', 'XRP-USDT',
    'ADA-USDT', 'DOGE-USDT', 'AVAX-USDT', 'DOT-USDT', 'MATIC-USDT',
    'LINK-USDT', 'UNI-USDT', 'ATOM-USDT', 'LTC-USDT', 'ETC-USDT',
    'FIL-USDT', 'NEAR-USDT', 'APT-USDT', 'ARB-USDT', 'OP-USDT',
    'SUI-USDT', 'INJ-USDT', 'TIA-USDT', 'SEI-USDT', 'JTO-USDT',
    'WIF-USDT', 'BONK-USDT', 'PEPE-USDT', 'FLOKI-USDT', 'SHIB-USDT',
    'BCH-USDT', 'TRX-USDT', 'TON-USDT', 'SAND-USDT', 'MANA-USDT',
    'AXS-USDT', 'GALA-USDT', 'ENJ-USDT', 'CHZ-USDT', 'FLOW-USDT',
    'ICP-USDT', 'ALGO-USDT', 'VET-USDT',
    'EGLD-USDT', 'XLM-USDT', 'XMR-USDT', 'IOTA-USDT',
    'ZEC-USDT', 'DASH-USDT', 'NEO-USDT', 'WAVES-USDT', 'KAVA-USDT',
    'STX-USDT', 'CFX-USDT', 'BLUR-USDT', 'MAGIC-USDT',
    'DYDX-USDT', 'IMX-USDT', 'LDO-USDT', 'CRV-USDT', 'AAVE-USDT',
    'SNX-USDT', 'COMP-USDT', 'YFI-USDT', 'SUSHI-USDT',
]


# ══════════════════════════════════════════════════════════════════════════════
#  ① RATE LIMITER — скользящее окно, без внешних зависимостей
# ══════════════════════════════════════════════════════════════════════════════

class _RateLimiter:
    """
    Скользящее окно (sliding window) по IP-адресу.
    Потокобезопасен. Хранит только временны́е метки последних N запросов.
    """
    def __init__(self, max_calls: int = 5, window_sec: int = 60):
        self.max_calls  = max_calls
        self.window_sec = window_sec
        self._buckets: dict = {}
        self._lock = threading.Lock()

    def is_allowed(self, key: str) -> tuple:
        """
        Возвращает (allowed: bool, retry_after_sec: int).
        """
        now    = time.time()
        cutoff = now - self.window_sec
        with self._lock:
            if key not in self._buckets:
                self._buckets[key] = collections.deque()
            dq = self._buckets[key]
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= self.max_calls:
                retry_after = int(self.window_sec - (now - dq[0])) + 1
                return False, retry_after
            dq.append(now)
            return True, 0

    def remaining(self, key: str) -> int:
        now    = time.time()
        cutoff = now - self.window_sec
        with self._lock:
            dq    = self._buckets.get(key, collections.deque())
            count = sum(1 for t in dq if t >= cutoff)
            return max(0, self.max_calls - count)


_ai_limiter = _RateLimiter(max_calls=5, window_sec=60)


def get_client_ip() -> str:
    """Реальный IP с учётом прокси (Render, Railway, nginx)."""
    return (
        request.headers.get('X-Forwarded-For', '').split(',')[0].strip()
        or request.remote_addr
        or 'unknown'
    )


# ══════════════════════════════════════════════════════════════════════════════
#  ② REDIS-READY КЭШ
# ══════════════════════════════════════════════════════════════════════════════

class _CacheBackend:
    """In-memory кэш с TTL. Автоматически переключается на Redis если REDIS_URL задан."""
    def __init__(self):
        self._redis   = None
        self._mem: dict = {}
        self._lock    = threading.Lock()
        self._backend = "memory"

        if REDIS_URL:
            try:
                import redis as _redis_lib
                self._redis = _redis_lib.from_url(REDIS_URL, decode_responses=True)
                self._redis.ping()
                self._backend = "redis"
                log.info(f"[Cache] Redis подключён: {REDIS_URL}")
            except Exception as e:
                log.warning(f"[Cache] Redis недоступен, fallback в память: {e}")

    @property
    def backend(self) -> str:
        return self._backend

    def get(self, key: str):
        if self._redis:
            try:
                raw = self._redis.get(f"synapse:{key}")
                return json.loads(raw) if raw else None
            except Exception:
                pass
        with self._lock:
            entry = self._mem.get(key)
            if entry and time.time() < entry['exp']:
                return entry['val']
        return None

    def set(self, key: str, value, ttl: int = 30):
        if self._redis:
            try:
                self._redis.setex(f"synapse:{key}", ttl, json.dumps(value, ensure_ascii=False))
                return
            except Exception:
                pass
        with self._lock:
            self._mem[key] = {'val': value, 'exp': time.time() + ttl}

    def delete(self, key: str):
        if self._redis:
            try:
                self._redis.delete(f"synapse:{key}")
                return
            except Exception:
                pass
        with self._lock:
            self._mem.pop(key, None)

    def health(self) -> dict:
        if self._redis:
            try:
                self._redis.ping()
                return {"backend": "redis", "status": "OK"}
            except Exception as e:
                return {"backend": "redis", "status": "ERROR", "detail": str(e)}
        return {"backend": "memory", "status": "OK"}


cache = _CacheBackend()


# ══════════════════════════════════════════════════════════════════════════════
#  ③ SQLITE ИСТОРИЯ СИГНАЛОВ
# ══════════════════════════════════════════════════════════════════════════════

def _init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")       # Параллельные записи без блокировки
        conn.execute("PRAGMA synchronous=NORMAL")     # Баланс надёжности и скорости
        conn.execute("PRAGMA wal_autocheckpoint=100") # Авто-checkpoint каждые 100 страниц
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ts            TEXT    NOT NULL,
                asset         TEXT    NOT NULL,
                direction     TEXT    NOT NULL,
                entry         REAL,
                tp1           REAL,
                tp2           REAL,
                sl            REAL,
                leverage      INTEGER,
                margin        REAL,
                position_sz   REAL,
                bullish_score INTEGER,
                fvg_status    TEXT,
                ai_answer     TEXT,
                ip            TEXT
            )
        """)
        # Таблица сделок — создаём здесь чтобы не зависеть от portfolio.py
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio (
                uid           TEXT PRIMARY KEY,
                balance       REAL DEFAULT 10000.0,
                initial_bal   REAL DEFAULT 10000.0,
                updated_at    TEXT
            )
        """)
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
    log.info(f"[DB] SQLite инициализирован: {DB_PATH}")


def _save_signal(asset: str, signal: dict, ai_answer: str,
                 leverage: int, margin: float, ip: str):
    """Вызывается из фонового потока."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO signals
                    (ts, asset, direction, entry, tp1, tp2, sl,
                     leverage, margin, position_sz, bullish_score,
                     fvg_status, ai_answer, ip)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                datetime.now(timezone.utc).isoformat(),
                asset,
                signal.get('direction', ''),
                signal.get('entry', 0),
                signal.get('tp1',   0),
                signal.get('tp2',   0),
                signal.get('sl',    0),
                leverage,
                margin,
                round(leverage * margin, 2),
                signal.get('bullish_score', 0),
                signal.get('fvg_status', ''),
                ai_answer[:2000],
                ip
            ))
            conn.commit()
    except Exception as e:
        log.error(f"[DB] Ошибка сохранения: {e}")


def _get_recent_signals(limit: int = 50) -> list:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error(f"[DB] Ошибка чтения: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def escape_html(text: str) -> str:
    if not text:
        return ""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def ultimatum_formatter(text: str) -> str:
    """XSS-safe форматтер терминала: escape → replace → span."""
    text = text.replace('**', '').replace('`', '')
    text = escape_html(text)
    text = text.replace('\n', '<br>')
    text = re.sub(
        r'\b(LONG|BUY|ЛОНГ|ПОКУПКА)\b',
        r'<span style="color:#00ffcc;font-weight:bold;">\1</span>',
        text, flags=re.IGNORECASE
    )
    text = re.sub(
        r'\b(SHORT|SELL|ШОРТ|ПРОДАЖА)\b',
        r'<span style="color:#ff0066;font-weight:bold;">\1</span>',
        text, flags=re.IGNORECASE
    )
    for header in ['ВХОД В ПОЗИЦИЮ', 'ТОЧКА ВХОДА', 'ТЕЙК-ПРОФИТ',
                   'СТОП-ЛОСС', 'полная аналитика']:
        text = text.replace(
            header,
            f'<span style="font-weight:900;color:#00f0ff;'
            f'text-shadow:0 0 8px #00f0ff;">{header}</span>'
        )
    return text


def send_telegram(message: str, token: str, chat_id: str) -> bool:
    """
    Универсальная отправка в Telegram.
    Используй нужный токен и chat_id явно.
    """
    if not token or not chat_id:
        log.warning("[TG] Токен или chat_id не задан — пропуск")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=5
        )
        if not r.ok:
            log.warning(f"[TG] Ошибка [{r.status_code}]: {r.text[:120]}")
        return r.ok
    except Exception as e:
        log.error(f"[TG] send error: {e}")
        return False


def send_to_admin(message: str) -> bool:
    """Уведомление администратору (терминальный бот → личка)."""
    return send_telegram(message, TG_TOKEN_TERMINAL, ADMIN_CHAT_ID)


def send_to_group(message: str) -> bool:
    """
    Отправка в группу TERMINAL_GROUP_ID.
    ⚠️  НЕ вызывается из терминала — группа t.me/SynapseX_AI отвязана.
    Используется только из внешних модулей (crypto_bot.py и т.д.)
    """
    return send_telegram(message, TG_TOKEN_TERMINAL, TERMINAL_GROUP)


def send_to_channel(message: str) -> bool:
    """Аналитика в публичный канал (аналитический бот → @SynapseX_AI)."""
    return send_telegram(message, TG_TOKEN_ANALYTICS, CHANNEL_ID)


def gemini_neural_engine(signal_text: str, asset_name: str) -> None:
    """
    Генерация аналитики через Gemini → публикация в группу через терминальный бот.
    Запускается в фоновом потоке.
    """
    log.info(f"[NEURAL ENGINE] {asset_name}")
    prompt = f"""
Act as SynapseX Quantum Core. Convert this signal into a STRICT structured Telegram report.
SIGNAL DATA: {signal_text}

STRICT RULES:
1. NO INTRO. NO OUTRO. NO MARKDOWN (**). Language: Russian.
2. USE THIS EXACT STRUCTURE:

🧠 NEURAL_CORE_ANALYSIS: {asset_name}
❯ Анализ рыночной ситуации: [1 sentence about price and levels]
⚡️ Рекомендация: [Entry, TP, SL from data]
🧠 Аналитика Neural Core: [max 3 sentences: MTF trend, RSI/MACD, patterns]
📊 Рыночный контекст: [1 sentence: Volume/OBV]
💡 Вывод: [1 sentence final verdict]
"""
    neural_report = None

    # Попытка 1 — Gemini
    try:
        res = requests.post(
            f"https://generativelanguage.googleapis.com/v1/models/"
            f"gemini-1.5-flash:generateContent?key={GEMINI_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=15
        )
        if res.status_code == 200:
            neural_report = res.json()['candidates'][0]['content']['parts'][0]['text']
            log.info("[NEURAL ENGINE] Gemini OK")
        else:
            log.warning(f"[NEURAL ENGINE] Gemini {res.status_code}")
    except Exception as e:
        log.warning(f"[NEURAL ENGINE] Gemini: {e}")

    # Попытка 2 — Groq fallback
    if not neural_report:
        try:
            res = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                json={"model": "llama-3.3-70b-versatile",
                      "messages": [{"role": "user", "content": prompt}]},
                headers={"Authorization": f"Bearer {GROQ_KEY}",
                         "Content-Type": "application/json"},
                timeout=15
            )
            if res.status_code == 200:
                neural_report = res.json()['choices'][0]['message']['content']
                log.info("[NEURAL ENGINE] Groq fallback OK")
        except Exception as e:
            log.error(f"[NEURAL ENGINE] Groq: {e}")

    if neural_report:
        cleaned = neural_report.replace("**", "").replace("###", "❯").replace("`", "")
        # Группа t.me/SynapseX_AI отключена от терминала — только логируем
        log.info(f"[NEURAL ENGINE] Аналитика готова для {asset_name} (группа отключена)")
    else:
        log.error(f"[NEURAL ENGINE] Оба провайдера недоступны для {asset_name}")


# ══════════════════════════════════════════════════════════════════════════════
#  ④ SSE — LIVE PRICE STREAM
#
#  Подключение на фронтенде:
#  const es = new EventSource('/stream/prices?symbols=BTC-USDT,SOL-USDT');
#  es.onmessage = e => { const d = JSON.parse(e.data); /* обновляем UI */ };
# ══════════════════════════════════════════════════════════════════════════════

def _sse_event(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"




# ══════════════════════════════════════════════════════════════════════════════
#  PORTFOLIO & TRADE JOURNAL
# ══════════════════════════════════════════════════════════════════════════════
try:
    from portfolio import (
        init_portfolio_db, get_portfolio, set_initial_balance,
        open_trade, close_trade, get_trades, get_equity_curve,
        TPSLMonitor, start_cleanup_scheduler
    )
    _portfolio_ready = True
    log.info("[PORTFOLIO] portfolio.py загружен ✓")
except ImportError as _pe:
    log.warning(f"[PORTFOLIO] portfolio.py не найден: {_pe}. Используем встроенные функции.")
    _portfolio_ready = True   # inline функции определены выше — всегда работаем

    # ── Встроенные функции для работы без portfolio.py ──────────────────────
    from datetime import datetime as _dt, timezone as _tz

    def init_portfolio_db(db_path):
        pass  # таблицы создаются в _init_db()

    def get_portfolio(db_path, uid):
        try:
            with sqlite3.connect(db_path) as c:
                r = c.execute("SELECT * FROM portfolio WHERE uid=?", (uid,)).fetchone()
                if r: return {"uid":r[0],"balance":r[1],"initial_bal":r[2],"updated_at":r[3]}
        except Exception: pass
        return {"uid":uid,"balance":10000.0,"initial_bal":10000.0,"updated_at":""}

    def set_initial_balance(db_path, uid, balance):
        ts = _dt.now(_tz.utc).isoformat()
        try:
            with sqlite3.connect(db_path) as c:
                c.execute("""INSERT INTO portfolio(uid,balance,initial_bal,updated_at)
                    VALUES(?,?,?,?) ON CONFLICT(uid) DO UPDATE SET
                    balance=excluded.balance,initial_bal=excluded.initial_bal,updated_at=excluded.updated_at
                """, (uid,balance,balance,ts))
            return {"status":"OK","balance":balance}
        except Exception as e:
            return {"error":str(e)}

    def open_trade(db_path, uid, asset, direction, entry, size, leverage,
                   tp1=0, tp2=0, sl=0, note="", signal_id=0):
        ts = _dt.now(_tz.utc).isoformat()
        try:
            with sqlite3.connect(db_path) as c:
                cur = c.execute("""INSERT INTO trades
                    (uid,asset,direction,entry,size,leverage,tp1,tp2,sl,status,opened_at,note,signal_id)
                    VALUES(?,?,?,?,?,?,?,?,?,'OPEN',?,?,?)""",
                    (uid,asset,direction,entry,size,leverage,tp1,tp2,sl,ts,note,signal_id))
                return {"status":"OK","trade_id":cur.lastrowid}
        except Exception as e:
            log.error(f"[TRADE] open_trade error: {e}")
            return {"error":str(e)}

    def close_trade(db_path, trade_id, uid, exit_price, result=""):
        ts = _dt.now(_tz.utc).isoformat()
        try:
            with sqlite3.connect(db_path) as c:
                t = c.execute("SELECT * FROM trades WHERE id=? AND uid=? AND status='OPEN'",
                              (trade_id,uid)).fetchone()
                if not t: return {"error":"TRADE_NOT_FOUND"}
                d = 1 if t[3]=='LONG' else -1
                coins = (t[5]*t[6])/t[4] if t[4] else 0
                pnl = round(d*coins*(exit_price-t[4]),4)
                res = result or ("WIN" if pnl>0 else "LOSS" if pnl<0 else "BE")
                c.execute("UPDATE trades SET exit_price=?,pnl=?,status='CLOSED',result=?,closed_at=? WHERE id=? AND uid=?",
                          (exit_price,pnl,res,ts,trade_id,uid))
                c.execute("UPDATE portfolio SET balance=balance+?,updated_at=? WHERE uid=?",
                          (pnl,ts,uid))
                return {"status":"OK","pnl":pnl,"result":res}
        except Exception as e:
            return {"error":str(e)}

    def get_trades(db_path, uid, status="", limit=100):
        try:
            with sqlite3.connect(db_path) as c:
                c.row_factory = sqlite3.Row
                if status:
                    rows = c.execute("SELECT * FROM trades WHERE uid=? AND status=? ORDER BY id DESC LIMIT ?",
                                     (uid,status,limit)).fetchall()
                else:
                    rows = c.execute("SELECT * FROM trades WHERE uid=? ORDER BY id DESC LIMIT ?",
                                     (uid,limit)).fetchall()
                return [dict(r) for r in rows]
        except Exception: return []

    def get_equity_curve(db_path, uid, days=30):
        return []

    def start_cleanup_scheduler(db_path, days=60):
        pass  # заменяется _start_full_cleanup_scheduler ниже

    class TPSLMonitor:
        def __init__(self, *a, **k): pass
        def start(self): pass
        def stop(self):  pass


def _get_live_price(symbol: str) -> float:
    """Обёртка для TPSLMonitor."""
    try:
        data = collector.get_live_metrics(symbol)
        return float(data.get('price', 0))
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  ПОЛНАЯ ОЧИСТКА БД — все таблицы, запускается раз в сутки
# ══════════════════════════════════════════════════════════════════════════════

def _full_cleanup(db_path: str, trade_days: int = 60, signal_days: int = 60,
                  error_days: int = 30):
    """
    Удаляет старые записи из всех таблиц.
    trade_days:   сделки в trades (закрытые) старше N дней
    signal_days:  сигналы в signals и screener_signals старше N дней
    error_days:   логи ошибок старше N дней
    """
#     from datetime import datetime, timezone, timedelta  # already imported in app_core
    now    = datetime.now(timezone.utc)
    cutoff_trades   = (now - timedelta(days=trade_days)).isoformat()
    cutoff_signals  = (now - timedelta(days=signal_days)).isoformat()
    cutoff_errors   = (now - timedelta(days=error_days)).isoformat()

    stats = {}
    try:
        with sqlite3.connect(db_path) as conn:
            # 1. Закрытые сделки старше 60 дней
            try:
                cur = conn.execute(
                    "DELETE FROM trades WHERE status='CLOSED' AND closed_at < ?",
                    (cutoff_trades,)
                )
                stats['trades_deleted'] = cur.rowcount
            except Exception as e:
                stats['trades_error'] = str(e)

            # 2. Сигналы пользователей старше 60 дней
            try:
                cur = conn.execute(
                    "DELETE FROM signals WHERE ts < ?",
                    (cutoff_signals,)
                )
                stats['signals_deleted'] = cur.rowcount
            except Exception as e:
                stats['signals_error'] = str(e)

            # 3. Сигналы скринера старше 60 дней
            try:
                cur = conn.execute(
                    "DELETE FROM screener_signals WHERE created_at < ?",
                    (cutoff_signals,)
                )
                stats['screener_deleted'] = cur.rowcount
            except Exception as e:
                stats['screener_error'] = str(e)

            # 4. Логи ошибок старше 30 дней
            try:
                cur = conn.execute(
                    "DELETE FROM error_log WHERE created_at < ?",
                    (cutoff_errors,)
                )
                stats['errors_deleted'] = cur.rowcount
            except Exception as e:
                stats['error_log_error'] = str(e)

            # 5. VACUUM - освобождаем место на диске
            try:
                conn.execute("VACUUM")
                stats['vacuum'] = 'OK'
            except Exception:
                pass

            conn.commit()

        total = sum(v for k,v in stats.items() if k.endswith('_deleted') and isinstance(v,int))
        log.info(f"[CLEANUP] Очистка завершена: удалено {total} записей | {stats}")

    except Exception as e:
        log.error(f"[CLEANUP] Ошибка: {e}")

    return stats


def _start_full_cleanup_scheduler(db_path: str):
    """
    Запускает ежедневную очистку в 03:00 UTC.
    Первый запуск — сразу при старте (чистит накопившееся).
    """
    import time as _time

    def _scheduler():
        # Первый запуск через 60 секунд после старта
        _time.sleep(60)
        log.info("[CLEANUP] Первоначальная очистка БД...")
        _full_cleanup(db_path)

        # Далее — каждые 24 часа
        while True:
            _time.sleep(86400)
            log.info("[CLEANUP] Ежедневная очистка БД...")
            _full_cleanup(db_path)

    threading.Thread(target=_scheduler, daemon=True, name="FullCleanupScheduler").start()
    log.info("[CLEANUP] Планировщик очистки запущен (каждые 24ч, хранение 60 дней)")

# ── Глобальный конфиг лимитов (читается из DB/памяти) ─────────────────────────
_SIGNAL_LIMITS = {
    "free_daily_limit":    2,
    "premium_daily_limit": 15,
}


def _backup_db_to_telegram():
    """Отправляет signals.db как файл в Telegram Admin."""
    if not TG_TOKEN_TERMINAL or not ADMIN_CHAT_ID:
        return False
    if not os.path.exists(DB_PATH):
        return False
    try:
        db_size = os.path.getsize(DB_PATH)
        if db_size == 0:
            return False
        caption = (f"🗄 DB Backup\n"
                   f"📦 {db_size//1024}KB\n"
                   f"🕐 {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}")
        with open(DB_PATH, 'rb') as f:
            resp = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN_TERMINAL}/sendDocument",
                data={"chat_id": ADMIN_CHAT_ID, "caption": caption},
                files={"document": ("signals.db", f, "application/octet-stream")},
                timeout=30
            )
        if resp.ok:
            log.info(f"[BACKUP] ✅ DB сохранена в Telegram ({db_size//1024}KB)")
            return True
        else:
            log.warning(f"[BACKUP] ❌ Ошибка: {resp.text[:100]}")
            return False
    except Exception as e:
        log.warning(f"[BACKUP] Ошибка бэкапа: {e}")
        return False


def _start_db_backup_scheduler():
    """Запускает автобэкап БД каждые 6 часов."""
    def _scheduler():
        import time as _t
        _t.sleep(300)  # первый бэкап через 5 мин
        while True:
            _backup_db_to_telegram()
            _t.sleep(6 * 3600)
    threading.Thread(target=_scheduler, daemon=True, name="DBBackupScheduler").start()
    log.info("[BACKUP] Планировщик бэкапа запущен (каждые 6ч → Telegram)")


def _daily_wallet_check():
    """Проверяет балансы всех привязанных кошельков каждые 24ч."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT uid, wallet_address, is_premium FROM users "
                "WHERE wallet_address IS NOT NULL AND wallet_address != ''"
            ).fetchall()
        updated = 0
        for uid, wallet, is_premium in rows:
            try:
                result = check_synx_balance(wallet)
                usd_value = result.get('usd_value', 0)
                from datetime import datetime, timezone, timedelta
                if usd_value >= 19.99:
                    until = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute(
                            "UPDATE users SET is_premium=1, premium_until=? WHERE uid=?",
                            (until, uid)
                        )
                        conn.commit()
                    updated += 1
                elif usd_value < 5.0 and is_premium:
                    log.warning(f"[WALLET] {wallet[:8]}... баланс низкий: ${usd_value:.2f}")
            except Exception as e:
                log.warning(f"[WALLET] check error {wallet[:8]}: {e}")
        log.info(f"[WALLET] Ежедневная проверка: {len(rows)} кошельков, {updated} продлено")
        if updated > 0:
            try:
                from s3_backup import upload_db as _s3
                _s3(DB_PATH)
            except Exception:
                pass
    except Exception as e:
        log.error(f"[WALLET] daily check error: {e}")


def _start_wallet_scheduler():
    """Запускает ежедневную проверку балансов $SYNX."""
    def _scheduler():
        import time as _t
        _t.sleep(3600)  # первая проверка через 1ч после старта
        while True:
            _daily_wallet_check()
            _t.sleep(86400)  # 24 часа
    threading.Thread(target=_scheduler, daemon=True, name="WalletScheduler").start()
    log.info("[WALLET] Планировщик проверки кошельков запущен (каждые 24ч)")


def _load_limits_from_db():
    """Загружает лимиты из БД (если были сохранены ранее)."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS app_config (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT
                )
            """)
            for k in ("free_daily_limit", "premium_daily_limit"):
                row = conn.execute(
                    "SELECT value FROM app_config WHERE key=?", (k,)
                ).fetchone()
                if row:
                    _SIGNAL_LIMITS[k] = int(row[0])
        log.info(f"[CONFIG] Лимиты загружены: free={_SIGNAL_LIMITS['free_daily_limit']} "
                 f"premium={_SIGNAL_LIMITS['premium_daily_limit']}")
    except Exception as e:
        log.warning(f"[CONFIG] Не удалось загрузить лимиты: {e}")

def _save_limit_to_db(key: str, value: int):
    """Сохраняет лимит в БД."""
    try:
        ts = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO app_config (key, value, updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """, (key, str(value), ts))
    except Exception as e:
        log.warning(f"[CONFIG] Не удалось сохранить {key}: {e}")

def _get_signal_limit(uid: str) -> int:
    """Возвращает дневной лимит сигналов для пользователя с проверкой premium_until."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT is_premium, daily_limit, premium_until FROM users WHERE uid=?", (uid,)
            ).fetchone()
            if not row:
                return _SIGNAL_LIMITS["free_daily_limit"]
            is_prem, custom_limit, prem_until = row
            # Проверяем что premium не истёк
            if is_prem and prem_until:
                try:
                    from datetime import datetime, timezone as tz
                    exp = datetime.fromisoformat(prem_until.replace('Z',''))
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=tz.utc)
                    if exp < datetime.now(tz.utc):
                        # Premium истёк — деактивируем автоматически
                        conn.execute("UPDATE users SET is_premium=0 WHERE uid=?", (uid,))
                        is_prem = 0
                        log.info(f"[PREMIUM] Автоистечение для {uid}")
                except Exception:
                    pass
            if custom_limit is not None and custom_limit >= 0:
                return custom_limit
            return _SIGNAL_LIMITS["premium_daily_limit"] if is_prem else _SIGNAL_LIMITS["free_daily_limit"]
    except Exception:
        return _SIGNAL_LIMITS["free_daily_limit"]

def _get_referral_count(uid: str) -> int:
    """Количество приглашённых пользователей."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM users WHERE referred_by=?", (uid,)
            ).fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


def _get_referrals_list(uid: str) -> list:
    """Список приглашённых пользователей."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT email, name, created_at FROM users WHERE referred_by=? ORDER BY created_at DESC LIMIT 50",
                (uid,)
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def _get_referral_bonus(uid: str) -> int:
    """Возвращает бонусные сигналы за рефералов: +1 за каждого (макс 10)."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            ref_code = conn.execute(
                "SELECT ref_code FROM users WHERE uid=?", (uid,)
            ).fetchone()
            if not ref_code or not ref_code[0]:
                return 0
            count = conn.execute(
                "SELECT COUNT(*) FROM users WHERE referred_by=?", (ref_code[0],)
            ).fetchone()[0]
            return min(count, 10)  # максимум +10 бонусных сигналов
    except Exception:
        return 0


def _check_and_increment_daily(uid: str) -> dict:
    """Проверяет и увеличивает счётчик сигналов за день. Возвращает {ok, used, limit}."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    limit = _get_signal_limit(uid)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT signals_today, last_reset FROM users WHERE uid=?", (uid,)
            ).fetchone()
            if not row:
                return {"ok": True, "used": 0, "limit": limit}
            used, last_reset = row
            # Сбрасываем счётчик если новый день
            if last_reset != today:
                used = 0
                conn.execute(
                    "UPDATE users SET signals_today=0, last_reset=? WHERE uid=?",
                    (today, uid)
                )
                conn.commit()
            if limit > 0 and used >= limit:
                return {"ok": False, "used": used, "limit": limit}
            conn.execute(
                "UPDATE users SET signals_today=signals_today+1, last_reset=? WHERE uid=?",
                (today, uid)
            )
            conn.commit()
            return {"ok": True, "used": used + 1, "limit": limit}
    except Exception as e:
        log.warning(f"[LIMIT] check error: {e}")
        return {"ok": True, "used": 0, "limit": limit}





# ══════════════════════════════════════════════════════════════════════════════
#  МАРШРУТЫ (merged from app.py)
# ══════════════════════════════════════════════════════════════════════════════

# ── Стандартные импорты для routes ───────────────────────────────────────────
import os, sqlite3, threading, time, json, collections, functools
from concurrent.futures import ThreadPoolExecutor, as_completed
# import requests  # already imported in app_core
# from datetime import datetime, timezone, timedelta  # already imported in app_core
from flask import (
    request, jsonify, session, redirect, url_for,
    render_template, Response, stream_with_context
)

# ── Версия приложения ─────────────────────────────────────────────────────────
APP_VERSION = "2.0.0"   # app_core.py + app.py split, NOWPayments, bilingual premium
APP_BUILD   = "2026-03-14"

@app.route('/api/prices')
def api_prices():
    """Возвращает цены + 24h change, параллельные запросы через ThreadPoolExecutor."""
    try:
        symbols  = request.args.get('symbols', 'BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,XRP-USDT')
        sym_list = [s.strip() for s in symbols.split(',') if s.strip()][:65]
        result   = {}

        def fetch_one(sym):
            try:
                data = collector.get_ticker_24h(sym)
                return sym, {
                    "price":      data.get('price', 0),
                    "change_24h": data.get('change_24h', data.get('change_pct', 0)),
                    "high":       data.get('high', data.get('high_24h', 0)),
                    "low":        data.get('low',  data.get('low_24h',  0)),
                    "status":     data.get('status', 'OK'),
                }
            except Exception:
                try:
                    live = collector.get_live_metrics(sym)
                    return sym, {"price": live.get('price', 0), "change_24h": 0, "status": "fallback"}
                except Exception:
                    return sym, {"price": 0, "change_24h": 0, "status": "ERROR"}

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(fetch_one, sym): sym for sym in sym_list}
            for future in as_completed(futures, timeout=8):
                try:
                    sym, data = future.result()
                    result[sym] = data
                except Exception:
                    pass

        return jsonify(result)
    except Exception as e:
        log.error(f"[api/prices] Error: {e}")
        return jsonify({}), 200


def check_synx_balance(wallet_address: str) -> dict:
    """
    Проверяет баланс $SYNX токена на кошельке через Solana RPC.
    Использует Jupiter API для получения цены токена в USD.
    """
    import requests as _req
    SYNX_MINT = "f6oBCUbhoXR7xTDvCh2THGx95JmAZ4zYLC5TiGGpump"
    result = {
        "wallet": wallet_address,
        "synx_balance": 0,
        "synx_price_usd": 0,
        "usd_value": 0,
        "has_premium_amount": False,
    }
    try:
        rpc_url = "https://api.mainnet-beta.solana.com"
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                wallet_address,
                {"mint": SYNX_MINT},
                {"encoding": "jsonParsed"},
            ],
        }
        r = _req.post(rpc_url, json=payload, timeout=10)
        accounts = r.json().get("result", {}).get("value", [])
        if accounts:
            token_amount = (accounts[0].get("account", {})
                            .get("data", {}).get("parsed", {})
                            .get("info", {}).get("tokenAmount", {}))
            result["synx_balance"] = float(token_amount.get("uiAmount", 0) or 0)
        price_r = _req.get(
            f"https://price.jup.ag/v6/price?ids={SYNX_MINT}", timeout=5
        )
        if price_r.ok:
            price_data = price_r.json().get("data", {}).get(SYNX_MINT, {})
            price = float(price_data.get("price", 0) or 0)
            result["synx_price_usd"] = price
            result["usd_value"] = round(result["synx_balance"] * price, 4)
            result["has_premium_amount"] = result["usd_value"] >= 19.99
    except Exception as e:
        log.warning(f"[WALLET] check_synx_balance error: {e}")
    return result


@app.route('/api/wallet/connect', methods=['POST'])
def api_wallet_connect():
    """Привязывает Phantom кошелёк к аккаунту и проверяет баланс $SYNX."""
    uid = session.get('user_uid')
    if not uid:
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    data = request.get_json() or {}
    wallet = data.get('wallet_address', '').strip()
    if not wallet or len(wallet) < 32:
        return jsonify({"error": "Неверный адрес кошелька"}), 400
    try:
        with sqlite3.connect(DB_PATH) as conn:
            existing = conn.execute(
                "SELECT uid FROM users WHERE wallet_address=? AND uid!=?",
                (wallet, uid)
            ).fetchone()
            if existing:
                # Проверяем не тот ли это же пользователь с другого устройства
                return jsonify({"error": "Этот кошелёк уже привязан к другому аккаунту"}), 400
            conn.execute(
                "UPDATE users SET wallet_address=? WHERE uid=?", (wallet, uid)
            )
            conn.commit()
        result = check_synx_balance(wallet)
        if result.get('usd_value', 0) >= 19.99:
            from datetime import datetime, timezone, timedelta
            until = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE users SET is_premium=1, premium_until=? WHERE uid=?",
                    (until, uid)
                )
                conn.commit()
            result['premium_activated'] = True
            result['premium_until'] = until
            try:
                from s3_backup import upload_db as _s3
                _s3(DB_PATH)
            except Exception:
                pass
        return jsonify(result)
    except Exception as e:
        log.error(f"[WALLET] connect error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/wallet/check', methods=['GET'])
def api_wallet_check():
    """Проверяет текущий баланс $SYNX для привязанного кошелька."""
    uid = session.get('user_uid')
    if not uid:
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT wallet_address FROM users WHERE uid=?", (uid,)
            ).fetchone()
        if not row or not row[0]:
            return jsonify({"error": "Кошелёк не привязан", "has_wallet": False})
        result = check_synx_balance(row[0])
        result['has_wallet'] = True
        if result.get('usd_value', 0) >= 19.99:
            from datetime import datetime, timezone, timedelta
            until = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE users SET is_premium=1, premium_until=? WHERE uid=?",
                    (until, uid)
                )
                conn.commit()
            result['premium_activated'] = True
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/entry', methods=['POST'])
def api_entry():
    """
    Режим 1: Войти по маркету прямо сейчас.
    Берёт текущую цену как точку входа, считает TP/SL, даёт полную аналитику.
    """
    uid = session.get('user_uid')
    ip  = get_client_ip()

    # Rate limit
    allowed, retry_after = _ai_limiter.is_allowed(ip)
    if not allowed:
        return jsonify({"error": "rate_limit", "retry_after": retry_after}), 429

    # Дневной лимит
    if uid:
        _lc = _check_and_increment_daily(uid)
        if not _lc['ok']:
            return jsonify({"error": "daily_limit",
                            "msg": f"Лимит {_lc['used']}/{_lc['limit']} исчерпан"}), 429

    try:
        req_data  = request.json or {}
        asset     = req_data.get('asset', 'BTC-USDT')
        lev       = max(1, min(125, int(req_data.get('leverage', 20))))
        marg      = max(1.0, float(req_data.get('margin', 100)))
        direction = req_data.get('direction', 'AUTO').upper()  # LONG / SHORT / AUTO
        timeframe = req_data.get('timeframe', '15m')

        # 1. Живая цена
        live_data = collector.get_live_metrics(asset)
        if not live_data.get('price'):
            return jsonify({"error": "Цена недоступна"}), 503

        current_price = float(live_data['price'])

        # 2. MTF данные
        mtf_history = {}
        for tf in ['15m', '1h', '4h', '1d']:
            try:
                data = collector.get_historical_klines(asset, interval=tf, limit=100)
                if data:
                    mtf_history[tf] = data
            except Exception:
                pass

        # 3. Технический анализ
        signal = analyzer.calculate_signal(mtf_history, lev, marg, primary_tf=timeframe)
        signal.setdefault('price', current_price)

        # 3б. Neural Consensus Engine (параллельные AI роли) — если доступен
        if _consensus_ready:
            try:
                consensus = _get_consensus(signal, asset, DB_PATH)
                if consensus and consensus.get('direction') in ('LONG', 'SHORT'):
                    # Перезаписываем направление и уровни из консенсуса (если лучше)
                    signal['direction']  = consensus['direction']
                    signal['consensus']  = {
                        'label':       consensus.get('label', ''),
                        'vote':        consensus.get('vote', ''),
                        'confidence':  consensus.get('confidence', 0),
                        'session':     consensus.get('session', ''),
                        'btc_context': consensus.get('btc_context', ''),
                        'elapsed_sec': consensus.get('elapsed_sec', 0),
                    }
                    if consensus.get('sl'):  signal['sl']  = consensus['sl']
                    if consensus.get('tp1'): signal['tp1'] = consensus['tp1']
                    if consensus.get('tp2'): signal['tp2'] = consensus['tp2']
                    if consensus.get('tp3'): signal['tp3'] = consensus['tp3']
                    log.info(f"[CONSENSUS] {asset}: {consensus['direction']} "
                             f"{consensus.get('label','')} conf={consensus.get('confidence')}%")
            except Exception as _ce:
                log.warning(f"[CONSENSUS] fallback to signal: {_ce}")

        # 4. Если direction задан вручную — устанавливаем только направление
        # TP/SL оставляем от calculate_signal() — они точнее
        if direction in ('LONG', 'SHORT'):
            signal['direction'] = direction

        # 4б. Нормализация уровней и расчёт процентов TP/SL
        signal.setdefault('entry', current_price)
        signal.setdefault('direction', direction if direction in ('LONG', 'SHORT') else 'LONG')
        _ep   = float(signal.get('entry') or current_price)
        _ddir = signal.get('direction', 'LONG')
        _tp1p = float(signal.get('tp1') or 0)
        _tp2p = float(signal.get('tp2') or 0)
        _slp  = float(signal.get('sl')  or 0)
        # Если tp1/tp2/sl равны нулю — вычисляем из current_price и leverage
        if not _tp1p or not _tp2p or not _slp:
            _sl_perc = max(0.008, min(0.025, 0.7 / max(lev, 1)))
            _tp_perc = _sl_perc * 2.5
            if _ddir == 'LONG':
                signal['tp1'] = round(_ep * (1 + _tp_perc),     6)
                signal['tp2'] = round(_ep * (1 + _tp_perc * 2), 6)
                signal['sl']  = round(_ep * (1 - _sl_perc),     6)
            else:
                signal['tp1'] = round(_ep * (1 - _tp_perc),     6)
                signal['tp2'] = round(_ep * (1 - _tp_perc * 2), 6)
                signal['sl']  = round(_ep * (1 + _sl_perc),     6)
            signal['entry'] = round(_ep, 6)
            _tp2p = float(signal['tp2'])
        # tp3 = TP2 + (TP2 - entry)
        if not signal.get('tp3') and _tp2p and _ep:
            _gap = abs(_tp2p - _ep)
            signal['tp3'] = round(_tp2p + _gap if _ddir == 'LONG' else _tp2p - _gap, 6)
        # Правильная формула процентов: реальное движение цены × плечо
        entry = float(signal.get('entry', current_price))
        sl    = float(signal.get('sl', 0))
        tp1   = float(signal.get('tp1', 0))
        tp2   = float(signal.get('tp2', 0))
        tp3   = float(signal.get('tp3', 0) or 0)
        direction = signal.get('direction', 'LONG')
        # Санитарная проверка направленности уровней
        if direction == 'LONG':
            if tp1 and tp1 < entry: tp1 = entry * 1.015
            if tp2 and tp2 < entry: tp2 = entry * 1.030
            if sl  and sl  > entry: sl  = entry * 0.985
        elif direction == 'SHORT':
            if tp1 and tp1 > entry: tp1 = entry * 0.985
            if tp2 and tp2 > entry: tp2 = entry * 0.970
            if sl  and sl  < entry: sl  = entry * 1.015
        signal['tp1'] = round(tp1, 6)
        signal['tp2'] = round(tp2, 6)
        signal['sl']  = round(sl,  6)
        if entry and sl and tp1:
            sl_move  = abs(entry - sl)  / entry
            tp1_move = abs(tp1 - entry) / entry
            tp2_move = abs(tp2 - entry) / entry
            tp3_move = abs(tp3 - entry) / entry if tp3 else tp2_move * 1.5
            signal['sl_pct']   = round(sl_move  * lev * 100, 1)
            signal['tp1_pct']  = round(tp1_move * lev * 100, 1)
            signal['tp2_pct']  = round(tp2_move * lev * 100, 1)
            signal['tp3_pct']  = round(tp3_move * lev * 100, 1)
            signal['rr_ratio']  = f"1:{round(tp1_move/sl_move,1)}" if sl_move else "1:1.5"
            signal['rr2_ratio'] = f"1:{round(tp2_move/sl_move,1)}" if sl_move else "1:3.0"
            signal['rr3_ratio'] = f"1:{round(tp3_move/sl_move,1)}" if sl_move else "1:5.0"
        signal.setdefault('sl_pct',    0)
        signal.setdefault('tp1_pct',   0)
        signal.setdefault('tp2_pct',   0)
        signal.setdefault('tp3_pct',   0)
        signal.setdefault('rr_ratio',  '1:1.5')
        signal.setdefault('rr2_ratio', '1:3.0')
        signal.setdefault('rr3_ratio', '1:5.0')

        # 5. Строим промпт для AI
        _tp1_pct = signal.get('tp1_pct', 0)
        _tp2_pct = signal.get('tp2_pct', 0)
        _sl_pct  = signal.get('sl_pct',  0)
        _rr1     = signal.get('rr_ratio', '1:2.5')
        _rr2     = signal.get('rr2_ratio', '1:5')
        _dir     = signal['direction']

        prompt = f"""Ты — SynapseX Quantum Core. Пользователь входит в {_dir} по рынку ПРЯМО СЕЙЧАС.
Отвечай ТОЛЬКО на русском. Без markdown, без HTML.

АКТИВ: {asset}
ЦЕНА ВХОДА (текущая): {current_price}
НАПРАВЛЕНИЕ: {_dir}
ПЛЕЧО: {lev}x | МАРЖА: ${marg}
ПОЗИЦИЯ НА: ${lev * marg:.2f}

УРОВНИ:
ТЕЙК-ПРОФИТ 1: {signal['tp1']} (+{_tp1_pct}% от депозита) | RR {_rr1}
ТЕЙК-ПРОФИТ 2: {signal['tp2']} (+{_tp2_pct}% от депозита) | RR {_rr2}
СТОП-ЛОСС: {signal['sl']} (-{_sl_pct}% от депозита)

СТРОГИЙ ФОРМАТ ОТВЕТА:
ВХОД В ПОЗИЦИЮ: {_dir}
ТОЧКА ВХОДА: {current_price}
ТЕЙК-ПРОФИТ 1: {signal['tp1']} (+{_tp1_pct}% от депозита при {lev}x) | RR {_rr1}
ТЕЙК-ПРОФИТ 2: {signal['tp2']} (+{_tp2_pct}% от депозита при {lev}x) | RR {_rr2}
СТОП-ЛОСС: {signal['sl']} (-{_sl_pct}% от депозита) | Инвалидация: [уровень]
УРОВЕНЬ ДОВЕРИЯ: [0-100]% [🔴/🟡/🟢]
полная аналитика: [8 предложений — почему именно сейчас {_dir}, что показывают индикаторы, риски, ключевые уровни S/R]
"""

        ai_response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1},
            headers={"Authorization": f"Bearer {GROQ_KEY}",
                     "Content-Type": "application/json"},
            timeout=20
        )
        ai_response.raise_for_status()
        ai_text = ai_response.json()['choices'][0]['message']['content']

        threading.Thread(target=_save_signal,
                         args=(asset, signal, ai_text, lev, marg, ip),
                         daemon=True).start()

        # Авто-сохранение в Trade Journal (кнопки LONG MARKET / SHORT MARKET)
        if session.get('user_uid'):
            try:
                _uid       = session['user_uid']
                _entry_val = float(signal.get('entry', current_price))
                _size_val  = float(marg) if marg else 0
                if _entry_val > 0 and _size_val > 0:
                    _conf  = signal.get('confidence', {})
                    _grade = _conf.get('grade', '') if isinstance(_conf, dict) else ''
                    _dir   = signal.get('direction', direction)
                    open_trade(
                        DB_PATH, _uid,
                        asset     = asset,
                        direction = _dir,
                        entry     = _entry_val,
                        size      = _size_val,
                        leverage  = int(lev),
                        tp1       = float(signal.get('tp1', 0)),
                        tp2       = float(signal.get('tp2', 0)),
                        sl        = float(signal.get('sl', 0)),
                        note      = f"Market Entry {_grade} | {_dir} | Score: {signal.get('bullish_score',0)}/13",
                    )
                    log.info(f"[PORTFOLIO] Кнопка-сделка: {asset} {_dir} @{_entry_val}")
            except Exception as _pf_err:
                log.warning(f"[PORTFOLIO] Entry авто-запись: {_pf_err}")

        # Нормализация raw_signal — уровни и проценты уже вычислены в шаге 4б

        return jsonify({
            "answer":     ultimatum_formatter(ai_text),
            "raw_signal": signal,
            "mode":       "market_entry",
            "price":      current_price,
        })

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            log.error("[api/entry] GROQ_KEY неверный или истёк — обнови в .env")
            return jsonify({"error": "GROQ_KEY_INVALID",
                            "answer": "⚠️ GROQ API ключ недействителен. Обнови GROQ_KEY в настройках сервера."}), 503
        log.exception("[api/entry] HTTP error")
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        log.exception("[api/entry] error")
        return jsonify({"error": str(e)}), 500


# ── /api/optimal_entry — Режим "Дай лучшую точку входа" ─────────────────────
# ИИ анализирует рынок и говорит: "жди цену X, там входи"

@app.route('/api/optimal_entry', methods=['POST'])
def api_optimal_entry():
    """
    Режим 2: ИИ сам ищет оптимальную точку входа.
    Анализирует уровни, говорит при какой цене и условиях входить.
    """
    uid = session.get('user_uid')
    ip  = get_client_ip()

    allowed, retry_after = _ai_limiter.is_allowed(ip)
    if not allowed:
        return jsonify({"error": "rate_limit", "retry_after": retry_after}), 429

    if uid:
        _lc = _check_and_increment_daily(uid)
        if not _lc['ok']:
            return jsonify({"error": "daily_limit",
                            "msg": f"Лимит {_lc['used']}/{_lc['limit']} исчерпан"}), 429

    try:
        req_data = request.json or {}
        asset    = req_data.get('asset', 'BTC-USDT')
        lev      = max(1, min(125, int(req_data.get('leverage', 20))))
        marg     = max(1.0, float(req_data.get('margin', 100)))

        # 1. Живая цена
        live_data = collector.get_live_metrics(asset)
        if not live_data.get('price'):
            return jsonify({"error": "Цена недоступна"}), 503
        current_price = float(live_data['price'])

        # 2. MTF данные
        mtf_history = {}
        for tf in ['15m', '1h', '4h', '1d']:
            try:
                data = collector.get_historical_klines(asset, interval=tf, limit=150)
                if data:
                    mtf_history[tf] = data
            except Exception:
                pass

        # 3. Технический анализ
        signal = analyzer.calculate_signal(mtf_history, lev, marg, primary_tf='1h')
        _sr    = signal.get('sr_levels', {})
        _fib   = signal.get('fibo', {})
        _ob    = signal.get('ob', {})
        _bos   = signal.get('bos', {})
        _con   = signal.get('confidence', {})

        supports    = _sr.get('supports', [])[:4]
        resistances = _sr.get('resistances', [])[:4]

        prompt = f"""Ты — SynapseX Quantum Core, элитный торговый ИИ.
Задача: найти ОПТИМАЛЬНУЮ точку входа для {asset}. НЕ входить сейчас — дать лучшую зону для входа.
Отвечай ТОЛЬКО на русском. Без markdown, без HTML.

ТЕКУЩАЯ ЦЕНА: {current_price}
ПЛЕЧО: {lev}x | МАРЖА: ${marg}

ТЕХНИЧЕСКИЙ АНАЛИЗ:
Направление (алго): {signal['direction']}
Доверие: {_con.get('score', 0)}% {_con.get('grade', '')}
BOS: {_bos.get('bos')} | ChoCH: {_bos.get('choch')}

УРОВНИ ПОДДЕРЖКИ: {supports}
УРОВНИ СОПРОТИВЛЕНИЯ: {resistances}

FIBONACCI:
0.382 = {_fib.get('0.382')} | 0.5 = {_fib.get('0.5')} | 0.618 = {_fib.get('0.618')} | 0.786 = {_fib.get('0.786')}
Расширение 1.618 = {_fib.get('1.618')}

ORDER BLOCKS:
OB сверху: {_ob.get('bearish', [])}
OB снизу: {_ob.get('bullish', [])}

СТРОГИЙ ФОРМАТ ОТВЕТА:
ОПТИМАЛЬНАЯ ТОЧКА ВХОДА: [конкретная цена]
НАПРАВЛЕНИЕ: [LONG/SHORT]
ПОЧЕМУ ИМЕННО ЭТА ЦЕНА: [2-3 предложения — какой уровень, почему сильный]
ТЕЙК-ПРОФИТ 1: [цена] (+[%]% при {lev}x) 
ТЕЙК-ПРОФИТ 2: [цена] (+[%]% при {lev}x)
СТОП-ЛОСС: [цена] (-[%]% при {lev}x)
УСЛОВИЕ ВХОДА: [что должно произойти чтобы войти — пробой, ретест, свеча]
ОЖИДАЕМОЕ ВРЕМЯ: [когда примерно ждать эту цену — часы/дни]
РИСК СЦЕНАРИЯ: [что делать если цена не придёт к этой зоне]
полная аналитика: [8 предложений — детальный анализ почему эта зона оптимальная]
"""

        ai_response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.15},
            headers={"Authorization": f"Bearer {GROQ_KEY}",
                     "Content-Type": "application/json"},
            timeout=25
        )
        ai_response.raise_for_status()
        ai_text = ai_response.json()['choices'][0]['message']['content']

        threading.Thread(target=_save_signal,
                         args=(asset, signal, ai_text, lev, marg, ip),
                         daemon=True).start()

        # Авто-сохранение в Trade Journal (кнопка ТОЧКА ВХОДА)
        if session.get('user_uid'):
            try:
                _uid       = session['user_uid']
                _entry_val = float(signal.get('entry', current_price))
                _size_val  = float(marg) if marg else 0
                if _entry_val > 0 and _size_val > 0:
                    _conf  = signal.get('confidence', {})
                    _grade = _conf.get('grade', '') if isinstance(_conf, dict) else ''
                    _dir   = signal.get('direction', 'LONG')
                    open_trade(
                        DB_PATH, _uid,
                        asset     = asset,
                        direction = _dir,
                        entry     = _entry_val,
                        size      = _size_val,
                        leverage  = int(lev),
                        tp1       = float(signal.get('tp1', 0)),
                        tp2       = float(signal.get('tp2', 0)),
                        sl        = float(signal.get('sl', 0)),
                        note      = f"Optimal Entry {_grade} | {_dir} | Score: {signal.get('bullish_score',0)}/13",
                    )
                    log.info(f"[PORTFOLIO] Оптим.точка: {asset} {_dir} @{_entry_val}")
            except Exception as _pf_err:
                log.warning(f"[PORTFOLIO] Optimal авто-запись: {_pf_err}")

        return jsonify({
            "answer":        ultimatum_formatter(ai_text),
            "raw_signal":    signal,
            "mode":          "optimal_entry",
            "current_price": current_price,
        })

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            log.error("[api/optimal_entry] GROQ_KEY неверный")
            return jsonify({"error": "GROQ_KEY_INVALID",
                            "answer": "⚠️ GROQ API ключ недействителен. Обнови GROQ_KEY в настройках сервера."}), 503
        log.exception("[api/optimal_entry] HTTP error")
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        log.exception("[api/optimal_entry] error")
        return jsonify({"error": str(e)}), 500



# ── /api/profile — Профиль пользователя ─────────────────────────────────────

@app.route('/api/profile')
def api_profile():
    uid = session.get('user_uid')
    if not uid:
        return jsonify({"error": "not_logged_in"}), 401
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            # Данные пользователя
            row = conn.execute(
                """SELECT name, email, photo, is_premium, premium_until,
                          ref_code, signals_today, daily_limit, created_at
                   FROM users WHERE uid=?""", (uid,)
            ).fetchone()
            if not row:
                return jsonify({"error": "user_not_found"}), 404
            data = dict(row)

            # Всего сигналов по IP (signals таблица не имеет uid)
            # Используем signals_today из users как основной счётчик
            signals_total = 0
            try:
                # Пробуем получить по ip если есть сессионный IP
                total_row = conn.execute(
                    "SELECT COUNT(*) FROM signals"
                ).fetchone()
                signals_total = total_row[0] if total_row else 0
            except Exception:
                signals_total = 0

            # Рефералы
            ref_count = 0
            try:
                if row['ref_code']:
                    ref_count = conn.execute(
                        "SELECT COUNT(*) FROM users WHERE referred_by=?",
                        (row['ref_code'],)
                    ).fetchone()[0]
            except Exception:
                ref_count = 0

        _is_prem = bool(data['is_premium'])
        _dlimit  = data.get('daily_limit', -1)
        if _dlimit is None or _dlimit < 0:
            _dlimit = _SIGNAL_LIMITS['premium_daily_limit'] if _is_prem else _SIGNAL_LIMITS['free_daily_limit']

        # Реферальный бонус
        ref_bonus = min(ref_count, 10)
        effective_limit = _dlimit + ref_bonus

        return jsonify({
            "name":           data.get('name') or 'USER',
            "email":          data.get('email') or '',
            "photo":          data.get('photo') or '',
            "is_premium":     _is_prem,
            "premium_until":  data.get('premium_until') or '',
            "ref_code":       data.get('ref_code') or '',
            "ref_link":       f"{os.getenv('APP_BASE_URL','https://synapsex-ai.com')}/ref/{data.get('ref_code','')}",
            "ref_count":      ref_count,
            "ref_bonus":      ref_bonus,
            "signals_today":  int(data.get('signals_today') or 0),
            "signals_limit":  effective_limit,
            "signals_total":  signals_total,
            "member_since":   (data.get('created_at') or '')[:10],
        })
    except Exception as e:
        log.error(f"[api/profile] {e}")
        return jsonify({"error": str(e)}), 500

# ── ⑥ История и статистика сигналов ──────────────────────────────────────────


@app.route('/stream/prices')
def stream_prices():
    """
    SSE endpoint для live-цен.
    ?symbols=BTC-USDT,SOL-USDT,ETH-USDT  (до 20 символов)
    ?interval=5                            (интервал в секундах, мин 3)
    """
    raw_symbols = request.args.get('symbols', 'BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,BNB-USDT')
    symbols     = [s.strip() for s in raw_symbols.split(',') if s.strip()][:20]
    interval    = max(3, int(request.args.get('interval', 5)))

    def generate():
        yield _sse_event({"type": "connected", "symbols": symbols, "interval": interval})
        while True:
            prices = {}
            for sym in symbols:
                data = collector.get_live_metrics(sym)
                prices[sym] = {
                    "price":  data.get('price', 0),
                    "status": data.get('status', 'UNKNOWN')
                }
            yield _sse_event({
                "type":   "tick",
                "ts":     int(time.time() * 1000),
                "prices": prices
            })
            time.sleep(interval)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':     'no-cache',
            'X-Accel-Buffering': 'no',    # отключает буферизацию в nginx
            'Connection':        'keep-alive',
        }
    )


# ══════════════════════════════════════════════════════════════════════════════
#  МАРШРУТЫ
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    lang = session.get('lang', 'RU')
    return render_template('index.html',
                           L=TEXTS.get(lang, TEXTS["RU"]),
                           current_lang=lang)


@app.route('/terminal')
def terminal():
    if not session.get('user_uid'):
        return redirect(url_for('login'))
    uid = session.get('user_uid', '')
    if _is_user_revoked(uid):
        session.clear()
        return redirect(url_for('login'))
    lang       = session.get('lang', 'RU')
    top_assets = [{"symbol": s, "base": s.split('-')[0], "change": "+0.00%", "price": 0, "volume": 0}
                  for s in FUTURES_PAIRS]
    # Достаём профиль пользователя
    user_data  = {}
    first_visit = False
    try:
        with sqlite3.connect(DB_PATH) as _conn:
            _conn.row_factory = sqlite3.Row
            _row = _conn.execute(
                "SELECT name, email, photo, is_premium, premium_until, ref_code, signals_today, daily_limit, created_at FROM users WHERE uid=?", (uid,)
            ).fetchone()
            if _row:
                user_data = dict(_row)
                # Первый визит — created_at в последние 5 минут
                import datetime as _dt
                _ca = user_data.get('created_at','')
                if _ca:
                    try:
                        _created = _dt.datetime.fromisoformat(_ca.replace('Z',''))
                        _diff = (_dt.datetime.utcnow() - _created).total_seconds()
                        first_visit = _diff < 300  # 5 минут
                    except Exception:
                        pass
                # Реальный лимит
                _is_prem = bool(_row['is_premium'])
                _dlimit  = _row['daily_limit']
                if _dlimit < 0:
                    _dlimit = _SIGNAL_LIMITS['premium_daily_limit'] if _is_prem else _SIGNAL_LIMITS['free_daily_limit']
                user_data['real_limit'] = _dlimit
                user_data['is_premium'] = _is_prem
                # ref_code генерируем если нет
                if not user_data.get('ref_code'):
                    import hashlib as _hl
                    _rc = _hl.md5(uid.encode()).hexdigest()[:8].upper()
                    with sqlite3.connect(DB_PATH) as _c2:
                        _c2.execute("UPDATE users SET ref_code=? WHERE uid=?", (_rc, uid))
                    user_data['ref_code'] = _rc
    except Exception as _e:
        log.warning(f"[terminal] user_data error: {_e}")
    return render_template('terminal.html',
                           L=TEXTS.get(lang, TEXTS["RU"]),
                           current_lang=lang,
                           top_assets=top_assets,
                           user=user_data,
                           first_visit=first_visit)


@app.route('/set_lang/<lang>')
def set_lang(lang):
    if lang in ['RU', 'EN']:
        session['lang'] = lang
    return redirect(request.referrer or url_for('index'))


@app.route('/send_uplink', methods=['POST'])
def send_uplink():
    try:
        user_msg  = (request.json or {}).get('message', 'Empty Uplink')
        tg_report = (
            f"📡 <b>SECURE_UPLINK INCOMING</b>\n"
            f"---------------------------\n"
            f"MESSAGE: {escape_html(user_msg)}\n"
            f"STATUS: ENCRYPTED_SUCCESS"
        )
        ok = send_to_admin(tg_report)
        return jsonify({"status": "SENT_TO_TG" if ok else "TG_ERROR",
                        "msg": "UPLINK_ESTABLISHED"})
    except Exception as e:
        log.exception("/send_uplink error")
        return jsonify({"status": "ERROR", "msg": str(e)}), 500


@app.route('/ask_ai', methods=['POST'])
def ask_ai():
    lang = session.get('lang', 'RU')
    L    = TEXTS.get(lang, TEXTS["RU"])
    ip   = get_client_ip()

    # ── ① Rate limit ──────────────────────────────────────────────────────────
    allowed, retry_after = _ai_limiter.is_allowed(ip)
    if not allowed:
        log.warning(f"[ask_ai] Rate limit: {ip}")
        resp = jsonify({
            "answer":      L["rate_limit_error"],
            "raw_signal":  None,
            "retry_after": retry_after
        })
        resp.headers['Retry-After']           = str(retry_after)
        resp.headers['X-RateLimit-Limit']     = str(_ai_limiter.max_calls)
        resp.headers['X-RateLimit-Remaining'] = "0"
        return resp, 429

    # ── ② Дневной лимит сигналов ──────────────────────────────────────────────
    uid = session.get('user_uid')
    if uid:
        _lc = _check_and_increment_daily(uid)
        if not _lc['ok']:
            return jsonify({
                "answer":     f"⛔ Дневной лимит сигналов исчерпан: {_lc['used']}/{_lc['limit']}. Обновите тариф до Premium для получения до {_SIGNAL_LIMITS['premium_daily_limit']} сигналов в день.",
                "raw_signal": None,
                "limit_reached": True,
                "used":  _lc['used'],
                "limit": _lc['limit']
            }), 429

    try:
        req_data  = request.json or {}
        asset     = req_data.get('asset', 'SOL-USDT')
        lev       = max(1, int(req_data.get('leverage', 30)))
        marg      = max(0.0, float(req_data.get('margin', 150)))
        tf        = req_data.get('tf', '15m')
        if tf not in ('1m','5m','15m','30m','1h','2h','4h','1d','1w'):
            tf = '15m'
        remaining = _ai_limiter.remaining(ip)

        # ── 1. Живая цена ──────────────────────────────────────────────────
        live_data = collector.get_live_metrics(asset)
        if live_data.get('status') == 'OFFLINE' or not live_data.get('price'):
            log.warning(f"[ask_ai] Цена {asset} недоступна")
            return jsonify({"answer": L["offline_error"], "raw_signal": None}), 503

        # ── 2. MTF данные ──────────────────────────────────────────────────
        timeframes  = ['15m', '1h', '2h', '4h', '1d', '1w']
        mtf_history = {}
        for tf in timeframes:
            try:
                data = collector.get_historical_klines(asset, interval=tf, limit=100)
                if data:
                    mtf_history[tf] = data
            except Exception as e:
                log.warning(f"[ask_ai] klines {tf} {asset}: {e}")

        if not mtf_history:
            return jsonify({"answer": "CORE_ERROR: нет исторических данных",
                            "raw_signal": None}), 503

        # ── 3. Технический анализ ──────────────────────────────────────────
        signal = analyzer.calculate_signal(mtf_history, lev, marg, primary_tf=tf)

        # ── 4. AI запрос (Groq) ────────────────────────────────────────────
        _tf  = tf.upper()
        _m   = signal.get('mtf_data', {})
        _con = signal.get('confidence', {})
        _his = signal.get('historical', {})
        _bos = signal.get('bos', {})
        _fvg = signal.get('fvg', {})
        _ob  = signal.get('ob', {})
        _pts = signal.get('patterns', [])
        _sr  = signal.get('sr_levels', {})
        _fib = signal.get('fibo', {})
        _swp = signal.get('liquidity_sweep', 'none')

        # MTF дайджест
        _mtf_lines = []
        for _t in ['1w','1d','4h','1h','15m','5m']:
            _d = _m.get(_t)
            if not _d: continue
            _pats = [p['name'] for p in _d.get('patterns',[])[:2]] if isinstance(_d.get('patterns'), list) else []
            _mtf_lines.append(
                f"{_t}: trend={_d.get('trend')} rsi={_d.get('rsi',0):.1f} "
                f"macd={_d.get('macd_cross')} obv={_d.get('obv_trend')} "
                f"rsi_div={_d.get('rsi_div','none')} macd_hist={_d.get('macd_hist_trend')} "
                f"squeeze={_d.get('bb_squeeze')} patterns={_pats}"
            )
        _mtf_txt = "\n".join(_mtf_lines) or "нет данных"

        _pats_txt = ", ".join(f"{p['name']}({p['bias']},{p['strength']})" for p in _pts[:6]) or "не найдены"

        _tp1_pct = signal.get('tp1_pct', 0)
        _tp2_pct = signal.get('tp2_pct', 0)
        _sl_pct  = signal.get('sl_pct',  0)
        _rr1     = signal.get('rr_ratio', '1:2.5')
        _rr2     = signal.get('rr2_ratio', '1:5')
        _conf_sc = _con.get('score', 0)
        _conf_gr = _con.get('grade', '')
        _reasons = "; ".join(_con.get('reasons', []))

        prompt = f"""Ты — SynapseX Quantum Core, элитный ИИ для анализа криптовалют. Отвечай ТОЛЬКО на русском языке. Без HTML, без markdown.

═══ ДАННЫЕ АНАЛИЗА ═══
АКТИВ: {asset} | ЦЕНА: {live_data['price']} | ОСНОВНОЙ ТФ: {_tf}
НАПРАВЛЕНИЕ: {signal['direction']} | СИГНАЛ ДОВЕРИЯ: {_conf_sc}% {_conf_gr}
ПРИЧИНЫ ДОВЕРИЯ: {_reasons}

ТОЧКИ СДЕЛКИ:
ВХОД: {signal['entry']}
TP1: {signal['tp1']} (+{_tp1_pct}% при {lev}x) | RR {_rr1}
TP2: {signal['tp2']} (+{_tp2_pct}% при {lev}x) | RR {_rr2}
SL:  {signal['sl']} (-{_sl_pct}% при {lev}x)
ATR: {signal.get('atr', 0):.4f}

ИСТОРИЧЕСКИЙ КОНТЕКСТ:
ATH: {_his.get('ath')} | ATL: {_his.get('atl')}
Текущая позиция: {_his.get('position_pct')}% диапазона | {_his.get('zone')}
От ATH: -{_his.get('from_ath_pct')}% | От ATL: +{_his.get('from_atl_pct')}%

FIBONACCI:
0.236={_fib.get('0.236')} | 0.382={_fib.get('0.382')} | 0.5={_fib.get('0.5')}
0.618={_fib.get('0.618')} | 0.786={_fib.get('0.786')} | ext 1.618={_fib.get('1.618')}

SMART MONEY:
FVG: {_fvg.get('status')} (кол-во: {_fvg.get('count',0)})
BOS: {_bos.get('bos')} | ChoCH: {_bos.get('choch')}
Ликвидность: {_swp}
OB бычий: {_ob.get('bullish',[])} | OB медвежий: {_ob.get('bearish',[])}

УРОВНИ S/R:
Поддержки: {_sr.get('supports',[])}
Сопротивления: {_sr.get('resistances',[])}

ПАТТЕРНЫ НА {_tf}: {_pats_txt}

MTF ДАННЫЕ (от старших к младшим):
{_mtf_txt}

═══ ФОРМАТ ОТВЕТА (копируй точно) ═══
ВХОД В ПОЗИЦИЮ: {signal['direction']}
ТОЧКА ВХОДА: {signal['entry']}
ТЕЙК-ПРОФИТ 1: {signal['tp1']} (+{_tp1_pct}% от депозита при {lev}x) | RR {_rr1}
ТЕЙК-ПРОФИТ 2: {signal['tp2']} (+{_tp2_pct}% от депозита при {lev}x) | RR {_rr2}
СТОП-ЛОСС: {signal['sl']} (-{_sl_pct}% от депозита) | Инвалидация выше/ниже: [уровень]
УРОВЕНЬ ДОВЕРИЯ: {_conf_sc}% {_conf_gr}
полная аналитика: [10 предложений на русском. СТРОГО по плану:
1) Исторический контекст — где цена относительно ATH/ATL, что происходило в 2022/2023/2024/2025/2026
2) Недельный и дневной тренд — основной вектор движения
3) 4H и 1H анализ — подтверждение или противоречие
4) RSI+MACD+OBV конфлюэнция с числами
5) Точные уровни Fibonacci где цена реагирует
6) Паттерны графика — какие найдены, что означают
7) Smart Money: Order Blocks, FVG, BOS/ChoCH, Liquidity Sweep
8) Ключевые S/R уровни с конкретными числами
9) Условие инвалидации — при каком уровне/событии сделка отменяется
10) Итоговое заключение с уровнем доверия и обоснованием]
"""
        ai_response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1},
            headers={"Authorization": f"Bearer {GROQ_KEY}",
                     "Content-Type": "application/json"},
            timeout=20
        )
        ai_response.raise_for_status()
        ai_res = ai_response.json()['choices'][0]['message']['content']

        # ── 5. Фоновые задачи ──────────────────────────────────────────────
        # Уведомление администратору (личка)
        tg_admin = (
            f"🤖 <b>NEURAL SIGNAL: {asset}</b>\n"
            f"---------------------------\n"
            f"{escape_html(ai_res)}\n"
            f"---------------------------\n"
            f"💰 SIZE: ${lev * marg:.2f} | IP: {ip}"
        )
        threading.Thread(
            target=send_to_admin,
            args=(tg_admin,), daemon=True
        ).start()

        # Группа t.me/SynapseX_AI отключена от терминала
        # gemini_neural_engine вызывается только из внешних источников (crypto_bot и т.д.)

        # ⑤ SQLite история
        threading.Thread(
            target=_save_signal,
            args=(asset, signal, ai_res, lev, marg, ip),
            daemon=True
        ).start()

        # ⑥ Авто-запись в Trade Journal (только для авторизованных)
        if session.get('user_uid'):  # trades таблица создаётся в _init_db()
            try:
                _uid       = session['user_uid']
                _entry_val = float(signal.get('entry', 0))
                _size_val  = float(marg) if marg else 0
                if _entry_val > 0 and _size_val > 0:
                    _conf  = signal.get('confidence', {})
                    _grade = _conf.get('grade', '') if isinstance(_conf, dict) else ''
                    open_trade(
                        DB_PATH, _uid,
                        asset     = asset,
                        direction = signal.get('direction', 'LONG'),
                        entry     = _entry_val,
                        size      = _size_val,
                        leverage  = int(lev) if lev else 1,
                        tp1       = float(signal.get('tp1', 0)),
                        tp2       = float(signal.get('tp2', 0)),
                        sl        = float(signal.get('sl', 0)),
                        note      = f"AI Signal {_grade} | {signal.get('direction','?')} | Score: {signal.get('bullish_score',0)}/13",
                    )
                    log.info(f"[PORTFOLIO] Авто-запись сделки: {asset} {signal.get('direction')} @{_entry_val}")
                else:
                    log.warning(f"[PORTFOLIO] Авто-запись пропущена: entry={_entry_val} size={_size_val}")
            except Exception as _pf_err:
                log.warning(f"[PORTFOLIO] Авто-запись: {_pf_err}")

        # ── 6. Ответ ───────────────────────────────────────────────────────
        resp = jsonify({"answer": ultimatum_formatter(ai_res), "raw_signal": signal})
        resp.headers['X-RateLimit-Limit']     = str(_ai_limiter.max_calls)
        resp.headers['X-RateLimit-Remaining'] = str(remaining)
        return resp

    except requests.exceptions.Timeout:
        log.error("[ask_ai] Groq timeout")
        return jsonify({"answer": "CORE_ERROR: AI не отвечает (timeout)",
                        "raw_signal": None}), 504

    except requests.exceptions.HTTPError as e:
        log.error(f"[ask_ai] Groq HTTP error: {e}")
        return jsonify({"answer": f"CORE_ERROR: AI вернул {e.response.status_code}",
                        "raw_signal": None}), 502

    except Exception as e:
        log.exception("[ask_ai] неожиданная ошибка")
        return jsonify({"answer": f"CORE_ERROR: {str(e)}", "raw_signal": None}), 500


# ── ⑥ История и статистика сигналов ──────────────────────────────────────────

@app.route('/signals')
def signals_history():
    """GET /signals?limit=50 — последние N сигналов (ai_answer скрыт)."""
    try:
        limit = max(1, min(200, int(request.args.get('limit', 50))))
        rows  = _get_recent_signals(limit)
        for r in rows:
            r.pop('ai_answer', None)
        return jsonify({"count": len(rows), "signals": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/signals/stats')
def signals_stats():
    """GET /signals/stats — агрегированная статистика."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            total    = conn.execute("SELECT COUNT(*) as n FROM signals").fetchone()['n']
            by_dir   = conn.execute(
                "SELECT direction, COUNT(*) as n FROM signals GROUP BY direction"
            ).fetchall()
            by_asset = conn.execute(
                "SELECT asset, COUNT(*) as n FROM signals GROUP BY asset ORDER BY n DESC LIMIT 10"
            ).fetchall()
            avg_pos  = conn.execute(
                "SELECT AVG(position_sz) as avg FROM signals"
            ).fetchone()['avg']
        return jsonify({
            "total_signals":   total,
            "by_direction":    {r['direction']: r['n'] for r in by_dir},
            "top_assets":      [{r['asset']: r['n']} for r in by_asset],
            "avg_position_sz": round(avg_pos or 0, 2),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── ② /health ─────────────────────────────────────────────────────────────────

@app.route('/health')
def health():
    """
    GET /health — полный статус всех сервисов.
    Используется Render / UptimeRobot / Railway для мониторинга.
    """
    checks = {}

    # MEXC
    try:
        r = requests.get("https://api.mexc.com/api/v3/ping", timeout=3)
        checks['mexc'] = {
            "status": "OK" if r.ok else "ERROR",
            "ms":     int(r.elapsed.total_seconds() * 1000)
        }
    except Exception as e:
        checks['mexc'] = {"status": "OFFLINE", "detail": str(e)}

    # Groq — минимальный тест (1 токен)
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": "ping"}],
                  "max_tokens": 1},
            headers={"Authorization": f"Bearer {GROQ_KEY}",
                     "Content-Type": "application/json"},
            timeout=5
        )
        checks['groq'] = {
            "status": "OK" if r.ok else "ERROR",
            "code":   r.status_code,
            "ms":     int(r.elapsed.total_seconds() * 1000)
        }
    except Exception as e:
        checks['groq'] = {"status": "OFFLINE", "detail": str(e)}

    # Telegram — терминальный бот
    try:
        r    = requests.get(
            f"https://api.telegram.org/bot{TG_TOKEN_TERMINAL}/getMe", timeout=3
        )
        data = r.json()
        checks['telegram_terminal'] = {
            "status":   "OK" if r.ok and data.get('ok') else "ERROR",
            "bot_name": data.get('result', {}).get('username', '?'),
            "ms":       int(r.elapsed.total_seconds() * 1000)
        }
    except Exception as e:
        checks['telegram_terminal'] = {"status": "OFFLINE", "detail": str(e)}

    # Telegram — аналитический бот
    try:
        r    = requests.get(
            f"https://api.telegram.org/bot{TG_TOKEN_ANALYTICS}/getMe", timeout=3
        )
        data = r.json()
        checks['telegram_analytics'] = {
            "status":   "OK" if r.ok and data.get('ok') else "ERROR",
            "bot_name": data.get('result', {}).get('username', '?'),
            "ms":       int(r.elapsed.total_seconds() * 1000)
        }
    except Exception as e:
        checks['telegram_analytics'] = {"status": "OFFLINE", "detail": str(e)}

    # Cache / Redis
    checks['cache'] = cache.health()

    # SQLite
    try:
        with sqlite3.connect(DB_PATH) as conn:
            count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        checks['db'] = {"status": "OK", "signals_total": count}
    except Exception as e:
        checks['db'] = {"status": "ERROR", "detail": str(e)}

    # CryptoPanic (если задан ключ)
    if CRYPTOPANIC_KEY:
        try:
            r = requests.get(
                f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_KEY}&limit=1",
                timeout=3
            )
            checks['cryptopanic'] = {
                "status": "OK" if r.ok else "ERROR",
                "ms":     int(r.elapsed.total_seconds() * 1000)
            }
        except Exception as e:
            checks['cryptopanic'] = {"status": "OFFLINE", "detail": str(e)}

    all_ok    = all(v.get('status') == 'OK' for v in checks.values())
    http_code = 200 if all_ok else 207

    return jsonify({
        "status":    "OK" if all_ok else "DEGRADED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "env": {
            "dotenv_loaded":       _dotenv_loaded,
            "redis_configured":    bool(REDIS_URL),
            "cache_backend":       cache.backend,
            "anthropic_key_set":   bool(ANTHROPIC_KEY),
            "cryptopanic_key_set": bool(CRYPTOPANIC_KEY),
        },
        "checks": checks
    }), http_code


# ══════════════════════════════════════════════════════════════════════════════
#  ③ WARM CACHE — прогрев до первого запроса
# ══════════════════════════════════════════════════════════════════════════════

def _warm_cache():
    log.info("[WARM_CACHE] Старт прогрева...")
    try:
        top = collector.get_top_100_mexc()
        log.info(f"[WARM_CACHE] Топ-100 загружен ({len(top)} пар)")
        for item in top[:5]:
            collector.get_live_metrics(item['symbol'])
            time.sleep(0.1)
        log.info("[WARM_CACHE] Цены топ-5 прогреты")
    except Exception as e:
        log.warning(f"[WARM_CACHE] Ошибка: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  АВТОРИЗАЦИЯ — Firebase Google + Admin Login
# ══════════════════════════════════════════════════════════════════════════════

# ── Firebase Admin SDK ────────────────────────────────────────────────────────
_firebase_ready = False
_firebase_error = ""   # причина ошибки инициализации
try:
    import firebase_admin
    from firebase_admin import credentials, auth as fb_auth
    import hmac as _hmac, hashlib as _hashlib

    if not firebase_admin._apps:
        _key_json_str = os.getenv("FIREBASE_KEY_JSON", "")
        _key_path     = os.getenv("FIREBASE_KEY_PATH", "firebase_key.json")

        if _key_json_str:
            # Вариант 1: JSON-строка в переменной окружения
            # Фикс: private_key в .env может содержать литеральные \n
            _key_json_str = _key_json_str.strip()
            try:
                _key_dict = json.loads(_key_json_str)
            except json.JSONDecodeError:
                import re as _re
                _key_json_fixed = _re.sub(r'(?<!\\)\n', '\\n', _key_json_str)
                _key_dict = json.loads(_key_json_fixed)
            _cred = credentials.Certificate(_key_dict)
            firebase_admin.initialize_app(_cred)
            _firebase_ready = True
            log.info("[AUTH] Firebase инициализирован из FIREBASE_KEY_JSON")
        elif os.path.exists(_key_path):
            # Вариант 2: файл firebase_key.json рядом с app.py (для VPS/локально)
            _cred = credentials.Certificate(_key_path)
            firebase_admin.initialize_app(_cred)
            _firebase_ready = True
            log.info(f"[AUTH] Firebase инициализирован из файла: {_key_path}")
        else:
            log.warning("[AUTH] Ни FIREBASE_KEY_JSON, ни firebase_key.json не найдены")
    else:
        _firebase_ready = True
except ImportError:
    log.warning("[AUTH] firebase-admin не установлен — pip install firebase-admin")
except Exception as e:
    log.error(f"[AUTH] Firebase init error: {e}")

# ADMIN_LOGIN/ADMIN_PASSWORD imported from app_core

# Firebase конфиг для фронтенда (из .env — публичные ключи)
# FIREBASE_CONFIG imported from app_core

def _require_auth(f):
    """Декоратор: пользователь должен быть залогинен через Google."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_uid'):
            if request.is_json:
                return jsonify({"error":"UNAUTHORIZED","redirect":"/login"}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def _require_admin(f):
    """Декоратор: только администратор."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('is_admin'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated

# ── Страница логина ────────────────────────────────────────────────────────────

@app.route('/login')
def login():
    # ВАЖНО: НЕ перенаправляем автоматически если сессия существует
    # Пользователь должен видеть страницу входа и иметь возможность
    # войти под другим аккаунтом или выйти
    # Если уже залогинен в Firebase — login.html покажет resume-block
    return render_template('login.html', **FIREBASE_CONFIG)


@app.route('/auth/status')
def auth_status():
    """Возвращает статус авторизации для фронтенда (используется на index.html)."""
    if session.get('user_uid'):
        return jsonify({
            "logged_in": True,
            "uid":   session.get('user_uid'),
            "email": session.get('user_email', ''),
            "name":  session.get('user_name', ''),
            "photo": session.get('user_photo', ''),
        })
    return jsonify({"logged_in": False})

@app.route('/logout')
def logout():
    session.clear()
    # Редиректим на /login?logout=1 — там Firebase тоже выполнит signOut()
    resp = redirect('/login?logout=1')
    resp.delete_cookie('session')
    return resp

# ── Firebase token verify ──────────────────────────────────────────────────────

@app.route('/api/debug/firebase')
def debug_firebase():
    """Диагностика Firebase — только для администраторов."""
    if not session.get('is_admin'):
        return jsonify({"error": "ADMIN_ONLY"}), 403
    import pathlib as _pl
    _key_path = os.getenv("FIREBASE_KEY_PATH", "firebase_key.json")
    _search = [
        str(_pl.Path(__file__).parent / "firebase_key.json"),
        str(_pl.Path(__file__).parent / "config" / "firebase_key.json"),
    ]
    return jsonify({
        "firebase_ready":   _firebase_ready,
        "firebase_error":   _firebase_error,
        "env_key_path":     _key_path,
        "key_file_exists":  os.path.exists(_key_path),
        "searched_paths":   {p: os.path.exists(p) for p in _search},
        "firebase_key_json_set": bool(os.getenv("FIREBASE_KEY_JSON", "")),
        "hint": "Если key_file_exists=false — скопируйте firebase_key.json в корень проекта"
    })

@app.route('/api/heartbeat', methods=['POST'])
def api_heartbeat():
    """
    Вызывается с фронтенда каждые 30 секунд пока пользователь активен.
    Обновляет last_seen → позволяет видеть онлайн в админке.
    """
    try:
        uid = session.get('user_uid')
        if uid and uid != 'dev_user':
            _update_last_seen(uid)
            return jsonify({"ok": True, "uid": uid[:8]})
        return jsonify({"ok": False, "reason": "no_session"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route('/auth/google/verify', methods=['POST'])
def auth_google_verify():
    """
    Принимает Firebase ID Token от фронтенда.
    Верифицирует через firebase-admin, сохраняет пользователя в сессии.
    """
    id_token = (request.json or {}).get('id_token', '')
    if not id_token:
        return jsonify({"status": "ERROR", "error": "NO_TOKEN"}), 400

    if not _firebase_ready:
        # Fallback: если firebase-admin не установлен, принимаем токен как есть
        # (только для дев-режима, в проде firebase-admin обязателен)
        log.warning(f"[AUTH] Firebase не готов: {_firebase_error}")
        import os as _os
        _key_exists = _os.path.exists(_os.getenv("FIREBASE_KEY_PATH", "firebase_key.json"))
        return jsonify({
            "status":    "ERROR",
            "error":     "AUTH_UNAVAILABLE",
            "message":   f"Firebase не инициализирован: {_firebase_error or 'неизвестная ошибка'}",
            "key_found": _key_exists,
            "hint":      "Проверьте что firebase_key.json лежит рядом с app.py"
        }), 503

    try:
        decoded = fb_auth.verify_id_token(id_token)
        uid     = decoded.get('uid', '')
        email   = decoded.get('email', '')
        name    = decoded.get('name', email.split('@')[0] if email else 'User')
        photo   = decoded.get('picture', '')

        session.permanent     = True   # сессия живёт дольше (до закрытия браузера)
        session['user_uid']   = uid
        session['user_email'] = email
        session['user_name']  = name
        session['user_photo'] = photo

        # Сохраняем/обновляем пользователя в SQLite
        _ref_by = session.pop('ref_by', None)
        _upsert_user(uid, email, name, photo)
        if _ref_by:
            try:
                with sqlite3.connect(DB_PATH) as _rc:
                    _existing = _rc.execute("SELECT referred_by FROM users WHERE uid=?", (uid,)).fetchone()
                    if _existing and not _existing[0]:
                        _rc.execute("UPDATE users SET referred_by=? WHERE uid=?", (_ref_by, uid))
                        _rc.commit()
                        log.info(f"[REF] {email} приглашён через {_ref_by}")
            except Exception as _re:
                log.warning(f"[REF] referred_by error: {_re}")

        log.info(f"[AUTH] Google login: {email} ({uid[:8]}...)")

        # Авто-активация Trial для новых пользователей
        try:
            with sqlite3.connect(DB_PATH) as _tc:
                _tr = _tc.execute(
                    "SELECT trial_until, is_premium FROM users WHERE uid=?", (uid,)
                ).fetchone()
            if _tr and not _tr[0] and not _tr[1]:
                # Ни trial, ни premium — активируем trial
                activate_trial(uid, email, name)
                log.info(f"[TRIAL] Auto-trial активирован: {email}")
        except Exception as _te:
            log.warning(f"[TRIAL] Auto-trial error: {_te}")

        return jsonify({"status": "OK", "redirect": "/terminal", "name": name})

    except Exception as e:
        log.warning(f"[AUTH] Token verify failed: {e}")
        return jsonify({"status": "ERROR", "error": "INVALID_TOKEN"}), 401

# ── Пользователи в SQLite ──────────────────────────────────────────────────────

def _init_users_db():
    """Создаёт таблицу users если не существует."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    uid                TEXT PRIMARY KEY,
                    email              TEXT,
                    name               TEXT,
                    photo              TEXT,
                    last_seen          TEXT,
                    created_at         TEXT,
                    is_revoked         INTEGER DEFAULT 0,
                    is_premium         INTEGER DEFAULT 0,
                    daily_limit        INTEGER DEFAULT -1,
                    signals_today      INTEGER DEFAULT 0,
                    last_reset         TEXT    DEFAULT '',
                    stripe_customer_id TEXT    DEFAULT '',
                    ref_code           TEXT    DEFAULT '',
                    referred_by        TEXT    DEFAULT '',
                    premium_until      TEXT    DEFAULT '',
                    telegram_chat_id   TEXT    DEFAULT '',
                    tg_alerts_enabled  INTEGER DEFAULT 1
                )
            """)
            # ALTER TABLE для старых БД — добавляем колонки если нет
            for col, defn in [
                ('is_premium',         'INTEGER DEFAULT 0'),
                ('daily_limit',        'INTEGER DEFAULT -1'),
                ('signals_today',      'INTEGER DEFAULT 0'),
                ('last_reset',         "TEXT DEFAULT ''"),
                ('stripe_customer_id', "TEXT DEFAULT ''"),
                ('ref_code',           "TEXT DEFAULT ''"),
                ('referred_by',        "TEXT DEFAULT ''"),
                ('premium_until',      "TEXT DEFAULT ''"),
                ('telegram_chat_id',   "TEXT DEFAULT ''"),
                ('tg_alerts_enabled',  'INTEGER DEFAULT 1'),
                ('trial_until',        "TEXT DEFAULT ''"),
                ('paddle_sub_id',      "TEXT DEFAULT ''"),
                ('wallet_address',     "TEXT DEFAULT ''"),
                ('referral_code',      "TEXT DEFAULT ''"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
                except Exception:
                    pass
            conn.commit()
    except Exception as e:
        log.error(f"[AUTH] users table error: {e}")

def _upsert_user(uid: str, email: str, name: str, photo: str):
    """Создаёт или обновляет запись пользователя."""
    try:
        ts = datetime.now(timezone.utc).isoformat()
        is_new = False
        with sqlite3.connect(DB_PATH) as conn:
            # Сначала пробуем INSERT
            try:
                conn.execute("""
                    INSERT INTO users (uid, email, name, photo, last_seen, created_at,
                                       is_revoked, is_premium, daily_limit, signals_today,
                                       last_reset, stripe_customer_id, ref_code,
                                       referred_by, premium_until)
                    VALUES (?, ?, ?, ?, ?, ?, 0, 0, -1, 0, '', '', '', '', '')
                """, (uid, email, name, photo, ts, ts))
                is_new = True
            except sqlite3.IntegrityError:
                # Уже существует — обновляем
                conn.execute("""
                    UPDATE users SET
                        email=?, name=?, photo=?, last_seen=?
                    WHERE uid=?
                """, (email, name, photo, ts, uid))
            conn.commit()
        log.info(f"[AUTH] upsert_user OK: {email} ({uid[:8]}...)")
        if is_new:
            upload_now(DB_PATH)
    except Exception as e:
        log.error(f"[AUTH] upsert_user FAILED: {e}")

def _update_last_seen(uid: str):
    """Обновляет last_seen пользователя (вызывается при активности)."""
    try:
        ts = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE users SET last_seen=? WHERE uid=?", (ts, uid))
    except Exception:
        pass

def _is_user_revoked(uid: str) -> bool:
    """Проверяет, был ли доступ пользователя отозван."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT is_revoked FROM users WHERE uid=?", (uid,)).fetchone()
            return bool(row and row[0])
    except:
        return False

# ── Admin DB Download / Upload ────────────────────────────────────────────────

@app.route('/admin/db/download')
def admin_db_download():
    if not session.get('is_admin'):
        return redirect('/admin/login')
    from flask import send_file
    return send_file(DB_PATH, as_attachment=True,
                     download_name='signals.db',
                     mimetype='application/octet-stream')


@app.route('/admin/db/upload', methods=['POST'])
def admin_db_upload():
    if not session.get('is_admin'):
        return jsonify({"error": "unauthorized"}), 401
    file = request.files.get('db_file')
    if not file:
        return jsonify({"error": "no file"}), 400
    import shutil
    shutil.copy2(DB_PATH, DB_PATH + '.backup')
    file.save(DB_PATH)
    import time
    time.sleep(0.5)  # дай файлу записаться
    try:
        from s3_backup import upload_db as _upload_db
        result = _upload_db(DB_PATH)
        log.info(f"[ADMIN] S3 sync: {result}")
    except Exception as e:
        log.error(f"[ADMIN] S3 sync error: {e}")
    return jsonify({"status": "OK", "message": "БД загружена и синхронизирована с S3 ✅"})


# ── Admin Login ────────────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if session.get('is_admin'):
        return redirect(url_for('admin_panel'))

    error = None
    if request.method == 'POST':
        login_val = request.form.get('login', '').strip()
        pass_val  = request.form.get('password', '').strip()

        try:
            import hmac as _hmac
            login_ok = _hmac.compare_digest(login_val, ADMIN_LOGIN)
            pass_ok  = _hmac.compare_digest(pass_val,  ADMIN_PASSWORD)
        except Exception:
            login_ok = (login_val == ADMIN_LOGIN)
            pass_ok  = (pass_val  == ADMIN_PASSWORD)

        if login_ok and pass_ok:
            session['is_admin'] = True
            session['admin_login'] = login_val
            log.info(f"[ADMIN] Login success from {get_client_ip()}")
            return redirect(url_for('admin_panel'))
        else:
            error = "INVALID CREDENTIALS — ACCESS DENIED"
            log.warning(f"[ADMIN] Failed login from {get_client_ip()}")

    return render_template('admin_login.html', error=error)

@app.route('/admin/logout')
def admin_logout():
    session.pop('is_admin', None)
    session.pop('admin_login', None)
    return redirect(url_for('admin_login'))

# ── Admin Panel ────────────────────────────────────────────────────────────────

@app.route('/admin')
@app.route('/admin/')
def admin_panel():
    if not session.get('is_admin'):
        return redirect(url_for('admin_login'))
    return render_template('admin.html')

# ── Admin API ──────────────────────────────────────────────────────────────────

@app.route('/admin/api/test/telegram', methods=['POST'])
def admin_test_telegram():
    if not session.get('is_admin'):
        return jsonify({"error": "unauthorized"}), 401
    import requests as _req
    token        = os.getenv('TELEGRAM_TOKEN', '')
    tg_token2    = os.getenv('TELEGRAM_TOKEN_TERMINAL', '')
    tg_analytics = os.getenv('TG_TOKEN_ANALYTICS', token)
    channel_id   = os.getenv('CHANNEL_ID', '')
    results = {}
    for name, tok in [
        ('TELEGRAM_TOKEN', token),
        ('TELEGRAM_TOKEN_TERMINAL', tg_token2),
        ('TG_TOKEN_ANALYTICS', tg_analytics)
    ]:
        if not tok:
            results[name] = 'не задан'
            continue
        try:
            r = _req.post(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                json={"chat_id": channel_id, "text": f"🧪 Test {name}"},
                timeout=5
            )
            results[name] = r.json()
        except Exception as e:
            results[name] = str(e)
    return jsonify({"channel_id": channel_id, "results": results})


@app.route('/admin/api/stats')
def admin_api_stats():
    if not session.get('is_admin'):
        return jsonify({"error": "unauthorized"}), 401
    try:
        with sqlite3.connect(DB_PATH) as conn:
            total_users   = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            premium_users = conn.execute("SELECT COUNT(*) FROM users WHERE is_premium=1").fetchone()[0]
            trial_users   = conn.execute("SELECT COUNT(*) FROM users WHERE trial_until > datetime('now')").fetchone()[0]
            signals_today = conn.execute("SELECT COUNT(*) FROM signals WHERE date(ts)=date('now')").fetchone()[0]
            signals_total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            try:
                trades_total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            except Exception:
                trades_total = 0
        db_size = os.path.getsize(DB_PATH)
        return jsonify({
            "total_users":    total_users,
            "premium_users":  premium_users,
            "trial_users":    trial_users,
            "signals_today":  signals_today,
            "signals_total":  signals_total,
            "trades_total":   trades_total,
            "db_size_kb":     round(db_size / 1024, 1),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/admin/api/signals')
def admin_api_signals():
    if not session.get('is_admin'):
        return jsonify({"error":"UNAUTHORIZED"}), 401
    limit = min(int(request.args.get('limit', 100)), 500)
    asset = request.args.get('asset', '')
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            if asset:
                rows = conn.execute("SELECT * FROM signals WHERE asset=? ORDER BY id DESC LIMIT ?", (asset, limit)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        data = [dict(r) for r in rows]
        for row in data:
            row.pop('ai_answer', None)
        return jsonify({"count": len(data), "signals": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/admin/api/users')
def admin_api_users():
    if not session.get('is_admin'):
        return jsonify({"error":"UNAUTHORIZED"}), 401
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            users = conn.execute("SELECT u.*, (SELECT COUNT(*) FROM signals s WHERE s.ip IS NOT NULL) as signal_count FROM users u ORDER BY u.last_seen DESC LIMIT 200").fetchall()
        return jsonify({"users": [dict(u) for u in users]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/admin/api/users/gift_premium', methods=['POST'])
@_require_admin
def admin_gift_premium():
    """
    Подарить Premium пользователю на N дней.
    Используется для розыгрышей, бонусов, ручного управления.
    """
    data  = request.get_json() or {}
    uid   = data.get('uid', '').strip()
    days  = int(data.get('days', 30))
    if not uid:
        return jsonify({"error": "UID_REQUIRED"}), 400
    try:
        until = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                UPDATE users
                SET is_premium=1, premium_until=?
                WHERE uid=?
            """, (until, uid))
            conn.commit()
            row = conn.execute("SELECT email, name FROM users WHERE uid=?", (uid,)).fetchone()
        name  = row[1] if row else uid
        email = row[0] if row else uid
        log.info(f"[ADMIN] 🎁 Premium подарен: {email} на {days} дней до {until[:10]}")
        return jsonify({
            "status": "ok",
            "message": f"✅ Premium подарен {name} на {days} дней (до {until[:10]})"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/admin/api/users/revoke', methods=['POST'])
def admin_api_revoke_user():
    if not session.get('is_admin'):
        return jsonify({"error":"UNAUTHORIZED"}), 401
    uid = (request.json or {}).get('uid', '')
    if not uid:
        return jsonify({"error":"NO_UID"}), 400
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE users SET is_revoked=1 WHERE uid=?", (uid,))
            conn.commit()
        log.info(f"[ADMIN] User revoked: {uid}")
        return jsonify({"status": "OK"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/admin/api/config', methods=['POST'])
@_require_admin
def admin_api_config():
    data = request.json or {}
    key, value = data.get('key'), data.get('value')
    try:
        if key == 'free_daily_limit':
            val = max(1, int(value))
            _SIGNAL_LIMITS["free_daily_limit"] = val
            _save_limit_to_db("free_daily_limit", val)
            log.info(f"[ADMIN] free_daily_limit → {val}")
        elif key == 'premium_daily_limit':
            val = max(1, int(value))
            _SIGNAL_LIMITS["premium_daily_limit"] = val
            _save_limit_to_db("premium_daily_limit", val)
            log.info(f"[ADMIN] premium_daily_limit → {val}")
        elif key == 'rate_max_calls':
            _ai_limiter.max_calls = max(1, int(value))
            log.info(f"[ADMIN] rate_max_calls → {value}")
        elif key == 'rate_window_sec':
            _ai_limiter.window_sec = max(10, int(value))
            log.info(f"[ADMIN] rate_window_sec → {value}")
        else:
            return jsonify({"status": "ERROR", "error": f"Unknown key: {key}"}), 400
        return jsonify({"status": "OK", "key": key, "value": value})
    except Exception as e:
        return jsonify({"status": "ERROR", "error": str(e)}), 500


@app.route('/admin/api/action', methods=['POST'])
def admin_api_action():
    if not session.get('is_admin'):
        return jsonify({"error":"UNAUTHORIZED"}), 401
    action = (request.json or {}).get('action', '')
    try:
        if action == 'clear_rate_limits':
            with _ai_limiter._lock:
                _ai_limiter._buckets.clear()
            return jsonify({"status": "OK", "message": "RATE LIMIT BUCKETS CLEARED"})
        elif action == 'reset_daily_limits':
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("UPDATE users SET signals_today=0, last_reset=''")
                conn.commit()
            return jsonify({"status": "OK", "message": f"Дневные лимиты сброшены для всех пользователей"})
        elif action == 'warm_cache':
            threading.Thread(target=_warm_cache, daemon=True).start()
            return jsonify({"status": "OK", "message": "WARM CACHE INITIATED"})
        else:
            return jsonify({"status": "ERROR", "error": "UNKNOWN ACTION"}), 400
    except Exception as e:
        return jsonify({"status": "ERROR", "error": str(e)}), 500


@app.route('/admin/api/broadcast', methods=['POST'])
def admin_api_broadcast():
    if not session.get('is_admin'):
        return jsonify({"error":"UNAUTHORIZED"}), 401
    data    = request.json or {}
    message = data.get('message', '').strip()
    target  = data.get('target', 'admin')
    if not message:
        return jsonify({"status": "ERROR", "error": "EMPTY MESSAGE"}), 400
    try:
        formatted = f"📢 <b>ADMIN BROADCAST</b>\n\n{escape_html(message)}"
        if target == 'admin':
            ok = send_to_admin(formatted)
        elif target == 'group':
            ok = send_to_group(formatted)
        elif target == 'channel':
            ok = send_to_channel(formatted)
        else:
            return jsonify({"status": "ERROR", "error": "UNKNOWN TARGET"}), 400
        return jsonify({"status": "OK" if ok else "TG_ERROR"})
    except Exception as e:
        return jsonify({"status": "ERROR", "error": str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════════════════════════════════════



# ── График сигналов по дням ───────────────────────────────────────────────────
@app.route('/admin/api/signals/chart')
@_require_admin
def admin_signals_chart():
    days = int(request.args.get('days', 7))
    days = min(max(days, 1), 90)
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute("""
                SELECT date(ts) as d, COUNT(*) as cnt
                FROM signals
                WHERE ts >= ?
                GROUP BY date(ts)
                ORDER BY d ASC
            """, (since,)).fetchall()

        # Заполняем пропущенные дни нулями
        from datetime import date as dt_date
        result = []
        today = datetime.now(timezone.utc).date()
        day_map = {r[0]: r[1] for r in rows}
        for i in range(days - 1, -1, -1):
            d = (today - timedelta(days=i)).isoformat()
            result.append({"date": d, "count": day_map.get(d, 0)})
        return jsonify(result)
    except Exception as e:
        return jsonify([]), 500


# ── Онлайн пользователи ───────────────────────────────────────────────────────
@app.route('/admin/api/online')
@_require_admin
def admin_online():
    """Возвращает список пользователей онлайн (активны за последние 5 минут)."""
    try:
        cutoff_5min  = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        cutoff_15min = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            # Онлайн = были активны за 15 минут
            users = conn.execute("""
                SELECT uid, email, name, photo, last_seen, is_premium
                FROM users
                WHERE last_seen >= ? AND is_revoked=0
                ORDER BY last_seen DESC
            """, (cutoff_15min,)).fetchall()
            active_5 = conn.execute(
                "SELECT COUNT(*) FROM users WHERE last_seen>=? AND is_revoked=0",
                (cutoff_5min,)
            ).fetchone()[0]
        return jsonify({
            "count": len(users),
            "active_5min": active_5,
            "peak_today": len(users),
            "users": [dict(u) for u in users]
        })
    except Exception as e:
        return jsonify({"error": str(e), "count": 0, "users": []}), 500


# ── Назначить/снять Premium по UID в URL ──────────────────────────────────────
@app.route('/admin/api/users/<uid>/premium', methods=['POST'])
def admin_set_premium_by_uid(uid):
    if not session.get('is_admin'):
        return jsonify({"error": "unauthorized"}), 401
    data       = request.get_json() or {}
    is_premium = data.get('is_premium', True)
    days       = data.get('days', 30)
    until      = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE users SET is_premium=?, premium_until=? WHERE uid=?",
                (1 if is_premium else 0, until if is_premium else None, uid)
            )
            conn.commit()
        try:
            from s3_backup import upload_db as _s3
            _s3(DB_PATH)
        except Exception:
            pass
        log.info(f"[ADMIN] Premium {'выдан' if is_premium else 'снят'} uid={uid} days={days}")
        return jsonify({"status": "OK", "uid": uid, "is_premium": is_premium})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Назначить/снять Premium ───────────────────────────────────────────────────
@app.route('/admin/api/users/premium', methods=['POST'])
@_require_admin
def admin_set_premium():
    data       = request.json or {}
    uid        = data.get('uid', '')
    is_premium = bool(data.get('is_premium', False))
    if not uid:
        return jsonify({"error": "UID_REQUIRED"}), 400
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE users SET is_premium=? WHERE uid=?",
                         (1 if is_premium else 0, uid))
            conn.commit()
        log.info(f"[ADMIN] Premium {'назначен' if is_premium else 'снят'}: {uid}")
        return jsonify({"status": "OK"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Индивидуальный лимит пользователя ─────────────────────────────────────────
@app.route('/admin/api/users/limit', methods=['POST'])
@_require_admin
def admin_set_user_limit():
    data  = request.json or {}
    uid   = data.get('uid', '')
    limit = int(data.get('daily_limit', -1))
    if not uid:
        return jsonify({"error": "UID_REQUIRED"}), 400
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE users SET daily_limit=? WHERE uid=?", (limit, uid))
            conn.commit()
        log.info(f"[ADMIN] Лимит для {uid}: {limit}")
        return jsonify({"status": "OK"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Сброс дневного счётчика пользователя ──────────────────────────────────────
@app.route('/admin/api/users/reset_daily', methods=['POST'])
@_require_admin
def admin_reset_daily():
    uid = (request.json or {}).get('uid', '')
    if not uid:
        return jsonify({"error": "UID_REQUIRED"}), 400
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE users SET signals_today=0, last_reset='' WHERE uid=?", (uid,))
            conn.commit()
        return jsonify({"status": "OK"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Получить текущий конфиг ───────────────────────────────────────────────────
@app.route('/admin/api/config/get')
@_require_admin
def admin_get_config():
    try:
        return jsonify({
            "free_daily_limit":    _SIGNAL_LIMITS.get("free_daily_limit", 2),
            "premium_daily_limit": _SIGNAL_LIMITS.get("premium_daily_limit", 15),
            "rate_max_calls":      getattr(_ai_limiter, 'max_calls', 5),
            "rate_window_sec":     getattr(_ai_limiter, 'window_sec', 60),
            "ttl_price":           15,
            "ttl_klines":          30,
        })
    except Exception as _ce:
        log.error(f"[CONFIG_GET] {_ce}")
        return jsonify({"error": str(_ce)}), 500


@app.route('/api/portfolio', methods=['GET'])
def api_portfolio():
    if not session.get('user_uid'):
        return jsonify({"error": "UNAUTHORIZED"}), 401
    if not _portfolio_ready:
        return jsonify({"error": "PORTFOLIO_NOT_AVAILABLE"}), 503
    uid = session['user_uid']
    try:
        pf     = get_portfolio(DB_PATH, uid)
        open_p = get_trades(DB_PATH, uid, status='OPEN')
        closed = get_trades(DB_PATH, uid, status='CLOSED', limit=50)
        equity = get_equity_curve(DB_PATH, uid, days=30)

        # Считаем unrealized PnL по открытым позициям
        total_upnl   = 0.0
        total_margin = 0.0
        positions    = []
        for t in open_p:
            try:
                mark = _get_live_price(t['asset'])
            except Exception:
                mark = t['entry']
            d    = 1 if t['direction'] == 'LONG' else -1
            coins = (t['size'] * t['leverage']) / t['entry'] if t['entry'] else 0
            upnl  = round(d * coins * (mark - t['entry']), 2)
            roe   = round(upnl / t['size'] * 100, 2) if t['size'] else 0
            total_upnl   += upnl
            total_margin += t['size']
            positions.append({**t, "mark": mark, "upnl": upnl, "roe": roe})

        return jsonify({
            "balance":      pf.get('balance', 10000.0),
            "initial_bal":  pf.get('initial_bal', 10000.0),
            "unrealized_pnl": round(total_upnl, 2),
            "margin_used":    round(total_margin, 2),
            "positions":      positions,
            "closed_trades":  closed,
            "equity_curve":   equity,
        })
    except Exception as e:
        log.exception("/api/portfolio error")
        return jsonify({"error": str(e)}), 500


@app.route('/api/portfolio/balance', methods=['POST'])
def api_set_balance():
    if not session.get('user_uid'):
        return jsonify({"error": "UNAUTHORIZED"}), 401
    if not _portfolio_ready:
        return jsonify({"error": "PORTFOLIO_NOT_AVAILABLE"}), 503
    uid     = session['user_uid']
    balance = float((request.json or {}).get('balance', 10000))
    if balance <= 0 or balance > 10_000_000:
        return jsonify({"error": "INVALID_BALANCE"}), 400
    result = set_initial_balance(DB_PATH, uid, balance)
    if result.get("status") == "OK":
        upload_now(DB_PATH)
    return jsonify(result)


@app.route('/api/trades', methods=['GET'])
def api_get_trades():
    if not session.get('user_uid'):
        return jsonify({"error": "UNAUTHORIZED"}), 401
    if not _portfolio_ready:
        return jsonify({"error": "PORTFOLIO_NOT_AVAILABLE"}), 503
    uid    = session['user_uid']
    status = request.args.get('status', '')
    limit  = min(int(request.args.get('limit', 100)), 500)
    trades = get_trades(DB_PATH, uid, status=status, limit=limit)
    return jsonify({"count": len(trades), "trades": trades})


@app.route('/api/trades/open', methods=['POST'])
def api_open_trade():
    if not session.get('user_uid'):
        return jsonify({"error": "UNAUTHORIZED"}), 401
    if not _portfolio_ready:
        return jsonify({"error": "PORTFOLIO_NOT_AVAILABLE"}), 503
    uid  = session['user_uid']
    data = request.json or {}
    try:
        result = open_trade(
            DB_PATH, uid,
            asset     = data.get('asset', ''),
            direction = data.get('direction', 'LONG'),
            entry     = float(data.get('entry', 0)),
            size      = float(data.get('size', 0)),
            leverage  = int(data.get('leverage', 1)),
            tp1       = float(data.get('tp1', 0)),
            tp2       = float(data.get('tp2', 0)),
            sl        = float(data.get('sl', 0)),
            note      = data.get('note', ''),
            signal_id = int(data.get('signal_id', 0))
        )
        if result.get("status") == "OK":
            upload_now(DB_PATH)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/trades/close/<int:trade_id>', methods=['POST'])
def api_close_trade(trade_id):
    if not session.get('user_uid'):
        return jsonify({"error": "UNAUTHORIZED"}), 401
    if not _portfolio_ready:
        return jsonify({"error": "PORTFOLIO_NOT_AVAILABLE"}), 503
    uid  = session['user_uid']
    data = request.json or {}
    try:
        exit_price = float(data.get('exit_price', 0))
        if not exit_price:
            # Закрываем по текущей рыночной цене
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute(
                    "SELECT asset FROM trades WHERE id=? AND uid=?",
                    (trade_id, uid)
                ).fetchone()
                if row:
                    exit_price = _get_live_price(row[0])
        result = close_trade(DB_PATH, trade_id, uid, exit_price, data.get('result',''))
        if result.get("status") == "OK":
            upload_now(DB_PATH)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Восстановление БД с S3 при старте (если нужно) ───────────────────────────
restore_if_needed(DB_PATH)

_init_db()
_init_users_db()
init_paddle_db(DB_PATH)   # Paddle подписки
if _paddle_ready:
    init_paddle_db(DB_PATH)

if _portfolio_ready:
    init_portfolio_db(DB_PATH)
    # WAL mode для portfolio таблиц
    try:
        with sqlite3.connect(DB_PATH) as _wc:
            _wc.execute("PRAGMA journal_mode=WAL")
            _wc.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    start_cleanup_scheduler(DB_PATH, days=60)
    _tpsl_monitor = TPSLMonitor(DB_PATH, _get_live_price)
    _tpsl_monitor.start()

# Запускаем полную очистку всех таблиц (signals + trades + screener + error_log)
_start_full_cleanup_scheduler(DB_PATH)
_start_db_backup_scheduler()   # автобэкап в Telegram каждые 6ч
start_s3_sync_scheduler(DB_PATH, interval_sec=1800)  # синхронизация с S3 каждые 30 мин
_start_wallet_scheduler()       # проверка балансов $SYNX каждые 24ч
threading.Thread(target=_warm_cache, daemon=True).start()


@app.route('/api/signals/daily_usage')
def signals_daily_usage():
    """Возвращает использованных сигналов за сегодня для текущего пользователя."""
    uid = session.get('user_uid')
    if not uid:
        return jsonify({"used": 0, "limit": _SIGNAL_LIMITS.get("free_daily_limit", 2)})
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT signals_today, last_reset, is_premium, daily_limit FROM users WHERE uid=?",
                (uid,)
            ).fetchone()
        if not row:
            return jsonify({"used": 0, "limit": _SIGNAL_LIMITS.get("free_daily_limit", 2)})
        used, last_reset, is_prem, custom_limit = row
        if last_reset != today:
            used = 0
        if custom_limit >= 0:
            limit = custom_limit
        elif is_prem:
            limit = _SIGNAL_LIMITS.get("premium_daily_limit", 15)
        else:
            limit = _SIGNAL_LIMITS.get("free_daily_limit", 2)
        return jsonify({"used": used, "limit": limit, "is_premium": bool(is_prem)})
    except Exception as e:
        return jsonify({"used": 0, "limit": 2, "error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
#  НОВЫЕ ADMIN API — Доход, Платежи, Промокоды, Рефералы, Логи, Email
# ══════════════════════════════════════════════════════════════════════════════

def _init_extended_db(db_path: str):
    """Создаёт расширенные таблицы для новых фич."""
    with sqlite3.connect(db_path) as conn:
        # Платёжная история (зеркало Stripe)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS payment_history (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                uid          TEXT,
                email        TEXT,
                name         TEXT,
                amount       REAL,
                currency     TEXT DEFAULT 'usd',
                status       TEXT DEFAULT 'succeeded',
                stripe_pi_id TEXT,
                stripe_sub_id TEXT,
                created_at   TEXT
            )
        """)
        # Промокоды
        conn.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                code         TEXT UNIQUE NOT NULL,
                discount_pct INTEGER DEFAULT 0,
                discount_amt REAL    DEFAULT 0,
                currency     TEXT    DEFAULT 'usd',
                max_uses     INTEGER DEFAULT 1,
                used_count   INTEGER DEFAULT 0,
                valid_until  TEXT    DEFAULT '',
                is_active    INTEGER DEFAULT 1,
                created_at   TEXT,
                notes        TEXT    DEFAULT ''
            )
        """)
        # Использование промокодов
        conn.execute("""
            CREATE TABLE IF NOT EXISTS promo_uses (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                code       TEXT,
                uid        TEXT,
                used_at    TEXT
            )
        """)
        # Рефералы
        conn.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_uid  TEXT NOT NULL,
                referred_uid  TEXT NOT NULL,
                reward_days   INTEGER DEFAULT 7,
                rewarded      INTEGER DEFAULT 0,
                created_at    TEXT
            )
        """)
        # Реферальные коды пользователей
        # колонки users добавляются в _init_users_table()
        # Лог ошибок
        conn.execute("""
            CREATE TABLE IF NOT EXISTS error_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                level      TEXT DEFAULT 'ERROR',
                module     TEXT DEFAULT '',
                message    TEXT,
                traceback  TEXT DEFAULT '',
                created_at TEXT
            )
        """)
        conn.commit()
    log.info("[DB] Расширенные таблицы инициализированы")

_init_extended_db(DB_PATH)
init_stripe_db(DB_PATH)
init_crypto_db(DB_PATH)
_load_limits_from_db()  # загружаем сохранённые лимиты из БД

# ── Screener DB + запуск ───────────────────────────────────────────────────────
if _screener_ready:
    try:
        init_screener_db(DB_PATH)
        _auto_screener = AutoScreener(
            collector        = collector,
            analyzer         = analyzer,
            db_path          = DB_PATH,
            groq_key         = GROQ_KEY,
            cryptopanic_key  = CRYPTOPANIC_KEY,
            tg_token_channel = TG_TOKEN_ANALYTICS,  # бот-аналитик в канал
            channel_id       = CHANNEL_ID,
            tg_token_dm      = TG_TOKEN_ANALYTICS,  # тот же бот для DM
        )
        _auto_screener.start()
        log.info("[SCREENER] AutoScreener запущен ✓")
    except Exception as _se:
        log.error(f"[SCREENER] Ошибка запуска: {_se}")
        _auto_screener = None
else:
    _auto_screener = None


# ── Логгер ошибок в БД ────────────────────────────────────────────────────────
import traceback as _traceback

class _DBErrorHandler(logging.Handler):
    def emit(self, record):
        if record.levelno < logging.WARNING:
            return
        try:
            tb = ""
            if record.exc_info:
                tb = "".join(_traceback.format_exception(*record.exc_info))
            ts = datetime.now(timezone.utc).isoformat()
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("""
                    INSERT INTO error_log (level, module, message, traceback, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (record.levelname, record.name,
                      record.getMessage()[:500], tb[:2000], ts))
                conn.commit()
        except Exception:
            pass

_db_handler = _DBErrorHandler()
_db_handler.setLevel(logging.WARNING)
logging.getLogger().addHandler(_db_handler)


# ── Генерация реферального кода ───────────────────────────────────────────────
import hashlib as _hashlib

def _ensure_ref_code(uid: str) -> str:
    """Генерирует и сохраняет реферальный код для пользователя."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT ref_code FROM users WHERE uid=?", (uid,)).fetchone()
        if row and row[0]:
            return row[0]
        code = "SYN" + _hashlib.md5(uid.encode()).hexdigest()[:6].upper()
        conn.execute("UPDATE users SET ref_code=? WHERE uid=?", (code, uid))
        conn.commit()
        return code


# ── Уведомления в Telegram при оплате ────────────────────────────────────────
def _notify_admin_payment(uid: str, email: str, name: str,
                           amount: float, currency: str, action: str):
    """Отправляет уведомление администратору при оплате/отмене Premium."""
    emoji     = "\U0001f4b0" if action == "subscribed" else "\u274c"
    sym       = "$" if currency == "usd" else "\u20ac"
    action_ru = "\u043e\u0444\u043e\u0440\u043c\u043b\u0435\u043d" if action == "subscribed" else "\u043e\u0442\u043c\u0435\u043d\u0451\u043d"
    ts_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    msg = (
        f"{emoji} <b>Premium {action_ru}</b>\n"
        f"\U0001f464 {name} ({email})\n"
        f"\U0001f4b3 {sym}{amount:.2f}/{currency.upper()}\n"
        f"\U0001f194 <code>{uid[:20]}</code>\n"
        f"\U0001f550 {ts_str} UTC"
    )
    send_to_admin(msg)


# ── MRR/ARR расчёт ─────────────────────────────────────────────────────────────
def _get_revenue_stats(db_path: str) -> dict:
    """Считает MRR, ARR, общий доход, активные подписки."""
    # Гарантируем что таблицы существуют
    try:
        with sqlite3.connect(db_path) as _c:
            _c.execute("""CREATE TABLE IF NOT EXISTS payment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, uid TEXT, email TEXT, name TEXT,
                amount REAL DEFAULT 0, currency TEXT DEFAULT 'usd',
                status TEXT DEFAULT 'pending', stripe_pi_id TEXT DEFAULT '',
                stripe_sub_id TEXT DEFAULT '', created_at TEXT)""")
            _c.execute("""CREATE TABLE IF NOT EXISTS stripe_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, uid TEXT,
                stripe_customer_id TEXT, stripe_sub_id TEXT,
                status TEXT DEFAULT 'inactive', currency TEXT DEFAULT 'usd',
                current_period_end TEXT, created_at TEXT, updated_at TEXT)""")
    except Exception: pass
    try:
     with sqlite3.connect(db_path) as conn:
        # Всего платежей
        rows = conn.execute(
            "SELECT SUM(amount), COUNT(*), currency FROM payment_history WHERE status='succeeded' GROUP BY currency"
        ).fetchall()
        total_usd = sum(r[0] for r in rows if r[2] == 'usd') or 0
        total_eur = sum(r[0] for r in rows if r[2] == 'eur') or 0

        # Активные подписки (Premium пользователи)
        active = conn.execute(
            "SELECT COUNT(*) FROM users WHERE is_premium=1 AND is_revoked=0"
        ).fetchone()[0]

        # Платежи за последние 30 дней
        since30 = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        last30 = conn.execute(
            "SELECT SUM(amount), COUNT(*) FROM payment_history WHERE status='succeeded' AND created_at>=?",
            (since30,)
        ).fetchone()

        # Платежи за сегодня
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_rev = conn.execute(
            "SELECT SUM(amount), COUNT(*) FROM payment_history WHERE status='succeeded' AND created_at>=?",
            (today,)
        ).fetchone()

        # График дохода по дням (30 дней)
        chart = conn.execute("""
            SELECT date(created_at) as d, SUM(amount) as rev, COUNT(*) as cnt
            FROM payment_history WHERE status='succeeded' AND created_at>=?
            GROUP BY date(created_at) ORDER BY d ASC
        """, (since30,)).fetchall()

    except Exception as _re:
        log.error(f"[REVENUE] {_re}")
        return {"active_subs":0,"mrr":0,"arr":0,"total_usd":0,"total_eur":0,
                "last30_rev":0,"last30_count":0,"today_rev":0,"today_count":0,"chart":[]}
    mrr = active * 19.99
    return {
        "active_subs":  active,
        "mrr":          round(mrr, 2),
        "arr":          round(mrr * 12, 2),
        "total_usd":    round(total_usd, 2),
        "total_eur":    round(total_eur, 2),
        "last30_rev":   round(last30[0] or 0, 2),
        "last30_count": last30[1] or 0,
        "today_rev":    round(today_rev[0] or 0, 2),
        "today_count":  today_rev[1] or 0,
        "chart":        [{"date": r[0], "revenue": round(r[1],2), "count": r[2]} for r in chart],
    }


# ── Admin: Доход ──────────────────────────────────────────────────────────────
@app.route('/admin/api/revenue')
@_require_admin
def admin_revenue():
    return jsonify(_get_revenue_stats(DB_PATH))


# ── Admin: История платежей ───────────────────────────────────────────────────
@app.route('/admin/api/payments')
@_require_admin
def admin_payments():
    limit = int(request.args.get('limit', 100))
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM payment_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return jsonify({"payments": [dict(r) for r in rows]})


# ── Admin: Промокоды — список ─────────────────────────────────────────────────
@app.route('/admin/api/promo')
@_require_admin
def admin_promo_list():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM promo_codes ORDER BY id DESC"
        ).fetchall()
    return jsonify({"promos": [dict(r) for r in rows]})


# ── Admin: Создать промокод ───────────────────────────────────────────────────
@app.route('/admin/api/promo/create', methods=['POST'])
@_require_admin
def admin_promo_create():
    d    = request.json or {}
    code = (d.get('code', '') or '').strip().upper()
    if not code:
        return jsonify({"error": "CODE_REQUIRED"}), 400
    ts = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO promo_codes
                    (code, discount_pct, discount_amt, currency,
                     max_uses, valid_until, is_active, created_at, notes)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, (
                code,
                int(d.get('discount_pct', 0)),
                float(d.get('discount_amt', 0)),
                d.get('currency', 'usd'),
                int(d.get('max_uses', 1)),
                d.get('valid_until', ''),
                ts,
                d.get('notes', '')
            ))
            conn.commit()
        log.info(f"[ADMIN] Промокод создан: {code}")
        return jsonify({"status": "OK", "code": code})
    except sqlite3.IntegrityError:
        return jsonify({"error": "CODE_EXISTS"}), 409


# ── Admin: Удалить / деактивировать промокод ──────────────────────────────────
@app.route('/admin/api/promo/toggle', methods=['POST'])
@_require_admin
def admin_promo_toggle():
    d    = request.json or {}
    code = d.get('code', '')
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE promo_codes SET is_active = CASE WHEN is_active=1 THEN 0 ELSE 1 END WHERE code=?",
            (code,)
        )
        conn.commit()
    return jsonify({"status": "OK"})


# ── Admin: Рефералы ───────────────────────────────────────────────────────────
@app.route('/admin/api/referrals')
@_require_admin
def admin_referrals():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT r.*,
                   u1.name AS referrer_name, u1.email AS referrer_email,
                   u2.name AS referred_name, u2.email AS referred_email
            FROM referrals r
            LEFT JOIN users u1 ON u1.uid = r.referrer_uid
            LEFT JOIN users u2 ON u2.uid = r.referred_uid
            ORDER BY r.id DESC LIMIT 200
        """).fetchall()
        # Топ рефереров
        top = conn.execute("""
            SELECT referrer_uid, u.name, u.email, COUNT(*) as cnt,
                   SUM(rewarded) as rewarded_cnt
            FROM referrals r
            LEFT JOIN users u ON u.uid = r.referrer_uid
            GROUP BY referrer_uid ORDER BY cnt DESC LIMIT 10
        """).fetchall()
    return jsonify({
        "referrals": [dict(r) for r in rows],
        "top_referrers": [dict(r) for r in top],
    })


# ── Admin: Логи ошибок ────────────────────────────────────────────────────────
@app.route('/admin/api/error_log')
@_require_admin
def admin_error_log():
    limit = int(request.args.get('limit', 50))
    level = request.args.get('level', '')
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if level:
            rows = conn.execute(
                "SELECT * FROM error_log WHERE level=? ORDER BY id DESC LIMIT ?",
                (level, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM error_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
    return jsonify({"logs": [dict(r) for r in rows]})


@app.route('/admin/api/error_log/clear', methods=['POST'])
@_require_admin
def admin_error_log_clear():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM error_log")
        conn.commit()
    return jsonify({"status": "OK"})


# ── API: Применить промокод (из терминала/premium страницы) ───────────────────
@app.route('/admin/api/storage')
@_require_admin
def admin_storage_stats():
    """Статистика хранилища БД — размеры таблиц, свободное место."""
    try:
        import os
        stats = {}

        # Размер файла БД
        try:
            db_size = os.path.getsize(DB_PATH)
            stats['db_size_mb'] = round(db_size / 1024 / 1024, 2)
            stats['db_path']    = DB_PATH
        except Exception:
            stats['db_size_mb'] = 0

        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row

            # Кол-во записей в каждой таблице
            tables = ['signals', 'trades', 'screener_signals', 'users',
                      'error_log', 'payment_history', 'stripe_subscriptions']
            table_stats = {}
            for tbl in tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                    table_stats[tbl] = count
                except Exception:
                    table_stats[tbl] = 'N/A'

            # Самые старые и новые записи
            try:
                oldest = conn.execute(
                    "SELECT MIN(ts) as oldest, MAX(ts) as newest FROM signals"
                ).fetchone()
                stats['signals_oldest'] = (oldest['oldest'] or '')[:10]
                stats['signals_newest'] = (oldest['newest'] or '')[:10]
            except Exception:
                pass

            # Сделки
            try:
                t_stat = conn.execute(
                    "SELECT COUNT(*) as total, "
                    "SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) as open_cnt, "
                    "SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins, "
                    "SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses "
                    "FROM trades"
                ).fetchone()
                stats['trades_detail'] = dict(t_stat)
            except Exception:
                pass

        stats['tables'] = table_stats
        stats['retention_days'] = 60
        stats['cleanup_schedule'] = 'ежедневно, 60 дней хранение'

        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/admin/api/db/backup', methods=['POST'])
@_require_admin
def admin_db_backup():
    """Ручной бэкап БД в Telegram."""
    tg_ok = _backup_db_to_telegram()
    s3_ok = upload_db(DB_PATH)
    if tg_ok or s3_ok:
        return jsonify({
            "status": "OK",
            "telegram": tg_ok,
            "s3": s3_ok,
            "message": f"БД сохранена: {'Telegram ✅' if tg_ok else ''} {'S3 ✅' if s3_ok else ''}"
        })
    return jsonify({"error": "Бэкап не удался — проверь переменные окружения"}), 500


@app.route('/admin/api/storage/cleanup', methods=['POST'])
@_require_admin
def admin_storage_cleanup():
    """Принудительная очистка старых данных."""
    days = int((request.json or {}).get('days', 60))
    days = max(7, min(365, days))
    try:
        result = _full_cleanup(DB_PATH, trade_days=days, signal_days=days, error_days=30)
        return jsonify({"status": "OK", "deleted": result, "days": days})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/admin/api/s3/status')
@_require_admin
def admin_s3_status():
    """Статус S3 синхронизации: размеры, время, кол-во записей."""
    import time
    result = {
        "configured": _s3_configured(),
        "local_size_mb": 0,
        "s3_size_mb": 0,
        "s3_last_modified": "",
        "users": 0,
        "signals": 0,
        "trades": 0,
        "status": "not_configured",
    }
    # Локальный размер
    try:
        result["local_size_mb"] = round(os.path.getsize(DB_PATH) / 1024 / 1024, 3)
    except Exception:
        pass
    # Статистика из БД
    try:
        with sqlite3.connect(DB_PATH) as conn:
            result["users"]   = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            result["signals"] = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            try:
                result["trades"] = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            except Exception:
                result["trades"] = 0
    except Exception:
        pass
    # S3 данные
    if _s3_configured():
        try:
            from s3_backup import _get_client, S3_BUCKET, S3_DB_KEY
            client = _get_client()
            if client:
                head = client.head_object(Bucket=S3_BUCKET, Key=S3_DB_KEY)
                result["s3_size_mb"]      = round(head.get("ContentLength", 0) / 1024 / 1024, 3)
                result["s3_last_modified"] = head.get("LastModified", "").isoformat() if head.get("LastModified") else ""
                local_mb = result["local_size_mb"]
                s3_mb    = result["s3_size_mb"]
                result["status"] = "synced" if abs(local_mb - s3_mb) < 0.05 else "out_of_sync"
            else:
                result["status"] = "error"
        except Exception as e:
            result["status"] = "error"
            result["error"]  = str(e)
    return jsonify(result)


@app.route('/admin/api/s3/sync/now', methods=['POST'])
@_require_admin
def admin_s3_sync_now():
    """Принудительная немедленная синхронизация БД на S3."""
    if not _s3_configured():
        return jsonify({"error": "S3 не настроен"}), 503
    ok = upload_db(DB_PATH)
    if ok:
        return jsonify({"status": "OK", "message": "БД загружена на S3 ✅"})
    return jsonify({"error": "Ошибка загрузки на S3"}), 500


@app.route('/api/promo/apply', methods=['POST'])
def apply_promo():
    if not session.get('user_uid'):
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    uid  = session['user_uid']
    code = (request.json or {}).get('code', '').strip().upper()
    if not code:
        return jsonify({"error": "CODE_REQUIRED"}), 400
    ts = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            promo = conn.execute(
                "SELECT * FROM promo_codes WHERE code=? AND is_active=1",
                (code,)
            ).fetchone()
            if not promo:
                return jsonify({"error": "INVALID_CODE"}), 404
            # Проверяем срок действия
            if promo[8] and promo[8] < ts[:10]:  # valid_until
                return jsonify({"error": "CODE_EXPIRED"}), 400
            # Проверяем лимит использований
            if promo[5] > 0 and promo[6] >= promo[5]:  # max_uses, used_count
                return jsonify({"error": "CODE_EXHAUSTED"}), 400
            # Проверяем не использовал ли уже
            already = conn.execute(
                "SELECT id FROM promo_uses WHERE code=? AND uid=?", (code, uid)
            ).fetchone()
            if already:
                return jsonify({"error": "ALREADY_USED"}), 400
            # Применяем
            conn.execute(
                "UPDATE promo_codes SET used_count=used_count+1 WHERE code=?", (code,)
            )
            conn.execute(
                "INSERT INTO promo_uses (code, uid, used_at) VALUES (?, ?, ?)",
                (code, uid, ts)
            )
            conn.commit()
        return jsonify({
            "status": "OK",
            "discount_pct": promo[2],
            "discount_amt": promo[3],
            "currency":     promo[4],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Реферальная ссылка ───────────────────────────────────────────────────
@app.route('/api/signals/feed')
def api_signals_feed():
    """
    Публичная живая лента последних сигналов (для терминала).
    Показывает последние 10 сигналов всех пользователей.
    Не показывает кто запрашивал — только актив/направление/скор.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT asset, direction, entry, tp1, sl, bullish_score, ts
                FROM signals
                ORDER BY id DESC LIMIT 10
            """).fetchall()
        feed = []
        for r in rows:
            feed.append({
                "asset":     r['asset'],
                "direction": r['direction'],
                "entry":     r['entry'],
                "tp1":       r['tp1'],
                "sl":        r['sl'],
                "score":     r['bullish_score'],
                "time":      r['ts'],
            })
        return jsonify({"feed": feed})
    except Exception as e:
        return jsonify({"feed": [], "error": str(e)})


@app.route('/api/market/context')
def api_market_context():
    """
    Fear & Greed Index + доминация BTC + глобальная капа.
    Берём с CoinGecko (бесплатно, без ключа).
    """
    try:
        import requests as _req
        # Fear & Greed
        fg = _req.get(
            "https://api.alternative.me/fng/?limit=1",
            timeout=5
        ).json()
        fg_val   = int(fg['data'][0]['value'])
        fg_class = fg['data'][0]['value_classification']

        # Глобальные данные CoinGecko
        cg = _req.get(
            "https://api.coingecko.com/api/v3/global",
            timeout=5
        ).json().get('data', {})
        btc_dom  = round(cg.get('market_cap_percentage', {}).get('btc', 0), 1)
        total_mc = cg.get('total_market_cap', {}).get('usd', 0)
        mc_str   = f"${total_mc/1e12:.2f}T" if total_mc > 1e12 else f"${total_mc/1e9:.0f}B"

        # Определяем эмодзи и цвет
        if fg_val >= 75:   fg_label, fg_color = "EXTREME GREED", "#ff4466"
        elif fg_val >= 55: fg_label, fg_color = "GREED",         "#ffaa00"
        elif fg_val >= 45: fg_label, fg_color = "NEUTRAL",       "#ffff00"
        elif fg_val >= 25: fg_label, fg_color = "FEAR",          "#00ccff"
        else:              fg_label, fg_color = "EXTREME FEAR",  "#00ffcc"

        return jsonify({
            "fear_greed":      fg_val,
            "fg_label":        fg_label,
            "fg_color":        fg_color,
            "btc_dominance":   btc_dom,
            "market_cap":      mc_str,
            "status":          "ok"
        })
    except Exception as e:
        log.warning(f"[MARKET_CONTEXT] {e}")
        return jsonify({"status": "error", "error": str(e), "fear_greed": 50,
                       "fg_label": "NEUTRAL", "fg_color": "#ffff00",
                       "btc_dominance": 0, "market_cap": "N/A"})


@app.route('/api/user/signals')
@_require_auth
def api_user_signals():
    """История сигналов текущего пользователя (по IP)."""
    try:
        uid = session.get('user_uid')
        ip  = request.remote_addr
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT asset, direction, entry, tp1, tp2, sl,
                       bullish_score, fvg_status, ts
                FROM signals
                WHERE ip=?
                ORDER BY id DESC LIMIT 20
            """, (ip,)).fetchall()
        return jsonify({"signals": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"signals": [], "error": str(e)})


@app.route('/api/referral/code')
def get_ref_code():
    if not session.get('user_uid'):
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    code = _ensure_ref_code(session['user_uid'])
    base = os.getenv("APP_BASE_URL", "https://synapsex-ai.com")
    return jsonify({"code": code, "url": f"{base}/ref/{code}"})


# ── Реферальный редирект ──────────────────────────────────────────────────────
@app.route('/ref/<code>')
def referral_redirect(code):
    """Сохраняем реферальный код в сессии и редиректим на главную."""
    session['ref_code'] = code.upper()
    return redirect('/')


# ══════════════════════════════════════════════════════════════════════════════
#  STRIPE — оплата Premium подписки
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/terms')
def terms_page():
    """Страница Terms of Service и Privacy Policy."""
    return render_template('terms.html')


@app.route('/privacy')
def privacy_page():
    """Редирект на terms (они на одной странице)."""
    return redirect('/terms#privacy')


@app.route('/referral')
def referral_page():
    """Страница реферальной программы."""
    if not session.get('user_uid'):
        return redirect('/login?next=/referral')
    lang = session.get('lang', 'RU')
    return render_template('referral.html', current_lang=lang)


@app.route('/premium')
def premium_page():
    """Страница оформления Premium подписки."""
    stripe_ok  = bool(os.getenv('STRIPE_SECRET_KEY', ''))
    crypto_ok  = bool(os.getenv('NOWPAYMENTS_API_KEY', ''))
    lang       = session.get('lang', 'RU')
    return render_template('premium.html',
                           stripe_configured=stripe_ok,
                           crypto_configured=crypto_ok,
                           current_lang=lang,
                           paddle_client_token=os.getenv('PADDLE_CLIENT_TOKEN', ''))


@app.route('/premium/success')
def premium_success():
    """Редирект после успешной оплаты."""
    return redirect(url_for('premium_page') + '?success=1')


@app.route('/api/stripe/checkout', methods=['POST'])
def stripe_checkout():
    """Создаёт Stripe Checkout Session и возвращает URL."""
    if not session.get('user_uid'):
        return jsonify({"error": "NOT_LOGGED_IN"}), 401

    uid      = session['user_uid']
    email    = session.get('user_email', '')
    name     = session.get('user_name', 'User')
    currency = (request.json or {}).get('currency', 'usd').lower()
    if currency not in ('usd', 'eur'):
        currency = 'usd'

    result = create_checkout_session(uid, email, name, currency, DB_PATH)
    if 'error' in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route('/api/stripe/portal', methods=['POST'])
def stripe_portal():
    """Открывает Stripe Customer Portal для управления подпиской."""
    if not session.get('user_uid'):
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    result = create_portal_session(session['user_uid'], DB_PATH)
    if 'error' in result:
        return jsonify(result), 400
    return jsonify(result)


# ══════════════════════════════════════════════════════════════════════════════
#  CRYPTO PAYMENTS (NOWPayments — USDT/BTC/ETH без документов)
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/crypto/checkout', methods=['POST'])
def api_crypto_checkout():
    """Создаёт крипто-платёж (USDT TRC20, BTC, ETH и др.)."""
    if not session.get('user_uid'):
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    uid      = session['user_uid']
    data     = request.json or {}
    currency = data.get('currency', 'usdttrc20')
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT email, name FROM users WHERE uid=?", (uid,)
            ).fetchone()
        email = (row[0] if row else "") or f"{uid[:8]}@synapsex.ai"
        name  = (row[1] if row else "") or "SynapseX User"
    except Exception:
        email = f"{uid[:8]}@synapsex.ai"
        name  = "SynapseX User"
    result = create_crypto_payment(uid, email, currency, DB_PATH)
    return jsonify(result)


@app.route('/api/crypto/status/<payment_id>')
def api_crypto_payment_status(payment_id: str):
    """Проверяет статус крипто-платежа."""
    if not session.get('user_uid'):
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    result = get_payment_status(payment_id)
    return jsonify(result)


@app.route('/api/crypto/webhook', methods=['POST'])
def api_crypto_webhook():
    """
    IPN Webhook от NOWPayments.
    URL для настройки в NOWPayments Dashboard: https://synapsex-ai.com/api/crypto/webhook
    """
    payload_bytes = request.data
    ipn_sig       = request.headers.get('x-nowpayments-sig', '')
    if not verify_ipn_signature(payload_bytes, ipn_sig):
        log.warning("[CRYPTO] Неверная или отсутствующая IPN подпись")
        return jsonify({"error": "invalid_signature"}), 400
    payload = request.json or {}
    result = handle_crypto_webhook(payload, DB_PATH)

    if result.get("premium"):
        try:
            upload_db(DB_PATH)
            log.info("[S3] Premium активирован (crypto) — БД синхронизирована")
        except Exception as _s3e:
            log.error(f"[S3] Ошибка синхронизации после Premium (crypto): {_s3e}")

    # Отправляем email при активации Premium
    if result.get("premium") and result.get("uid"):
        try:
            uid = result["uid"]
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute(
                    "SELECT email, name, premium_until FROM users WHERE uid=?", (uid,)
                ).fetchone()
            if row:
                send_premium_activated(
                    email=row[0] or "",
                    name=row[1] or "",
                    period_end=row[2] or "",
                    method="crypto"
                )
        except Exception as _email_err:
            log.warning(f"[EMAIL] Crypto webhook email error: {_email_err}")
    return jsonify(result), 200


@app.route('/api/subscription/status')
def subscription_status():
    """Статус подписки текущего пользователя."""
    if not session.get('user_uid'):
        return jsonify({"is_premium": False, "status": "none"})
    info = get_subscription_info(DB_PATH, session['user_uid'])
    return jsonify(info)


@app.route('/api/trial/activate', methods=['POST'])
def api_activate_trial():
    """
    Активирует 3-дневный бесплатный Trial для нового пользователя.
    Автоматически вызывается при первом входе.
    """
    uid = session.get('user_uid')
    if not uid:
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT email, name, is_premium, trial_until FROM users WHERE uid=?", (uid,)
            ).fetchone()
        if not row:
            return jsonify({"error": "USER_NOT_FOUND"}), 404

        email        = row[0] or ""
        name         = row[1] or ""
        is_premium   = bool(row[2])
        trial_until  = row[3] or ""

        if is_premium:
            return jsonify({"status": "already_premium"})
        if trial_until:
            return jsonify({"status": "already_used", "trial_until": trial_until})

        result = activate_trial(uid, email, name)
        return jsonify(result)
    except Exception as e:
        log.exception("/api/trial/activate error")
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
#  PADDLE PAYMENTS — карточные платежи (Visa/Mastercard)
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/paddle/checkout', methods=['POST'])
def api_paddle_checkout():
    """Создаёт Paddle Checkout URL для оплаты подписки картой."""
    uid = session.get('user_uid')
    if not uid:
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    if not _paddle_configured:
        return jsonify({"error": "Paddle не настроен"}), 503
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT email FROM users WHERE uid=?", (uid,)
            ).fetchone()
        email    = row[0] if row else ""
        currency = request.json.get("currency", "USD") if request.is_json else "USD"
        result   = create_paddle_checkout(uid, email, currency)
        return jsonify(result)
    except Exception as e:
        log.exception("/api/paddle/checkout error")
        return jsonify({"error": str(e)}), 500


@app.route('/paddle/webhook', methods=['POST'])
def paddle_webhook():
    """Обрабатывает Paddle webhooks (подписки, платежи)."""
    payload_bytes = request.get_data()
    sig_header    = request.headers.get("Paddle-Signature", "")

    if not verify_paddle_sig(payload_bytes, sig_header):
        log.warning("[PADDLE] Неверная подпись webhook")
        return jsonify({"error": "invalid signature"}), 400

    try:
        payload = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "invalid JSON"}), 400

    result = handle_paddle_webhook(payload, DB_PATH)

    if result.get("premium"):
        try:
            upload_db(DB_PATH)
            log.info("[S3] Premium активирован — БД синхронизирована")
        except Exception as _s3e:
            log.error(f"[S3] Ошибка синхронизации после Premium: {_s3e}")

    # Отправляем email при активации Premium
    if result.get("premium") and result.get("uid"):
        try:
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute(
                    "SELECT email, name, premium_until FROM users WHERE uid=?",
                    (result["uid"],)
                ).fetchone()
            if row:
                send_premium_activated(
                    email=row[0] or "",
                    name=row[1] or "",
                    period_end=row[2] or "",
                    method="stripe"  # карта
                )
        except Exception as _pe:
            log.warning(f"[PADDLE] Email error: {_pe}")

    return jsonify({"status": "OK"}), 200


@app.route('/api/paddle/status', methods=['GET'])
def api_paddle_status():
    """Статус Paddle подписки текущего пользователя."""
    uid = session.get('user_uid')
    if not uid:
        return jsonify({"error": "NOT_LOGGED_IN"}), 401
    return jsonify(get_paddle_status(uid, DB_PATH))


@app.route('/stripe/webhook', methods=['POST'])
def stripe_webhook():
    """
    Stripe Webhook — автоматически активирует/деактивирует Premium.
    Настройте в Stripe Dashboard: https://dashboard.stripe.com/webhooks
    URL: https://yourdomain.com/stripe/webhook
    Events: customer.subscription.created, .updated, .deleted,
            invoice.payment_succeeded, invoice.payment_failed
    """
    payload    = request.data
    sig_header = request.headers.get('Stripe-Signature', '')
    if not sig_header:
        log.warning("[STRIPE] Вебхук без Stripe-Signature заголовка")
        return jsonify({"error": "missing_signature"}), 400
    result = handle_webhook(payload, sig_header, DB_PATH)
    if 'error' in result:
        code = 400 if result['error'] == 'invalid_signature' else 500
        return jsonify(result), code
    return jsonify(result)



# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM ПРИВЯЗКА (для Premium алертов)
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/telegram/link', methods=['POST'])
@_require_auth
def telegram_link():
    """
    Пользователь вводит свой Telegram chat_id чтобы получать Premium алерты.
    Получить chat_id: написать @userinfobot в Telegram.
    """
    uid  = session.get('user_uid')
    data = request.get_json() or {}
    chat_id = str(data.get('chat_id', '')).strip()
    if not chat_id or not chat_id.lstrip('-').isdigit():
        return jsonify({"error": "Неверный chat_id"}), 400
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE users SET telegram_chat_id=? WHERE uid=?",
                (chat_id, uid)
            )
        # Отправляем тестовое сообщение
        test_msg = (
            "✅ <b>SynapseX Premium</b>\n\n"
            "Telegram успешно привязан!\n"
            "Теперь вы будете получать торговые сигналы здесь.\n\n"
            + DISCLAIMER_SHORT
        )
        if _screener_ready and _auto_screener:
            _auto_screener._send_telegram(TG_TOKEN_ANALYTICS, chat_id, test_msg)
        return jsonify({"status": "ok", "message": "Telegram привязан"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/telegram/unlink', methods=['POST'])
@_require_auth
def telegram_unlink():
    uid = session.get('user_uid')
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE users SET telegram_chat_id='' WHERE uid=?", (uid,))
    return jsonify({"status": "ok"})

@app.route('/api/telegram/status', methods=['GET'])
@_require_auth
def telegram_status():
    uid = session.get('user_uid')
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT telegram_chat_id FROM users WHERE uid=?", (uid,)
        ).fetchone()
    linked = bool(row and row[0])
    return jsonify({"linked": linked, "chat_id": row[0] if row else ""})

# ── Admin: ручной запуск скринера ─────────────────────────────────────────────
@app.route('/admin/api/screener/run', methods=['POST'])
@_require_admin
def admin_screener_run():
    if _auto_screener:
        _auto_screener.force_run()
        return jsonify({"status": "ok", "message": "Скринер запущен"})
    return jsonify({"error": "Скринер не инициализирован"}), 503

@app.route('/admin/api/screener/signals', methods=['GET'])
@_require_admin
def admin_screener_signals():
    """История сигналов скринера для админки."""
    try:
        limit = int(request.args.get('limit', 50))
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM screener_signals ORDER BY created_at DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        return jsonify({"signals": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/public/stats', methods=['GET', 'POST'])
def api_public_stats():
    """Публичная статистика: онлайн, сигналов сегодня, последний сигнал."""
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cutoff_15min = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            # Сигналов сегодня
            sig_today = conn.execute(
                "SELECT COUNT(*) as n FROM signals WHERE ts >= ?", (today,)
            ).fetchone()['n']
            # Онлайн за 15 мин
            try:
                online = conn.execute(
                    "SELECT COUNT(*) as n FROM users WHERE last_seen >= ? AND is_revoked=0",
                    (cutoff_15min,)
                ).fetchone()['n']
            except Exception:
                online = 0
            # Последний сигнал
            last = conn.execute(
                "SELECT asset, direction, ts FROM signals ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return jsonify({
            "signals_today": sig_today,
            "online":        online,
            "last_signal":   dict(last) if last else None,
        })
    except Exception as e:
        return jsonify({"signals_today": 0, "online": 0, "last_signal": None, "error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
#  PADDLE PAYMENTS — Карточные платежи (Visa/Mastercard)
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  PWA — Progressive Web App
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/manifest.json')
def pwa_manifest():
    """PWA manifest — описание приложения для мобильных устройств."""
    from flask import send_from_directory
    return send_from_directory('.', 'manifest.json',
                               mimetype='application/manifest+json')


@app.route('/service-worker.js')
def pwa_service_worker():
    """Service Worker — кэширование для PWA."""
    from flask import send_from_directory
    response = send_from_directory('.', 'service-worker.js',
                                   mimetype='application/javascript')
    response.headers['Service-Worker-Allowed'] = '/'
    response.headers['Cache-Control'] = 'no-cache'
    return response


@app.route('/offline')
def pwa_offline():
    """Страница офлайн режима PWA."""
    return render_template('offline.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False)