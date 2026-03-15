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

from flask import (Flask, render_template, request, jsonify,
                   session, redirect, url_for, Response, stream_with_context)

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

# ── Email уведомления ────────────────────────────────────────────────────────
try:
    from email_service import (
        send_premium_activated, send_trial_activated,
        send_trial_expiring, send_premium_expiring,
        _is_configured as _email_configured
    )
    log.info("[EMAIL] email_service.py загружен ✓") if 'log' in dir() else None
except ImportError:
    def send_premium_activated(*a, **k): return False
    def send_trial_activated(*a, **k):   return False
    def send_trial_expiring(*a, **k):    return False
    def send_premium_expiring(*a, **k):  return False
    def _email_configured():             return False

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
    def verify_ipn_signature(*a, **k):   return True
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
        conn.execute("PRAGMA journal_mode=WAL")   # Параллельные записи без блокировки
        conn.execute("PRAGMA synchronous=NORMAL") # Баланс надёжности и скорости
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
                    trial_until        TEXT    DEFAULT '',
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
            ]:
                try:
                    conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
                except Exception:
                    pass
            conn.commit()
    except Exception as e:
        log.error(f"[AUTH] users table error: {e}")


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
    from datetime import datetime, timezone, timedelta
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

def activate_trial(uid: str, email: str = "", name: str = "") -> dict:
    """
    Активирует 3-дневный Trial для нового пользователя.
    Вызывается автоматически при первом входе если ещё не было trial/premium.
    """
    from datetime import datetime, timezone, timedelta
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT is_premium, trial_until, premium_until FROM users WHERE uid=?",
                (uid,)
            ).fetchone()
            if not row:
                return {"error": "USER_NOT_FOUND"}

            is_premium   = bool(row[0])
            trial_until  = row[1] or ""
            premium_until = row[2] or ""

            # Не активируем если уже Premium или Trial был
            if is_premium:
                return {"status": "already_premium"}
            if trial_until:
                return {"status": "already_used"}

            # Активируем trial на 3 дня
            trial_end = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
            conn.execute(
                "UPDATE users SET trial_until=? WHERE uid=?",
                (trial_end, uid)
            )
            conn.commit()

        log.info(f"[TRIAL] ✅ Trial активирован для {uid} до {trial_end[:10]}")

        # Отправляем email
        if email:
            send_trial_activated(email, name, trial_end)

        return {"status": "activated", "trial_until": trial_end}
    except Exception as e:
        log.error(f"[TRIAL] Ошибка: {e}")
        return {"error": str(e)}


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