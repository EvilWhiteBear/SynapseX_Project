# -*- coding: utf-8 -*-
"""
nowpayments.py — Crypto оплата через NOWPayments
─────────────────────────────────────────────────────
• Принимает: USDT, BTC, ETH, TON, BNB, MATIC и 200+ монет
• Работает без документов, из любой страны
• Автоматическое подтверждение через IPN (webhook)
• Нужен только API ключ с nowpayments.io

.env переменные:
  NOWPAYMENTS_API_KEY     = ваш_ключ
  NOWPAYMENTS_IPN_SECRET  = секрет_для_webhook
  CRYPTO_PRICE_USD        = 19.99  (цена подписки)
"""
import os
import hmac
import hashlib
import json
import logging
import sqlite3
import requests
from datetime import datetime, timezone, timedelta

log = logging.getLogger("NOWPAYMENTS")

NOWPAYMENTS_API_KEY    = os.getenv("NOWPAYMENTS_API_KEY", "")
NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET", "")
APP_BASE_URL           = os.getenv("APP_BASE_URL", "https://synapsex-ai.com")
CRYPTO_PRICE_USD       = float(os.getenv("CRYPTO_PRICE_USD", "19.99"))
NP_BASE                = "https://api.nowpayments.io/v1"


def is_configured() -> bool:
    return bool(NOWPAYMENTS_API_KEY)


def create_crypto_payment(uid: str, email: str, currency: str = "usdttrc20",
                           db_path: str = "signals.db") -> dict:
    """
    Создаёт платёж. currency примеры: usdttrc20, usdterc20, btc, eth, ton, bnb
    Возвращает: {payment_url, payment_id, pay_address, pay_amount, status}
    """
    if not NOWPAYMENTS_API_KEY:
        return {"error": "NOWPAYMENTS_API_KEY не задан в .env"}

    try:
        headers = {
            "x-api-key": NOWPAYMENTS_API_KEY,
            "Content-Type": "application/json"
        }
        payload = {
            "price_amount":    CRYPTO_PRICE_USD,
            "price_currency":  "usd",
            "pay_currency":    currency,
            "order_id":        f"synapsex_{uid}_{int(datetime.now().timestamp())}",
            "order_description": f"SynapseX Premium - {email}",
            "ipn_callback_url":  f"{APP_BASE_URL}/api/crypto/webhook",
            "success_url":       f"{APP_BASE_URL}/premium?success=1&method=crypto",
            "cancel_url":        f"{APP_BASE_URL}/premium?cancelled=1",
        }
        r = requests.post(f"{NP_BASE}/invoice", json=payload, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        # Сохраняем pending платёж
        _save_crypto_payment(db_path, uid, data.get("id",""), currency, data.get("invoice_url",""))

        log.info(f"[CRYPTO] Платёж создан для {uid}: {data.get('id')}")
        return {
            "payment_url":  data.get("invoice_url", ""),
            "payment_id":   data.get("id", ""),
            "status":       "pending",
            "currency":     currency,
            "amount_usd":   CRYPTO_PRICE_USD,
        }
    except Exception as e:
        log.error(f"[CRYPTO] create_payment error: {e}")
        return {"error": str(e)}


def verify_ipn_signature(payload: bytes, received_hmac: str) -> bool:
    """Проверяет подпись IPN от NOWPayments.
    Возвращает False если секрет не задан или подпись неверна.
    """
    if not NOWPAYMENTS_IPN_SECRET:
        log.error("[CRYPTO] NOWPAYMENTS_IPN_SECRET не задан — вебхук отклонён")
        return False
    if not received_hmac:
        log.warning("[CRYPTO] Отсутствует заголовок x-nowpayments-sig")
        return False
    try:
        computed = hmac.new(
            NOWPAYMENTS_IPN_SECRET.encode(),
            payload,
            hashlib.sha512
        ).hexdigest()
        return hmac.compare_digest(computed, received_hmac)
    except Exception:
        return False


def handle_crypto_webhook(payload: dict, db_path: str) -> dict:
    """
    Обрабатывает IPN от NOWPayments.
    Статусы: waiting → confirming → confirmed → finished → failed/expired
    """
    payment_id = payload.get("payment_id", "")
    status     = payload.get("payment_status", "")
    order_id   = payload.get("order_id", "")  # synapsex_{uid}_{ts}

    log.info(f"[CRYPTO] IPN: payment_id={payment_id} status={status}")

    if status in ("finished", "confirmed"):
        # Идемпотентность: не активируем повторно уже обработанный платёж
        if _is_payment_already_processed(db_path, payment_id):
            log.info(f"[CRYPTO] Платёж {payment_id} уже обработан — пропускаем")
            return {"status": "OK", "already_processed": True}

        # Извлекаем uid из order_id
        uid = ""
        if order_id.startswith("synapsex_"):
            parts = order_id.split("_")
            if len(parts) >= 3:
                uid = parts[1]

        if not uid:
            # Ищем по payment_id в нашей БД
            uid = _get_uid_by_payment(db_path, payment_id)

        if uid:
            # Активируем Premium на 30 дней
            period_end = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
            _activate_crypto_premium(db_path, uid, payment_id, period_end)
            log.info(f"[CRYPTO] ✅ Premium активирован для {uid}")
            return {"status": "OK", "uid": uid, "premium": True}
        else:
            log.warning(f"[CRYPTO] UID не найден для payment_id={payment_id}")
            return {"status": "UID_NOT_FOUND"}

    elif status in ("failed", "expired", "refunded"):
        _update_payment_status(db_path, payment_id, status)
        log.info(f"[CRYPTO] Платёж {payment_id} → {status}")

    return {"status": "OK", "payment_status": status}


# ── DB helpers ────────────────────────────────────────────────────────────────

def init_crypto_db(db_path: str):
    """Создаёт таблицу crypto_payments."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS crypto_payments (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                uid          TEXT NOT NULL,
                payment_id   TEXT,
                currency     TEXT,
                amount_usd   REAL DEFAULT 19.99,
                status       TEXT DEFAULT 'pending',
                payment_url  TEXT,
                created_at   TEXT,
                updated_at   TEXT
            )
        """)
        conn.commit()
    log.info("[CRYPTO] БД инициализирована")


def _save_crypto_payment(db_path, uid, payment_id, currency, payment_url):
    ts = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                INSERT INTO crypto_payments (uid, payment_id, currency, amount_usd, status, payment_url, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
            """, (uid, payment_id, currency, CRYPTO_PRICE_USD, payment_url, ts, ts))
    except Exception as e:
        log.warning(f"[CRYPTO] save_payment: {e}")


def _is_payment_already_processed(db_path: str, payment_id: str) -> bool:
    """Проверяет, был ли платёж уже обработан (статус finished)."""
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT status FROM crypto_payments WHERE payment_id=?", (payment_id,)
            ).fetchone()
            return row is not None and row[0] == "finished"
    except Exception:
        return False


def _get_uid_by_payment(db_path, payment_id) -> str:
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT uid FROM crypto_payments WHERE payment_id=?", (payment_id,)
            ).fetchone()
            return row[0] if row else ""
    except Exception:
        return ""


def _update_payment_status(db_path, payment_id, status):
    ts = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "UPDATE crypto_payments SET status=?, updated_at=? WHERE payment_id=?",
                (status, ts, payment_id)
            )
    except Exception as e:
        log.warning(f"[CRYPTO] update_status: {e}")


def _activate_crypto_premium(db_path, uid, payment_id, period_end):
    ts = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE users SET is_premium=1, premium_until=? WHERE uid=?", (period_end, uid))
        conn.execute(
            "UPDATE crypto_payments SET status='finished', updated_at=? WHERE payment_id=?",
            (ts, payment_id)
        )
        # Также записываем в payment_history
        try:
            conn.execute("""
                INSERT INTO payment_history (uid, amount, currency, method, status, created_at)
                VALUES (?, ?, 'USD', 'crypto', 'success', ?)
            """, (uid, CRYPTO_PRICE_USD, ts))
        except Exception:
            pass
        conn.commit()


def get_payment_status(payment_id: str) -> dict:
    """Проверяет статус платежа через NOWPayments API."""
    if not NOWPAYMENTS_API_KEY:
        return {"error": "not_configured"}
    try:
        r = requests.get(
            f"{NP_BASE}/payment/{payment_id}",
            headers={"x-api-key": NOWPAYMENTS_API_KEY},
            timeout=8
        )
        if r.ok:
            d = r.json()
            return {
                "payment_id":     d.get("payment_id"),
                "status":         d.get("payment_status"),
                "pay_address":    d.get("pay_address"),
                "pay_amount":     d.get("pay_amount"),
                "pay_currency":   d.get("pay_currency"),
                "actually_paid":  d.get("actually_paid", 0),
            }
    except Exception as e:
        log.warning(f"[CRYPTO] get_status error: {e}")
    return {"error": "request_failed"}