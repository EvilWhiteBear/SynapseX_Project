# -*- coding: utf-8 -*-
"""
paddle_payments.py — Paddle Billing интеграция для SynapseX
────────────────────────────────────────────────────────────
Paddle = Merchant of Record (они платят налоги сами)
Поддерживает Сербию и 200+ стран.

.env переменные:
  PADDLE_API_KEY        = pdl_live_...   (API ключ из Paddle Dashboard)
  PADDLE_WEBHOOK_SECRET = pdl_ntf_...   (Webhook signing secret)
  PADDLE_PRICE_ID_USD   = pri_...       (Price ID подписки USD)
  PADDLE_PRICE_ID_EUR   = pri_...       (Price ID подписки EUR)
  APP_BASE_URL          = https://synapsex-ai.com
"""
import os
import hmac
import hashlib
import logging
import sqlite3
from datetime import datetime, timezone, timedelta

import requests

log = logging.getLogger("PADDLE")

PADDLE_API_KEY        = os.getenv("PADDLE_API_KEY",        "")
PADDLE_WEBHOOK_SECRET = os.getenv("PADDLE_WEBHOOK_SECRET", "")
PADDLE_PRICE_ID_USD   = os.getenv("PADDLE_PRICE_ID_USD",   "")
PADDLE_PRICE_ID_EUR   = os.getenv("PADDLE_PRICE_ID_EUR",   "")
APP_BASE_URL          = os.getenv("APP_BASE_URL",          "https://synapsex-ai.com")

# Paddle API base URL (Sandbox или Production)
PADDLE_SANDBOX = os.getenv("PADDLE_SANDBOX", "false").lower() == "true"
PADDLE_API_BASE = (
    "https://sandbox-api.paddle.com" if PADDLE_SANDBOX
    else "https://api.paddle.com"
)

_paddle_configured = bool(PADDLE_API_KEY and PADDLE_PRICE_ID_USD)


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {PADDLE_API_KEY}",
        "Content-Type":  "application/json",
    }


def init_paddle_db(db_path: str):
    """Создаёт таблицу paddle_subscriptions если не существует."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paddle_subscriptions (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    uid                TEXT NOT NULL,
                    paddle_sub_id      TEXT UNIQUE,
                    paddle_customer_id TEXT,
                    status             TEXT DEFAULT 'active',
                    price_id           TEXT,
                    currency           TEXT DEFAULT 'USD',
                    created_at         TEXT,
                    updated_at         TEXT,
                    next_billing       TEXT
                )
            """)
            conn.commit()
        log.info("[PADDLE] DB инициализирована ✓")
    except Exception as e:
        log.error(f"[PADDLE] DB init error: {e}")


def create_checkout_url(uid: str, email: str,
                        currency: str = "USD") -> dict:
    """
    Создаёт Paddle Checkout URL для подписки.
    Возвращает {"url": "...", "transaction_id": "..."}
    """
    if not _paddle_configured:
        return {"error": "Paddle не настроен — добавь PADDLE_API_KEY и PADDLE_PRICE_ID_USD"}

    price_id = PADDLE_PRICE_ID_EUR if currency == "EUR" else PADDLE_PRICE_ID_USD

    try:
        payload = {
            "items": [{"price_id": price_id, "quantity": 1}],
            "customer": {"email": email},
            "custom_data": {"uid": uid},
        }

        resp = requests.post(
            f"{PADDLE_API_BASE}/transactions",
            json=payload,
            headers=_headers(),
            timeout=15
        )

        if not resp.ok:
            log.error(f"[PADDLE] Checkout error: {resp.text[:200]}")
            return {"error": f"Paddle API error: {resp.status_code}"}

        data = resp.json().get("data", {})
        checkout = data.get("checkout", {})
        url = checkout.get("url", "")
        tx_id = data.get("id", "")

        log.info(f"[PADDLE] Checkout создан: {tx_id} для {uid}")
        return {"url": url, "transaction_id": tx_id}

    except Exception as e:
        log.error(f"[PADDLE] create_checkout_url error: {e}")
        return {"error": str(e)}


def verify_webhook_signature(payload_bytes: bytes,
                              signature_header: str) -> bool:
    """Проверяет подпись входящего Paddle webhook."""
    if not PADDLE_WEBHOOK_SECRET:
        return True  # если секрет не задан — пропускаем проверку (dev mode)
    try:
        # Paddle webhook signature format:
        # ts=timestamp;h1=signature
        parts = dict(p.split("=", 1) for p in signature_header.split(";"))
        ts    = parts.get("ts", "")
        h1    = parts.get("h1", "")

        signed_payload = f"{ts}:{payload_bytes.decode()}"
        expected = hmac.new(
            PADDLE_WEBHOOK_SECRET.encode(),
            signed_payload.encode(),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(expected, h1)
    except Exception as e:
        log.error(f"[PADDLE] Signature verify error: {e}")
        return False


def handle_paddle_webhook(payload: dict, db_path: str) -> dict:
    """
    Обрабатывает входящий Paddle webhook.
    Основные события:
    - transaction.completed     → активируем Premium
    - subscription.activated    → подписка активна
    - subscription.canceled     → отменяем Premium
    - subscription.past_due     → платёж просрочен
    """
    event_type = payload.get("event_type", "")
    data       = payload.get("data", {})

    log.info(f"[PADDLE] Webhook: {event_type}")

    # ── Успешная транзакция (первый платёж) ─────────────────────────────────
    if event_type == "transaction.completed":
        uid     = (data.get("custom_data") or {}).get("uid", "")
        sub_id  = data.get("subscription_id", "")
        cust_id = data.get("customer_id", "")

        if not uid:
            log.warning("[PADDLE] transaction.completed без uid")
            return {"status": "no_uid"}

        _activate_premium(db_path, uid, sub_id, cust_id)
        return {"status": "OK", "uid": uid, "premium": True}

    # ── Подписка активирована ────────────────────────────────────────────────
    elif event_type == "subscription.activated":
        sub_id  = data.get("id", "")
        cust_id = data.get("customer_id", "")
        uid     = (data.get("custom_data") or {}).get("uid", "")
        next_b  = data.get("next_billed_at", "")

        if uid:
            _activate_premium(db_path, uid, sub_id, cust_id, next_b)
        return {"status": "OK"}

    # ── Подписка отменена ────────────────────────────────────────────────────
    elif event_type in ("subscription.canceled", "subscription.paused"):
        sub_id = data.get("id", "")
        _deactivate_by_sub_id(db_path, sub_id)
        return {"status": "OK"}

    # ── Платёж не прошёл ────────────────────────────────────────────────────
    elif event_type == "subscription.past_due":
        log.warning(f"[PADDLE] Платёж просрочен: {data.get('id')}")
        return {"status": "OK"}

    return {"status": "ignored", "event": event_type}


def _activate_premium(db_path: str, uid: str,
                       sub_id: str = "", cust_id: str = "",
                       next_billing: str = ""):
    """Активирует Premium в БД пользователей."""
    try:
        now = datetime.now(timezone.utc)
        period_end = (now + timedelta(days=30)).isoformat()

        with sqlite3.connect(db_path) as conn:
            # Обновляем users
            conn.execute("""
                UPDATE users
                SET is_premium=1, premium_until=?
                WHERE uid=?
            """, (period_end, uid))

            # Сохраняем подписку
            conn.execute("""
                INSERT OR REPLACE INTO paddle_subscriptions
                (uid, paddle_sub_id, paddle_customer_id, status,
                 created_at, updated_at, next_billing)
                VALUES (?,?,?,'active',?,?,?)
            """, (uid, sub_id, cust_id,
                  now.isoformat(), now.isoformat(), next_billing))
            conn.commit()

        log.info(f"[PADDLE] ✅ Premium активирован: {uid} до {period_end[:10]}")
    except Exception as e:
        log.error(f"[PADDLE] _activate_premium error: {e}")


def _deactivate_by_sub_id(db_path: str, sub_id: str):
    """Деактивирует Premium по paddle_sub_id."""
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT uid FROM paddle_subscriptions WHERE paddle_sub_id=?",
                (sub_id,)
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE users SET is_premium=0 WHERE uid=?", (row[0],)
                )
                conn.execute(
                    "UPDATE paddle_subscriptions SET status='canceled' WHERE paddle_sub_id=?",
                    (sub_id,)
                )
                conn.commit()
                log.info(f"[PADDLE] Premium деактивирован: {row[0]}")
    except Exception as e:
        log.error(f"[PADDLE] _deactivate error: {e}")


def get_paddle_status(uid: str, db_path: str) -> dict:
    """Возвращает статус Paddle подписки для пользователя."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT ps.*, u.is_premium, u.premium_until
                   FROM paddle_subscriptions ps
                   JOIN users u ON ps.uid = u.uid
                   WHERE ps.uid=?
                   ORDER BY ps.id DESC LIMIT 1""",
                (uid,)
            ).fetchone()
        if not row:
            return {"has_subscription": False}
        return {
            "has_subscription": True,
            "status":           row["status"],
            "next_billing":     row["next_billing"] or "",
            "is_premium":       bool(row["is_premium"]),
            "premium_until":    row["premium_until"] or "",
        }
    except Exception as e:
        log.error(f"[PADDLE] get_status error: {e}")
        return {"has_subscription": False}
