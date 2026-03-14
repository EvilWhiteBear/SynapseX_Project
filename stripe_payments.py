# -*- coding: utf-8 -*-
"""
stripe_payments.py — Stripe подписки для Synapse X
────────────────────────────────────────────────────
• Создание Checkout Session (оформление подписки)
• Webhook — автоматическая активация/деактивация Premium
• Управление подпиской через Customer Portal
• Поддержка USD и EUR
────────────────────────────────────────────────────
Нужно установить: pip install stripe
Переменные в .env:
  STRIPE_SECRET_KEY      = sk_live_...
  STRIPE_WEBHOOK_SECRET  = whsec_...
  STRIPE_PRICE_ID_USD    = price_xxx  (Monthly $19.99 USD)
  STRIPE_PRICE_ID_EUR    = price_yyy  (Monthly €19.99 EUR)
  APP_BASE_URL           = https://yourdomain.com
"""

import os
import sqlite3
import logging
from datetime import datetime, timezone

log = logging.getLogger("STRIPE")

try:
    import stripe
    STRIPE_AVAILABLE = True
except ImportError:
    STRIPE_AVAILABLE = False
    log.warning("[STRIPE] stripe не установлен. pip install stripe")

# ── Конфиг ────────────────────────────────────────────────────────────────────
STRIPE_SECRET_KEY     = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_ID_USD   = os.getenv("STRIPE_PRICE_ID_USD", "")
STRIPE_PRICE_ID_EUR   = os.getenv("STRIPE_PRICE_ID_EUR", "")
APP_BASE_URL          = os.getenv("APP_BASE_URL", "https://synapsex-ai.com")

# Цены для отображения на странице
PLAN_PRICE_USD = "19.99"
PLAN_PRICE_EUR = "19.99"
PLAN_SIGNALS   = 15   # сигналов/день для Premium

if STRIPE_AVAILABLE and STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


# ── Инициализация таблицы подписок ───────────────────────────────────────────

def init_stripe_db(db_path: str):
    """Создаёт таблицу stripe_subscriptions если не существует."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stripe_subscriptions (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                uid                 TEXT NOT NULL,
                stripe_customer_id  TEXT,
                stripe_sub_id       TEXT,
                status              TEXT DEFAULT 'inactive',
                currency            TEXT DEFAULT 'usd',
                current_period_end  TEXT,
                created_at          TEXT,
                updated_at          TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE users ADD COLUMN stripe_customer_id TEXT DEFAULT ''")
        except Exception:
            pass
        conn.commit()
    log.info("[STRIPE] БД инициализирована")


# ── Вспомогательные функции ───────────────────────────────────────────────────

def _get_or_create_customer(uid: str, email: str, name: str, db_path: str) -> str:
    """Возвращает Stripe Customer ID, создаёт если нет."""
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT stripe_customer_id FROM users WHERE uid=?", (uid,)
        ).fetchone()
        if row and row[0]:
            return row[0]

    # Создаём нового клиента в Stripe
    customer = stripe.Customer.create(
        email=email,
        name=name,
        metadata={"uid": uid}
    )
    cid = customer.id

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE users SET stripe_customer_id=? WHERE uid=?", (cid, uid)
        )
        conn.commit()

    log.info(f"[STRIPE] Создан Customer {cid} для {uid}")
    return cid


def _set_premium(db_path: str, uid: str, active: bool,
                 sub_id: str = "", period_end: str = ""):
    """Устанавливает/снимает Premium в БД."""
    ts = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE users SET is_premium=? WHERE uid=?",
            (1 if active else 0, uid)
        )
        if sub_id:
            conn.execute("""
                INSERT INTO stripe_subscriptions
                    (uid, stripe_sub_id, status, current_period_end, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, (uid, sub_id, "active" if active else "cancelled", period_end, ts, ts))
            conn.execute("""
                UPDATE stripe_subscriptions
                SET status=?, current_period_end=?, updated_at=?
                WHERE uid=? AND stripe_sub_id=?
            """, ("active" if active else "cancelled", period_end, ts, uid, sub_id))
        conn.commit()
    log.info(f"[STRIPE] Premium {'включён' if active else 'отключён'}: {uid}")


def get_subscription_info(db_path: str, uid: str) -> dict:
    """Возвращает информацию о подписке пользователя."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        sub = conn.execute("""
            SELECT * FROM stripe_subscriptions
            WHERE uid=? ORDER BY id DESC LIMIT 1
        """, (uid,)).fetchone()
        user = conn.execute(
            "SELECT is_premium, stripe_customer_id FROM users WHERE uid=?", (uid,)
        ).fetchone()

    is_prem = bool(user[0]) if user else False
    cid     = user[1] if user else ""
    if sub:
        return {
            "is_premium":         is_prem,
            "status":             sub["status"],
            "current_period_end": sub["current_period_end"],
            "stripe_customer_id": cid,
            "sub_id":             sub["stripe_sub_id"],
        }
    return {"is_premium": is_prem, "status": "none", "stripe_customer_id": cid}


# ── Checkout ──────────────────────────────────────────────────────────────────

def create_checkout_session(uid: str, email: str, name: str,
                             currency: str, db_path: str) -> dict:
    """
    Создаёт Stripe Checkout Session.
    currency: 'usd' или 'eur'
    Возвращает {"url": "https://checkout.stripe.com/..."} или {"error": "..."}
    """
    if not STRIPE_AVAILABLE:
        return {"error": "Stripe не установлен на сервере"}
    if not STRIPE_SECRET_KEY:
        return {"error": "STRIPE_SECRET_KEY не задан в .env"}

    price_id = STRIPE_PRICE_ID_USD if currency == "usd" else STRIPE_PRICE_ID_EUR
    if not price_id:
        key_name = "STRIPE_PRICE_ID_USD" if currency == "usd" else "STRIPE_PRICE_ID_EUR"
        return {"error": f"{key_name} не задан в .env"}

    try:
        customer_id = _get_or_create_customer(uid, email, name, db_path)
        session = stripe.checkout.Session.create(
            customer=customer_id,
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            success_url=f"{APP_BASE_URL}/premium/success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{APP_BASE_URL}/premium?cancelled=1",
            metadata={"uid": uid},
            subscription_data={"metadata": {"uid": uid}},
            allow_promotion_codes=True,
        )
        log.info(f"[STRIPE] Checkout session создана для {uid}: {session.id}")
        return {"url": session.url, "session_id": session.id}
    except stripe.error.StripeError as e:
        log.error(f"[STRIPE] Checkout error: {e}")
        return {"error": str(e)}


# ── Customer Portal ───────────────────────────────────────────────────────────

def create_portal_session(uid: str, db_path: str) -> dict:
    """Создаёт ссылку на Stripe Customer Portal (управление подпиской)."""
    if not STRIPE_AVAILABLE or not STRIPE_SECRET_KEY:
        return {"error": "Stripe недоступен"}
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT stripe_customer_id FROM users WHERE uid=?", (uid,)
            ).fetchone()
        if not row or not row[0]:
            return {"error": "Подписка не найдена"}

        portal = stripe.billing_portal.Session.create(
            customer=row[0],
            return_url=f"{APP_BASE_URL}/terminal"
        )
        return {"url": portal.url}
    except stripe.error.StripeError as e:
        log.error(f"[STRIPE] Portal error: {e}")
        return {"error": str(e)}


# ── Webhook ───────────────────────────────────────────────────────────────────

def handle_webhook(payload: bytes, sig_header: str, db_path: str) -> dict:
    """
    Обрабатывает Stripe Webhook.
    Вызывать из Flask route: handle_webhook(request.data, request.headers.get('Stripe-Signature'), DB_PATH)
    """
    if not STRIPE_AVAILABLE or not STRIPE_WEBHOOK_SECRET:
        return {"error": "Stripe webhook не настроен"}

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        log.error("[STRIPE] Неверная подпись webhook")
        return {"error": "invalid_signature"}
    except Exception as e:
        log.error(f"[STRIPE] Webhook parse error: {e}")
        return {"error": str(e)}

    etype = event["type"]
    data  = event["data"]["object"]
    log.info(f"[STRIPE] Webhook: {etype}")

    # ── Подписка активирована / обновлена ──────────────────────────────────
    if etype in ("customer.subscription.created", "customer.subscription.updated"):
        sub    = data
        status = sub.get("status")  # active, trialing, past_due, etc.
        uid    = sub.get("metadata", {}).get("uid", "")
        sub_id = sub.get("id", "")
        period_end = datetime.fromtimestamp(
            sub.get("current_period_end", 0), tz=timezone.utc
        ).isoformat() if sub.get("current_period_end") else ""

        if not uid:
            # Ищем uid через customer
            cid = sub.get("customer", "")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT uid FROM users WHERE stripe_customer_id=?", (cid,)
                ).fetchone()
            uid = row[0] if row else ""

        if uid:
            is_active = status in ("active", "trialing")
            _set_premium(db_path, uid, is_active, sub_id, period_end)

    # ── Подписка отменена / истекла ─────────────────────────────────────────
    elif etype == "customer.subscription.deleted":
        sub    = data
        uid    = sub.get("metadata", {}).get("uid", "")
        sub_id = sub.get("id", "")
        if not uid:
            cid = sub.get("customer", "")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT uid FROM users WHERE stripe_customer_id=?", (cid,)
                ).fetchone()
            uid = row[0] if row else ""
        if uid:
            _set_premium(db_path, uid, False, sub_id)

    # ── Платёж прошёл ───────────────────────────────────────────────────────
    elif etype == "invoice.payment_succeeded":
        cid = data.get("customer", "")
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT uid FROM users WHERE stripe_customer_id=?", (cid,)
            ).fetchone()
        if row:
            log.info(f"[STRIPE] Платёж получен от {row[0]}")

    # ── Платёж не прошёл ────────────────────────────────────────────────────
    elif etype == "invoice.payment_failed":
        cid = data.get("customer", "")
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT uid FROM users WHERE stripe_customer_id=?", (cid,)
            ).fetchone()
        if row:
            log.warning(f"[STRIPE] Платёж не прошёл от {row[0]}")

    return {"status": "OK", "event": etype}
