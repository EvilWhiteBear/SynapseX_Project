# -*- coding: utf-8 -*-
"""
tests/test_payments.py — Базовые тесты платёжных модулей SynapseX

Запуск:
    pip install pytest
    pytest tests/ -v
"""
import json
import hmac
import hashlib
import sqlite3
import tempfile
import os
import pytest

# ─── Фикстуры ────────────────────────────────────────────────────────────────

TEST_UID = "usertest1"  # без подчёркиваний — NOWPayments парсит uid из order_id через split("_")

@pytest.fixture
def tmp_db():
    """Временная SQLite БД для каждого теста."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    # Минимальная схема
    with sqlite3.connect(db_path) as conn:
        conn.executescript(f"""
            CREATE TABLE users (
                uid TEXT PRIMARY KEY,
                email TEXT,
                is_premium INTEGER DEFAULT 0,
                stripe_customer_id TEXT DEFAULT '',
                premium_until TEXT DEFAULT ''
            );
            CREATE TABLE stripe_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT,
                stripe_sub_id TEXT,
                status TEXT,
                current_period_end TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE crypto_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT,
                payment_id TEXT,
                currency TEXT,
                amount_usd REAL DEFAULT 19.99,
                status TEXT DEFAULT 'pending',
                payment_url TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE payment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT,
                amount REAL,
                currency TEXT,
                method TEXT,
                status TEXT,
                created_at TEXT
            );
            INSERT INTO users (uid, email, is_premium) VALUES ('{TEST_UID}', 'test@example.com', 0);
        """)
    yield db_path
    # На Windows SQLite может держать файл — игнорируем ошибку
    try:
        os.unlink(db_path)
    except PermissionError:
        pass


# ─── Stripe: get_subscription_info ───────────────────────────────────────────

class TestStripeSubscriptionInfo:
    def test_no_subscription_returns_none_status(self, tmp_db):
        from stripe_payments import get_subscription_info
        result = get_subscription_info(tmp_db, TEST_UID)
        assert result["is_premium"] is False
        assert result["status"] == "none"

    def test_unknown_user_returns_not_premium(self, tmp_db):
        from stripe_payments import get_subscription_info
        result = get_subscription_info(tmp_db, "nonexistentuid")
        assert result["is_premium"] is False

    def test_premium_user_returns_correct_status(self, tmp_db):
        from stripe_payments import get_subscription_info
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(f"UPDATE users SET is_premium=1 WHERE uid='{TEST_UID}'")
            conn.execute(f"""
                INSERT INTO stripe_subscriptions (uid, stripe_sub_id, status, current_period_end, created_at, updated_at)
                VALUES ('{TEST_UID}', 'sub_test123', 'active', '2026-12-31T00:00:00', '2026-01-01', '2026-01-01')
            """)
        result = get_subscription_info(tmp_db, TEST_UID)
        assert result["is_premium"] is True
        assert result["status"] == "active"
        assert result["sub_id"] == "sub_test123"


# ─── Stripe: _set_premium ─────────────────────────────────────────────────────

class TestStripePremiumToggle:
    def test_activate_premium(self, tmp_db):
        from stripe_payments import _set_premium
        _set_premium(tmp_db, TEST_UID, True, "sub_abc", "2026-12-31T00:00:00")
        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(f"SELECT is_premium FROM users WHERE uid='{TEST_UID}'").fetchone()
        assert row[0] == 1

    def test_deactivate_premium(self, tmp_db):
        from stripe_payments import _set_premium
        _set_premium(tmp_db, TEST_UID, True, "sub_abc", "2026-12-31T00:00:00")
        _set_premium(tmp_db, TEST_UID, False, "sub_abc")
        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(f"SELECT is_premium FROM users WHERE uid='{TEST_UID}'").fetchone()
        assert row[0] == 0


# ─── Stripe: handle_webhook (без реального Stripe) ────────────────────────────

class TestStripeWebhook:
    def test_webhook_without_secret_returns_error(self, tmp_db):
        from stripe_payments import handle_webhook
        # STRIPE_WEBHOOK_SECRET не задан → должна вернуть ошибку
        result = handle_webhook(b'{}', "fake_sig", tmp_db)
        assert "error" in result

    def test_webhook_invalid_signature(self, tmp_db, monkeypatch):
        """Неверная подпись должна вернуть invalid_signature."""
        stripe = pytest.importorskip("stripe")  # пропускаем если stripe не установлен
        import stripe_payments
        stripe_payments.STRIPE_WEBHOOK_SECRET = "whsec_test"
        stripe_payments.STRIPE_AVAILABLE = True

        def fake_construct(payload, sig, secret):
            raise stripe.error.SignatureVerificationError("bad sig", sig)
        monkeypatch.setattr(stripe.Webhook, "construct_event", fake_construct)

        result = stripe_payments.handle_webhook(b'{"type":"test"}', "bad_sig", tmp_db)
        assert result.get("error") == "invalid_signature"


# ─── NOWPayments: verify_ipn_signature ───────────────────────────────────────

class TestNowPaymentsSignature:
    def test_empty_secret_returns_false(self):
        from nowpayments import verify_ipn_signature
        import nowpayments
        original = nowpayments.NOWPAYMENTS_IPN_SECRET
        nowpayments.NOWPAYMENTS_IPN_SECRET = ""
        result = verify_ipn_signature(b'payload', "some_hmac")
        nowpayments.NOWPAYMENTS_IPN_SECRET = original
        assert result is False

    def test_missing_hmac_returns_false(self):
        from nowpayments import verify_ipn_signature
        import nowpayments
        nowpayments.NOWPAYMENTS_IPN_SECRET = "test_secret"
        result = verify_ipn_signature(b'payload', "")
        assert result is False

    def test_correct_hmac_returns_true(self):
        from nowpayments import verify_ipn_signature
        import nowpayments
        secret = "test_ipn_secret"
        payload = b'{"payment_id":"123","payment_status":"finished"}'
        correct_hmac = hmac.new(
            secret.encode(), payload, hashlib.sha512
        ).hexdigest()
        nowpayments.NOWPAYMENTS_IPN_SECRET = secret
        assert verify_ipn_signature(payload, correct_hmac) is True

    def test_wrong_hmac_returns_false(self):
        from nowpayments import verify_ipn_signature
        import nowpayments
        nowpayments.NOWPAYMENTS_IPN_SECRET = "test_ipn_secret"
        result = verify_ipn_signature(b'payload', "wrong_hmac_value")
        assert result is False


# ─── NOWPayments: handle_crypto_webhook ──────────────────────────────────────

class TestNowPaymentsWebhook:
    def test_finished_payment_activates_premium(self, tmp_db):
        from nowpayments import handle_crypto_webhook, init_crypto_db
        init_crypto_db(tmp_db)
        # order_id: uid должен быть без подчёркиваний (парсится через split("_")[1])
        order_id = f"synapsex_{TEST_UID}_1700000000"
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(f"""
                INSERT INTO crypto_payments (uid, payment_id, currency, status, created_at, updated_at)
                VALUES ('{TEST_UID}', 'pay_finished_001', 'usdttrc20', 'pending', '2026-01-01', '2026-01-01')
            """)
        payload = {
            "payment_id": "pay_finished_001",
            "payment_status": "finished",
            "order_id": order_id,
        }
        result = handle_crypto_webhook(payload, tmp_db)
        assert result["status"] == "OK"
        assert result.get("premium") is True
        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(f"SELECT is_premium FROM users WHERE uid='{TEST_UID}'").fetchone()
        assert row[0] == 1

    def test_idempotent_already_processed(self, tmp_db):
        """Повторный webhook для уже обработанного платежа не дублирует активацию."""
        from nowpayments import handle_crypto_webhook, init_crypto_db
        init_crypto_db(tmp_db)
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(f"""
                INSERT INTO crypto_payments (uid, payment_id, currency, status, created_at, updated_at)
                VALUES ('{TEST_UID}', 'pay_dup_001', 'usdttrc20', 'finished', '2026-01-01', '2026-01-01')
            """)
        payload = {
            "payment_id": "pay_dup_001",
            "payment_status": "finished",
            "order_id": f"synapsex_{TEST_UID}_1700000000",
        }
        result = handle_crypto_webhook(payload, tmp_db)
        assert result.get("already_processed") is True

    def test_failed_payment_does_not_activate_premium(self, tmp_db):
        from nowpayments import handle_crypto_webhook, init_crypto_db
        init_crypto_db(tmp_db)
        payload = {
            "payment_id": "pay_fail_001",
            "payment_status": "failed",
            "order_id": f"synapsex_{TEST_UID}_1700000000",
        }
        result = handle_crypto_webhook(payload, tmp_db)
        assert result["status"] == "OK"
        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(f"SELECT is_premium FROM users WHERE uid='{TEST_UID}'").fetchone()
        assert row[0] == 0

    def test_uid_not_found_returns_warning(self, tmp_db):
        """order_id без префикса synapsex_ → uid пустой → UID_NOT_FOUND."""
        from nowpayments import handle_crypto_webhook, init_crypto_db
        init_crypto_db(tmp_db)
        payload = {
            "payment_id": "pay_no_uid_001",
            "payment_status": "finished",
            "order_id": "unknown_order_format",  # не начинается с synapsex_
        }
        result = handle_crypto_webhook(payload, tmp_db)
        assert result["status"] == "UID_NOT_FOUND"


# ─── NOWPayments: is_configured ──────────────────────────────────────────────

class TestNowPaymentsConfig:
    def test_not_configured_without_api_key(self):
        import nowpayments
        original = nowpayments.NOWPAYMENTS_API_KEY
        nowpayments.NOWPAYMENTS_API_KEY = ""
        assert nowpayments.is_configured() is False
        nowpayments.NOWPAYMENTS_API_KEY = original

    def test_configured_with_api_key(self):
        import nowpayments
        original = nowpayments.NOWPAYMENTS_API_KEY
        nowpayments.NOWPAYMENTS_API_KEY = "test_key_123"
        assert nowpayments.is_configured() is True
        nowpayments.NOWPAYMENTS_API_KEY = original
