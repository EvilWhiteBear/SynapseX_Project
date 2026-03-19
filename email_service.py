# -*- coding: utf-8 -*-
"""
email_service.py — Email уведомления SynapseX
──────────────────────────────────────────────
• Premium активирован — приветственное письмо
• Trial активирован — письмо с объяснением
• Trial истекает — напоминание за 1 день
• Использует SMTP (Gmail / любой SMTP)

.env переменные:
  SMTP_HOST     = smtp.gmail.com
  SMTP_PORT     = 587
  SMTP_USER     = your@gmail.com
  SMTP_PASSWORD = app_password (не основной пароль!)
  SMTP_FROM     = SynapseX <your@gmail.com>
"""
import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone

log = logging.getLogger("EMAIL")

SMTP_HOST     = os.getenv("SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER",     "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM     = os.getenv("SMTP_FROM",     f"SynapseX AI <{SMTP_USER}>")
APP_BASE_URL  = os.getenv("APP_BASE_URL",  "https://synapsex-ai.com")


def _is_configured() -> bool:
    return bool(SMTP_USER and SMTP_PASSWORD)


def _send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Отправляет HTML email. Возвращает True если успешно."""
    if not _is_configured():
        log.warning("[EMAIL] SMTP не настроен — пропускаем отправку")
        return False
    if not to_email or "@" not in to_email:
        log.warning(f"[EMAIL] Невалидный email: {to_email}")
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SMTP_FROM
        msg["To"]      = to_email
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_email, msg.as_string())

        log.info(f"[EMAIL] ✅ Отправлено: {subject} → {to_email}")
        return True
    except Exception as smtp_err:
        log.warning(f"[EMAIL] SMTP недоступен: {smtp_err} — отправляем через Telegram")
        try:
            import re as _re
            import requests as _req
            tg_token = os.getenv('TELEGRAM_TOKEN', '') or os.getenv('TG_TOKEN', '')
            admin_id = os.getenv('ADMIN_CHAT_ID', '')
            if tg_token and admin_id:
                # Убираем HTML-теги для Telegram
                body = _re.sub(r'<[^>]+>', '', html_body)
                body = _re.sub(r'\n{3,}', '\n\n', body).strip()
                tg_msg = (
                    f"📧 Email уведомление (SMTP недоступен):\n"
                    f"Кому: {to_email}\n"
                    f"Тема: {subject}\n\n"
                    f"{body[:500]}"
                )
                _req.post(
                    f"https://api.telegram.org/bot{tg_token}/sendMessage",
                    json={"chat_id": admin_id, "text": tg_msg},
                    timeout=5
                )
                log.info(f"[EMAIL] Telegram fallback отправлен admin_id={admin_id}")
                return True
        except Exception as tg_err:
            log.error(f"[EMAIL] Telegram fallback тоже не сработал: {tg_err}")
        return False


# ── Шаблоны писем ─────────────────────────────────────────────────────────────

def _base_template(content: str, title: str) -> str:
    """Базовый HTML шаблон в стиле SynapseX."""
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  body {{ margin:0; padding:0; background:#050a14; font-family:'Courier New',monospace; color:#e0e0e0; }}
  .wrap {{ max-width:600px; margin:0 auto; padding:32px 16px; }}
  .logo {{ font-size:22px; font-weight:700; color:#b8ff00; letter-spacing:4px; margin-bottom:24px; }}
  .card {{ background:#0a1525; border:1px solid rgba(184,255,0,0.15); border-radius:10px; padding:28px; }}
  h1 {{ color:#b8ff00; font-size:18px; letter-spacing:2px; margin:0 0 16px; }}
  p {{ color:#a0b0c0; font-size:13px; line-height:1.8; margin:8px 0; }}
  .highlight {{ color:#00e5ff; font-weight:700; }}
  .btn {{ display:inline-block; margin:20px 0; padding:12px 28px; background:#b8ff00;
          color:#050a14; font-weight:700; font-size:13px; letter-spacing:2px;
          text-decoration:none; border-radius:6px; }}
  .divider {{ border:none; border-top:1px solid rgba(255,255,255,0.08); margin:20px 0; }}
  .footer {{ text-align:center; font-size:11px; color:#3a4a5a; margin-top:24px; }}
  .tag {{ display:inline-block; padding:2px 8px; border:1px solid rgba(184,255,0,0.3);
          color:#b8ff00; font-size:10px; letter-spacing:1px; border-radius:3px; margin:2px; }}
</style></head>
<body><div class="wrap">
  <div class="logo">SYNAPSE X</div>
  <div class="card">
    <h1>{title}</h1>
    {content}
  </div>
  <div class="footer">
    © 2026 SynapseX AI · <a href="{APP_BASE_URL}" style="color:#3a4a5a;">synapsex-ai.com</a><br>
    Это автоматическое уведомление. Все сигналы носят исключительно информационный характер.
  </div>
</div></body></html>"""


def send_premium_activated(email: str, name: str,
                            period_end: str = "", method: str = "crypto") -> bool:
    """
    Письмо при активации Premium подписки.
    Отправляется автоматически после оплаты.
    """
    method_label = {
        "crypto": "USDT / Crypto",
        "stripe": "Visa / Mastercard",
        "admin":  "Администратор",
        "trial":  "Trial период",
    }.get(method, method)

    end_str = ""
    if period_end:
        try:
            dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            end_str = dt.strftime("%d.%m.%Y")
        except Exception:
            end_str = period_end[:10]

    greeting = f"Привет, {name}!" if name else "Привет!"

    content = f"""
    <p>{greeting}</p>
    <p>🎉 Твоя <span class="highlight">PREMIUM подписка активирована!</span></p>
    <hr class="divider">
    <p>📋 <b>Что теперь доступно:</b></p>
    <p>
      <span class="tag">⚡ 15 AI сигналов/день</span>
      <span class="tag">🤖 Авто-скринер</span>
      <span class="tag">📊 Полный анализ</span>
      <span class="tag">📱 Telegram DM</span>
    </p>
    <hr class="divider">
    <p>💳 Метод оплаты: <span class="highlight">{method_label}</span></p>
    {"<p>📅 Действует до: <span class='highlight'>" + end_str + "</span></p>" if end_str else ""}
    <p style="text-align:center">
      <a href="{APP_BASE_URL}/terminal" class="btn">→ ОТКРЫТЬ ТЕРМИНАЛ</a>
    </p>
    <hr class="divider">
    <p style="font-size:11px;color:#3a4a5a;">
      ⚠️ Все торговые сигналы носят исключительно информационный характер
      и не являются финансовым советом. Торговля криптовалютами сопряжена с рисками.
    </p>"""

    subject = "✅ SynapseX Premium активирован"
    return _send_email(email, subject, _base_template(content, "PREMIUM АКТИВИРОВАН"))


def send_trial_activated(email: str, name: str, trial_end: str) -> bool:
    """Письмо при активации 3-дневного trial."""
    try:
        dt = datetime.fromisoformat(trial_end.replace("Z", "+00:00"))
        end_str = dt.strftime("%d.%m.%Y в %H:%M UTC")
    except Exception:
        end_str = trial_end[:10]

    greeting = f"Привет, {name}!" if name else "Привет!"

    content = f"""
    <p>{greeting}</p>
    <p>🎁 Тебе активирован <span class="highlight">БЕСПЛАТНЫЙ TRIAL на 3 дня!</span></p>
    <p>Попробуй все возможности Premium без оплаты.</p>
    <hr class="divider">
    <p>📋 <b>Что доступно в Trial:</b></p>
    <p>
      <span class="tag">⚡ 15 AI сигналов/день</span>
      <span class="tag">🤖 Авто-скринер</span>
      <span class="tag">📊 Полный анализ</span>
    </p>
    <hr class="divider">
    <p>⏰ Trial истекает: <span class="highlight">{end_str}</span></p>
    <p>После окончания — автоматически вернётся Free план (2 сигнала/день).</p>
    <p style="text-align:center">
      <a href="{APP_BASE_URL}/terminal" class="btn">→ ОТКРЫТЬ ТЕРМИНАЛ</a>
    </p>
    <p style="text-align:center;margin-top:12px;">
      <a href="{APP_BASE_URL}/premium" style="color:#b8ff00;font-size:12px;">
        Оформить Premium после trial →
      </a>
    </p>"""

    subject = "🎁 SynapseX — 3 дня Premium бесплатно!"
    return _send_email(email, subject, _base_template(content, "TRIAL АКТИВИРОВАН"))


def send_trial_expiring(email: str, name: str) -> bool:
    """Напоминание за 1 день до конца trial."""
    greeting = f"Привет, {name}!" if name else "Привет!"
    content = f"""
    <p>{greeting}</p>
    <p>⏰ Твой <span class="highlight">бесплатный Trial истекает завтра!</span></p>
    <p>Чтобы продолжить пользоваться всеми возможностями Premium — оформи подписку.</p>
    <hr class="divider">
    <p>💰 <b>Premium подписка:</b> <span class="highlight">$19.99 / месяц</span></p>
    <p>Принимаем: USDT TRC20 · BTC · ETH · TON</p>
    <p style="text-align:center">
      <a href="{APP_BASE_URL}/premium" class="btn">→ ОФОРМИТЬ PREMIUM</a>
    </p>"""

    subject = "⏰ SynapseX — Trial истекает завтра"
    return _send_email(email, subject, _base_template(content, "TRIAL ИСТЕКАЕТ"))


def send_premium_expiring(email: str, name: str) -> bool:
    """Напоминание за 3 дня до конца Premium."""
    greeting = f"Привет, {name}!" if name else "Привет!"
    content = f"""
    <p>{greeting}</p>
    <p>⚠️ Твоя <span class="highlight">Premium подписка истекает через 3 дня.</span></p>
    <p>Не теряй доступ к 15 AI сигналам в день и авто-скринеру!</p>
    <p style="text-align:center">
      <a href="{APP_BASE_URL}/premium" class="btn">→ ПРОДЛИТЬ PREMIUM</a>
    </p>"""

    subject = "⚠️ SynapseX — Premium истекает через 3 дня"
    return _send_email(email, subject, _base_template(content, "ПРОДЛИ ПОДПИСКУ"))