# -*- coding: utf-8 -*-
"""
screener.py — AutoScreener v2
──────────────────────────────────────────────────────────────────────────────
• Сканирует топ фьючерсные активы MEXC каждые 30 минут
• 2-этапная фильтрация: быстрый скоринг → полный AI анализ
• Публикует лучшие сигналы в Telegram-бот @SynapseX_ProjectBot
• Отправляет Premium сигналы в личку подписчикам
• Новостной контекст (CryptoPanic, если ключ задан)
• Дисклеймер на каждом сообщении
• Импорт analysis.py: поддерживает оба расположения (корень / logic/)

⚠️ Все сигналы — исключительно информационный характер.
   НЕ являются финансовым советом или рекомендацией к торговле.
"""
import sys
import os
import threading
import time
import json
import logging
import sqlite3
import requests
from datetime import datetime, timezone

logger = logging.getLogger("SYNAPSE_SCREENER")

# ── Путь к analysis.py ────────────────────────────────────────────────────────
_base = os.path.dirname(os.path.abspath(__file__))
if _base not in sys.path:
    sys.path.insert(0, _base)


# ══════════════════════════════════════════════════════════════════════════════
#  СПИСОК АКТИВОВ ДЛЯ СКРИНИНГА
#  Топ фьючерсные пары MEXC — обновлён, включает актуальные активы
# ══════════════════════════════════════════════════════════════════════════════

SCREENER_ASSETS = [
    'BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT', 'XRP-USDT',
    'ADA-USDT', 'DOGE-USDT', 'AVAX-USDT', 'DOT-USDT', 'MATIC-USDT',
    'LINK-USDT', 'UNI-USDT', 'ATOM-USDT', 'LTC-USDT', 'ETC-USDT',
    'FIL-USDT', 'NEAR-USDT', 'APT-USDT', 'ARB-USDT', 'OP-USDT',
    'SUI-USDT', 'INJ-USDT', 'TIA-USDT', 'SEI-USDT', 'JTO-USDT',
    'WIF-USDT', 'BONK-USDT', 'PEPE-USDT', 'FLOKI-USDT', 'SHIB-USDT',
    'BCH-USDT', 'TRX-USDT', 'TON-USDT', 'SAND-USDT', 'MANA-USDT',
    'AXS-USDT', 'GALA-USDT', 'ENJ-USDT', 'CHZ-USDT', 'FLOW-USDT',
    'ICP-USDT', 'FTM-USDT', 'ALGO-USDT', 'VET-USDT', 'THETA-USDT',
    'EGLD-USDT', 'XLM-USDT', 'XMR-USDT', 'EOS-USDT', 'IOTA-USDT',
    'ZEC-USDT', 'DASH-USDT', 'NEO-USDT', 'WAVES-USDT', 'KAVA-USDT',
    'GMT-USDT', 'STX-USDT', 'CFX-USDT', 'BLUR-USDT', 'MAGIC-USDT',
    'DYDX-USDT', 'IMX-USDT', 'LDO-USDT', 'CRV-USDT', 'AAVE-USDT',
    'MKR-USDT', 'SNX-USDT', 'COMP-USDT', 'YFI-USDT', 'SUSHI-USDT',
    '1000SHIB-USDT', '1000PEPE-USDT', '1000BONK-USDT', '1000FLOKI-USDT',
]

# ── Дисклеймер ────────────────────────────────────────────────────────────────
DISCLAIMER = (
    "\n\n⚠️ <i>Данный сигнал носит исключительно информационный характер "
    "и НЕ является финансовым советом, инвестиционной рекомендацией или "
    "призывом к торговле. Торговля криптовалютами сопряжена с высоким риском. "
    "Принимайте торговые решения самостоятельно и с умом.</i>"
)

DISCLAIMER_SHORT = "⚠️ <i>Не является финансовым советом. Торгуйте осознанно.</i>"


# ══════════════════════════════════════════════════════════════════════════════
#  НОВОСТНОЙ КОНТЕКСТ (CryptoPanic — если ключ задан в .env)
# ══════════════════════════════════════════════════════════════════════════════

def get_news_context(asset: str, cryptopanic_key: str, groq_key: str) -> str:
    """
    Получает топ-3 новости и AI-оценку тональности.
    Возвращает "" если ключ не задан.
    """
    if not cryptopanic_key:
        return ""
    coin = asset.replace("-USDT", "").replace("-USDC", "")
    try:
        resp = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={"auth_token": cryptopanic_key, "currencies": coin,
                    "kind": "news", "filter": "hot", "limit": 5},
            timeout=8
        )
        data = resp.json()
        posts = data.get("results", [])[:3]
        if not posts:
            return ""
        titles = "\n".join([f"- {p.get('title', '')}" for p in posts])

        if not groq_key:
            return f"\n📰 <b>НОВОСТИ {coin}:</b>\n{titles}\n"

        news_prompt = f"""Проанализируй новости о {coin} и дай КРАТКУЮ оценку в 1-2 предложения:
{titles}

Формат:
Тон: [ПОЗИТИВНЫЙ/НЕГАТИВНЫЙ/НЕЙТРАЛЬНЫЙ]
Влияние: [1 предложение о влиянии на цену {coin}]"""

        ai_resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": news_prompt}],
                  "temperature": 0.1, "max_tokens": 120},
            headers={"Authorization": f"Bearer {groq_key}",
                     "Content-Type": "application/json"},
            timeout=10
        )
        ai_resp.raise_for_status()
        summary = ai_resp.json()['choices'][0]['message']['content'].strip()
        return f"\n\n📰 <b>НОВОСТНОЙ ФОН {coin}:</b>\n{summary}\n<i>Источник: CryptoPanic</i>"
    except Exception as e:
        logger.warning(f"[NEWS] {asset}: {e}")
        return ""


# ══════════════════════════════════════════════════════════════════════════════
#  БЫСТРЫЙ МАТЕМАТИЧЕСКИЙ СКОРИНГ (без AI)
# ══════════════════════════════════════════════════════════════════════════════

def _quick_score(asset: str, collector) -> dict | None:
    """
    Быстрый скоринг без AI. Используется для первичной фильтрации.
    Возвращает dict с score или None если данных нет.
    """
    try:
        live = collector.get_live_metrics(asset)
        if not live or live.get('status') == 'OFFLINE' or not live.get('price'):
            return None

        h1  = collector.get_historical_klines(asset, '1h',  limit=60)
        h4  = collector.get_historical_klines(asset, '4h',  limit=40)

        if not h1 or len(h1) < 20:
            return None

        # Импортируем QuantumAnalyzer (поддерживает оба расположения)
        try:
            from logic.analysis import QuantumAnalyzer
        except ImportError:
            try:
                from analysis import QuantumAnalyzer
            except ImportError as ie:
                logger.warning(f"[QUICK_SCORE] analysis module not found: {ie}")
                return None

        qa  = QuantumAnalyzer()
        c1  = [float(k['close']) for k in h1]
        c4  = [float(k['close']) for k in h4] if h4 else []
        h1_high = [float(k['high']) for k in h1]
        h1_low  = [float(k['low'])  for k in h1]
        h1_vol  = [float(k.get('volume', 0)) for k in h1]

        rsi_1h   = qa._rsi(c1)
        rsi_4h   = qa._rsi(c4) if c4 else 50
        macd_1h  = qa._macd(c1)
        trend_1h = qa._trend(c1)
        trend_4h = qa._trend(c4) if c4 else "SIDEWAYS"
        obv_tr   = qa._obv_trend(c1, h1_vol)
        vol_spike= qa._volume_spike(h1_vol)
        atr      = qa._atr(h1_high, h1_low, c1)

        score = 0
        # Бычьи факторы
        if trend_1h == "UPTREND":           score += 2
        if trend_4h == "UPTREND":           score += 2
        if rsi_1h < 35:                     score += 3
        elif rsi_1h < 45:                   score += 1
        if macd_1h['cross'] == 'BULLISH':   score += 2
        if "bullish" in obv_tr:             score += 1
        if vol_spike:                       score += 1
        # Медвежьи факторы
        if trend_1h == "DOWNTREND":         score -= 2
        if trend_4h == "DOWNTREND":         score -= 2
        if rsi_1h > 65:                     score -= 3
        elif rsi_1h > 55:                   score -= 1
        if macd_1h['cross'] == 'BEARISH':   score -= 2
        if "bearish" in obv_tr:             score -= 1
        if vol_spike and trend_1h == "DOWNTREND": score -= 1

        direction = "LONG" if score > 0 else "SHORT"

        return {
            "asset":      asset,
            "score":      score,
            "direction":  direction,
            "price":      float(live['price']),
            "rsi_1h":     rsi_1h,
            "rsi_4h":     rsi_4h,
            "trend_1h":   trend_1h,
            "trend_4h":   trend_4h,
            "macd_cross": macd_1h['cross'],
            "obv":        obv_tr,
            "atr":        atr,
            "vol_spike":  vol_spike,
        }
    except Exception as e:
        logger.warning(f"[QUICK_SCORE] {asset}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  ПОЛНЫЙ AI АНАЛИЗ
# ══════════════════════════════════════════════════════════════════════════════

def _full_ai_signal(asset: str, collector, analyzer,
                    groq_key: str, primary_tf: str = "1h",
                    leverage: int = 20, margin: float = 100) -> dict | None:
    """Полный анализ: MTF + QuantumAnalyzer + Groq AI."""
    try:
        live = collector.get_live_metrics(asset)
        if not live or not live.get('price'):
            return None

        timeframes = ['15m', '1h', '4h', '1d']
        mtf = {}
        for tf in timeframes:
            try:
                d = collector.get_historical_klines(asset, tf, limit=100)
                if d and len(d) >= 10:
                    mtf[tf] = d
            except Exception as e:
                logger.warning(f"[SCREENER_MTF] {asset} {tf}: {e}")

        if not mtf:
            return None

        signal = analyzer.calculate_signal(mtf, leverage, margin, primary_tf)
        if not signal or signal.get('entry', 0) == 0:
            return None

        # Neural Consensus для высококонфидентных сигналов (bullish_score >= 7)
        if signal.get('bullish_score', 0) >= 7:
            try:
                from ai_consensus import get_consensus as _screener_consensus
                signal.setdefault('price', float(live.get('price', 0)))
                consensus = _screener_consensus(signal, asset)
                if consensus and consensus.get('direction') in ('LONG', 'SHORT'):
                    signal['direction'] = consensus['direction']
                    signal['consensus_label'] = consensus.get('label', '')
                    signal['consensus_vote']  = consensus.get('vote', '')
                    if consensus.get('sl'):  signal['sl']  = consensus['sl']
                    if consensus.get('tp1'): signal['tp1'] = consensus['tp1']
                    if consensus.get('tp2'): signal['tp2'] = consensus['tp2']
                    if consensus.get('tp3'): signal['tp3'] = consensus['tp3']
                    logger.info(f"[SCREENER_CONSENSUS] {asset}: "
                                f"{consensus['direction']} {consensus.get('label','')}")
            except Exception as _ce:
                logger.warning(f"[SCREENER_CONSENSUS] {asset}: {_ce}")

        _tf    = primary_tf.upper()
        _con   = signal.get('confidence', {})
        _his   = signal.get('historical', {})
        _bos   = signal.get('bos', {})
        _sr    = signal.get('sr_levels', {})
        _fib   = signal.get('fibo', {})
        _pts   = signal.get('patterns', [])
        _swp   = signal.get('liquidity_sweep', 'none')
        _atr   = signal.get('atr', 0)
        _wpr   = signal.get('williams_r', -50)
        _stoch = signal.get('stochastic', {})
        _tp1p  = signal.get('tp1_pct', 0)
        _tp2p  = signal.get('tp2_pct', 0)
        _slp   = signal.get('sl_pct', 0)
        _rr1   = signal.get('rr_ratio', '1:2.5')
        _rr2   = signal.get('rr2_ratio', '1:5')
        _conf_sc  = _con.get('score', 0)
        _conf_gr  = _con.get('grade', '')
        _reasons  = "; ".join(_con.get('reasons', [])[:5])
        _pats_txt = ", ".join(f"{p['name']}({p['bias']})" for p in _pts[:4]) or "не найдены"

        # MTF дайджест для промпта
        _m = signal.get('mtf_data', {})
        _mtf_lines = []
        for _t in ['1d', '4h', '1h', '15m']:
            _d = _m.get(_t)
            if not _d: continue
            _mtf_lines.append(
                f"{_t}: trend={_d.get('trend')} rsi={_d.get('rsi',50):.1f} "
                f"macd={_d.get('macd_cross')} obv={_d.get('obv_trend','?')} "
                f"stoch={_d.get('stochastic',{}).get('signal','?')}"
            )
        _mtf_txt = "\n".join(_mtf_lines) or "нет данных"

        prompt = f"""Ты — SynapseX Quantum Core. Напиши торговый сигнал для Telegram на русском. Без HTML, без markdown.

АКТИВ: {asset} | ЦЕНА: {live['price']} | ТФ: {_tf}
НАПРАВЛЕНИЕ: {signal['direction']} | ДОВЕРИЕ: {_conf_sc}% {_conf_gr}
ПРИЧИНЫ СИГНАЛА: {_reasons}

УРОВНИ:
ВХОД={signal['entry']}
TP1={signal['tp1']} (+{_tp1p}% при {leverage}x) | RR {_rr1}
TP2={signal['tp2']} (+{_tp2p}% при {leverage}x) | RR {_rr2}
SL={signal['sl']} (-{_slp}%)

ТЕХНИЧЕСКИЙ АНАЛИЗ:
ATR={_atr:.4f} | Williams%R={_wpr:.0f} | Stoch={_stoch.get('k',50):.0f}/{_stoch.get('d',50):.0f} ({_stoch.get('signal','?')})
FIB 0.618={_fib.get('0.618')} | 0.786={_fib.get('0.786')}
SR Поддержки={_sr.get('supports',[])} | Сопротивления={_sr.get('resistances',[])}
BOS={_bos.get('bos')} | ChoCH={_bos.get('choch')} | Sweep={_swp}
Паттерны: {_pats_txt}
История: {_his.get('zone')} | от ATH -{_his.get('from_ath_pct')}%

MTF:
{_mtf_txt}

СТРОГИЙ ФОРМАТ ОТВЕТА:
ВХОД В ПОЗИЦИЮ: {signal['direction']}
ТОЧКА ВХОДА: {signal['entry']}
ТЕЙК-ПРОФИТ 1: {signal['tp1']} (+{_tp1p}% при {leverage}x) | RR {_rr1}
ТЕЙК-ПРОФИТ 2: {signal['tp2']} (+{_tp2p}% при {leverage}x) | RR {_rr2}
СТОП-ЛОСС: {signal['sl']} (-{_slp}%) | Инвалидация: [уровень]
УРОВЕНЬ ДОВЕРИЯ: {_conf_sc}% {_conf_gr}
аналитика: [7-8 предложений: 1)истор.контекст ATH/zone 2)тренд старших ТФ 3)RSI/MACD/OBV с числами 4)Fibonacci 5)паттерны 6)SMC BOS/FVG/Sweep 7)точные S/R уровни 8)условие инвалидации]"""

        ai_resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1, "max_tokens": 700},
            headers={"Authorization": f"Bearer {groq_key}",
                     "Content-Type": "application/json"},
            timeout=25
        )
        ai_resp.raise_for_status()
        ai_text = ai_resp.json()['choices'][0]['message']['content'].strip()

        signal['ai_text']  = ai_text
        signal['asset']    = asset
        signal['price']    = live['price']
        signal['leverage'] = leverage
        signal['tf']       = primary_tf
        return signal

    except Exception as e:
        logger.error(f"[FULL_AI] {asset}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  ФОРМАТИРОВАНИЕ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════════════════════════

def _format_channel_message(signal: dict, news_ctx: str, is_best: bool = False) -> str:
    """Форматирует сигнал для публикации в Telegram канал/бот."""
    asset   = signal['asset']
    price   = signal['price']
    direct  = signal['direction']
    entry   = signal['entry']
    tp1, tp2, sl = signal['tp1'], signal['tp2'], signal['sl']
    tp1p    = signal.get('tp1_pct', 0)
    tp2p    = signal.get('tp2_pct', 0)
    slp     = signal.get('sl_pct', 0)
    rr1     = signal.get('rr_ratio', '1:2.5')
    rr2     = signal.get('rr2_ratio', '1:5')
    lev     = signal.get('leverage', 20)
    tf      = signal.get('tf', '1h').upper()
    conf    = signal.get('confidence', {})
    conf_sc = conf.get('score', 0)
    conf_gr = conf.get('grade', '')
    hist    = signal.get('historical', {})
    ai_txt  = signal.get('ai_text', '')
    bos     = signal.get('bos', {})
    pts     = signal.get('patterns', [])
    stoch   = signal.get('stochastic', {})
    wpr     = signal.get('williams_r', -50)

    dir_emoji = "🟢 LONG" if direct == "LONG" else "🔴 SHORT"
    conf_bar  = "█" * (int(conf_sc) // 10) + "░" * (10 - int(conf_sc) // 10)

    pts_str = ""
    if pts:
        pts_str = "\n🔷 <b>ПАТТЕРНЫ:</b> " + ", ".join(p['name'] for p in pts[:3])

    bos_str = ""
    if bos.get('bos') not in ('none', None, ''):
        bos_str = f"\n⛓ <b>SMC:</b> {bos.get('bos')} {bos.get('choch','')}"

    hist_str = ""
    if hist.get('zone'):
        hist_str = f"\n📊 <b>ИСТОРИЯ:</b> {hist.get('zone')} | от ATH -{hist.get('from_ath_pct',0)}%"

    stoch_str = ""
    if stoch.get('signal') not in ('NEUTRAL', None, ''):
        stoch_str = f"\n📈 <b>STOCH:</b> {stoch.get('k',0):.0f}/{stoch.get('d',0):.0f} ({stoch.get('signal','')})"

    best_badge = "🏆 <b>ЛУЧШИЙ СИГНАЛ</b>\n" if is_best else ""
    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")

    msg = (
        f"{best_badge}"
        f"⚡ <b>SYNAPSE X — NEURAL SIGNAL</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{asset}</b>  |  💲 {price}  |  ⏱ {tf}\n"
        f"📍 {dir_emoji}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>ТОЧКА ВХОДА:</b> <code>{entry}</code>\n"
        f"✅ <b>ТЕЙК-ПРОФИТ 1:</b> <code>{tp1}</code>  +{tp1p}%  |  RR {rr1}\n"
        f"✅ <b>ТЕЙК-ПРОФИТ 2:</b> <code>{tp2}</code>  +{tp2p}%  |  RR {rr2}\n"
        f"❌ <b>СТОП-ЛОСС:</b>    <code>{sl}</code>  -{slp}%\n"
        f"📐 <b>Плечо:</b> {lev}x\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>ДОВЕРИЕ:</b> {conf_sc}%  {conf_gr}\n"
        f"[{conf_bar}]\n"
        f"{pts_str}{bos_str}{hist_str}{stoch_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 <b>АНАЛИТИКА:</b>\n{ai_txt}"
        f"{news_ctx}"
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {now_str}\n"
        f"🤖 Powered by @SynapseX_ProjectBot"
        f"{DISCLAIMER}"
    )
    return msg


def _format_premium_dm(signal: dict, news_ctx: str, user_name: str = "") -> str:
    """Форматирует сигнал для личного сообщения Premium пользователю."""
    asset  = signal['asset']
    direct = signal['direction']
    entry  = signal['entry']
    tp1, tp2, sl = signal['tp1'], signal['tp2'], signal['sl']
    tp1p   = signal.get('tp1_pct', 0)
    tp2p   = signal.get('tp2_pct', 0)
    slp    = signal.get('sl_pct', 0)
    rr1    = signal.get('rr_ratio', '1:2.5')
    rr2    = signal.get('rr2_ratio', '1:5')
    lev    = signal.get('leverage', 20)
    tf     = signal.get('tf', '1h').upper()
    conf   = signal.get('confidence', {})
    conf_sc = conf.get('score', 0)
    conf_gr = conf.get('grade', '')
    ai_txt  = signal.get('ai_text', '')

    greeting  = f"👋 {user_name}, " if user_name else ""
    dir_emoji = "🟢" if direct == "LONG" else "🔴"

    return (
        f"💎 <b>PREMIUM СИГНАЛ</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{greeting}новый сигнал от SynapseX:\n\n"
        f"🪙 <b>{asset}</b> | {dir_emoji} <b>{direct}</b> | ⏱ {tf}\n\n"
        f"🎯 Вход: <code>{entry}</code>\n"
        f"✅ TP1: <code>{tp1}</code>  (+{tp1p}%,  RR {rr1})\n"
        f"✅ TP2: <code>{tp2}</code>  (+{tp2p}%,  RR {rr2})\n"
        f"❌ SL:  <code>{sl}</code>   (-{slp}%)\n"
        f"📐 Плечо: {lev}x\n\n"
        f"🧠 Доверие: {conf_sc}%  {conf_gr}\n\n"
        f"📝 {ai_txt}"
        f"{news_ctx}"
        f"\n━━━━━━━━━━━━━━━━━━\n"
        f"{DISCLAIMER_SHORT}\n"
        f"🌐 synapsex-ai.com | @SynapseX_ProjectBot"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def _get_premium_telegram_users(db_path: str) -> list:
    """Premium пользователи с привязанным Telegram."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT uid, name, telegram_chat_id
                FROM users
                WHERE is_premium = 1
                  AND telegram_chat_id IS NOT NULL
                  AND telegram_chat_id != ''
                  AND is_revoked = 0
            """).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"[PREMIUM_USERS] {e}")
        return []


def _save_screener_signal(db_path: str, asset: str, signal: dict, ai_text: str):
    """Сохраняет сигнал скринера в БД."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO screener_signals
                (asset, direction, entry, tp1, tp2, sl, confidence, tf, ai_text, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                asset,
                signal.get('direction'),
                signal.get('entry'),
                signal.get('tp1'),
                signal.get('tp2'),
                signal.get('sl'),
                signal.get('confidence', {}).get('score', 0),
                signal.get('tf', '1h'),
                ai_text[:3000],
                datetime.now(timezone.utc).isoformat()
            ))
    except Exception as e:
        logger.warning(f"[SAVE_SIGNAL] {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  ГЛАВНЫЙ КЛАСС СКРИНЕРА
# ══════════════════════════════════════════════════════════════════════════════

class AutoScreener:
    """
    Автоматический скринер рынка.
    Каждые INTERVAL_MIN минут сканирует SCREENER_ASSETS,
    публикует лучшие сигналы в Telegram.
    """

    INTERVAL_MIN = 30      # интервал сканирования (минуты)
    SCORE_THRESH  = 4      # минимальный |score| для полного AI анализа
    MIN_CONF      = 45     # минимальный confidence% для публикации
    MAX_SIGNALS   = 3      # максимум сигналов за один цикл
    COOLDOWN_SEC  = 7200   # паузa между сигналами по одному активу (2 часа)

    def __init__(self, collector, analyzer, db_path: str,
                 groq_key: str, cryptopanic_key: str,
                 tg_token_channel: str, channel_id: str,
                 tg_token_dm: str):
        self.collector      = collector
        self.analyzer       = analyzer
        self.db_path        = db_path
        self.groq_key       = groq_key
        self.cp_key         = cryptopanic_key
        self.tg_channel_tok = tg_token_channel
        self.channel_id     = channel_id
        self.tg_dm_tok      = tg_token_dm
        self._running       = False
        self._thread        = None
        self._last_signals: dict = {}  # asset → timestamp последнего сигнала

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(
            target=self._loop, daemon=True, name="AutoScreener"
        )
        self._thread.start()
        logger.info("[SCREENER] Запущен. Интервал: %d мин", self.INTERVAL_MIN)

    def stop(self):
        self._running = False
        logger.info("[SCREENER] Остановлен")

    def _loop(self):
        """Основной цикл — первый запуск через 2 мин после старта сервера."""
        time.sleep(120)
        while self._running:
            try:
                self._run_cycle()
            except Exception as e:
                logger.error(f"[SCREENER] Ошибка цикла: {e}")
            for _ in range(self.INTERVAL_MIN * 60):
                if not self._running:
                    return
                time.sleep(1)

    def _run_cycle(self):
        """Один цикл скрининга."""
        logger.info("[SCREENER] ═══ Начало цикла сканирования ═══")
        now = time.time()

        # ── 1. Быстрый скоринг ────────────────────────────────────────────
        candidates = []
        for asset in SCREENER_ASSETS:
            # Пропускаем если сигнал был недавно
            if now - self._last_signals.get(asset, 0) < self.COOLDOWN_SEC:
                continue
            result = _quick_score(asset, self.collector)
            if result and abs(result['score']) >= self.SCORE_THRESH:
                candidates.append(result)
            time.sleep(0.3)

        if not candidates:
            logger.info("[SCREENER] Нет сильных сигналов в этом цикле")
            return

        # Сортируем по силе сигнала
        candidates.sort(key=lambda x: abs(x['score']), reverse=True)
        top = candidates[:self.MAX_SIGNALS * 2]  # берём с запасом
        logger.info(f"[SCREENER] Кандидаты ({len(top)}): {[c['asset'] for c in top]}")

        # ── 2. Полный AI анализ ───────────────────────────────────────────
        full_signals = []
        for cand in top[:self.MAX_SIGNALS + 1]:
            sig = _full_ai_signal(
                cand['asset'], self.collector, self.analyzer,
                self.groq_key, primary_tf='1h', leverage=20, margin=100
            )
            if sig and sig.get('confidence', {}).get('score', 0) >= self.MIN_CONF:
                full_signals.append(sig)
                self._last_signals[cand['asset']] = now
            time.sleep(1)

        if not full_signals:
            logger.info("[SCREENER] Нет сигналов с достаточным доверием")
            return

        # Сортируем по confidence
        full_signals.sort(
            key=lambda s: s.get('confidence', {}).get('score', 0), reverse=True
        )
        full_signals = full_signals[:self.MAX_SIGNALS]

        # ── 3. Новости ────────────────────────────────────────────────────
        news_map = {}
        for sig in full_signals:
            news_map[sig['asset']] = get_news_context(
                sig['asset'], self.cp_key, self.groq_key)
            time.sleep(0.5)

        # ── 4. Публикуем в канал/бот ──────────────────────────────────────
        for i, sig in enumerate(full_signals):
            asset    = sig['asset']
            news_ctx = news_map.get(asset, "")
            is_best  = (i == 0)
            msg      = _format_channel_message(sig, news_ctx, is_best)

            # Отправляем в канал (или бот @SynapseX_ProjectBot)
            if self.tg_channel_tok and self.channel_id:
                ok = self._send_telegram(self.tg_channel_tok, self.channel_id, msg)
                logger.info(
                    f"[SCREENER] {'✅' if ok else '❌'} Опубликован: {asset} "
                    f"{sig['direction']} conf={sig.get('confidence',{}).get('score')}% "
                    f"→ {self.channel_id}"
                )

            _save_screener_signal(self.db_path, asset, sig, sig.get('ai_text', ''))
            time.sleep(2)

        # ── 5. Premium DM ─────────────────────────────────────────────────
        premium_users = _get_premium_telegram_users(self.db_path)
        if not premium_users:
            logger.info("[SCREENER] Premium пользователей с Telegram нет")
            return

        for sig in full_signals:
            asset    = sig['asset']
            news_ctx = news_map.get(asset, "")
            for user in premium_users:
                chat_id  = str(user.get('telegram_chat_id', ''))
                username = user.get('name', '')
                if not chat_id:
                    continue
                dm_msg = _format_premium_dm(sig, news_ctx, username)
                self._send_telegram(self.tg_dm_tok, chat_id, dm_msg)
                time.sleep(0.08)  # rate limit TG: ~12 сообщений/сек
            logger.info(f"[SCREENER] DM отправлено {len(premium_users)} Premium: {asset}")

        logger.info(f"[SCREENER] ═══ Цикл завершён. Опубликовано: {len(full_signals)} ═══")

    def _send_telegram(self, token: str, chat_id: str, text: str) -> bool:
        """Отправка сообщения в Telegram."""
        if not token or not chat_id:
            return False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id":    chat_id,
                    "text":       text[:4096],
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10
            )
            if not resp.ok:
                logger.warning(f"[TG] {resp.status_code}: {resp.text[:120]}")
            return resp.ok
        except Exception as e:
            logger.warning(f"[TG_SEND] {chat_id}: {e}")
            return False

    def force_run(self):
        """Принудительный запуск цикла (из /admin)."""
        threading.Thread(target=self._run_cycle, daemon=True, name="ScreenerForce").start()


# ══════════════════════════════════════════════════════════════════════════════
#  ИНИЦИАЛИЗАЦИЯ БД
# ══════════════════════════════════════════════════════════════════════════════

def init_screener_db(db_path: str):
    """Создаёт таблицы для скринера."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS screener_signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                asset       TEXT    NOT NULL,
                direction   TEXT    NOT NULL,
                entry       REAL,
                tp1         REAL,
                tp2         REAL,
                sl          REAL,
                confidence  INTEGER DEFAULT 0,
                tf          TEXT    DEFAULT '1h',
                ai_text     TEXT,
                result      TEXT    DEFAULT 'pending',
                closed_at   TEXT,
                created_at  TEXT    NOT NULL
            )
        """)
        # Добавляем колонки в users если нет
        for col, defn in [
            ('telegram_chat_id',  "TEXT DEFAULT ''"),
            ('tg_alerts_enabled', "INTEGER DEFAULT 1"),
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
            except Exception:
                pass
        conn.commit()
    logger.info("[SCREENER_DB] Таблицы инициализированы")