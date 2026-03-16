# -*- coding: utf-8 -*-
"""
ai_consensus.py — Neural Consensus Engine для SynapseX
Три роли: Технический Аналитик + SMC Аналитик + Риск Менеджер
"""
import os, re, logging, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import requests

log = logging.getLogger("AI_CONSENSUS")

GROQ_KEY   = os.getenv("GROQ_KEY",       "")
GEMINI_KEY = os.getenv("GEMINI_KEY",      "")
CP_KEY     = os.getenv("CRYPTOPANIC_KEY", "")

GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

_btc_cache: dict  = {}
_btc_ts:    float = 0.0
_news_cache: dict = {}
_news_ts:    dict = {}


def _market_session() -> str:
    h = datetime.now(timezone.utc).hour
    if   0  <= h < 4:  return "АЗИЯ НОЧЬ — низкая ликвидность"
    elif 4  <= h < 8:  return "АЗИЯ УТРО — умеренная активность"
    elif 8  <= h < 12: return "ЕВРОПА — высокая активность"
    elif 12 <= h < 17: return "США+ЕВРОПА — МАКСИМАЛЬНАЯ ликвидность"
    elif 17 <= h < 21: return "США ВЕЧЕР — снижается"
    else:              return "США НОЧЬ — низкая ликвидность"


def _btc_context() -> dict:
    global _btc_cache, _btc_ts
    if time.time() - _btc_ts < 300 and _btc_cache:
        return _btc_cache
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/24hr",
                         params={"symbol": "BTCUSDT"}, timeout=5)
        if r.ok:
            d, p, chg = r.json(), float(r.json().get("lastPrice",0)), float(r.json().get("priceChangePercent",0))
            p   = float(d.get("lastPrice", 0))
            chg = float(d.get("priceChangePercent", 0))
            if chg > 3:    trend = "СИЛЬНЫЙ РОСТ"
            elif chg > 1:  trend = "РОСТ"
            elif chg > -1: trend = "БОКОВИК"
            elif chg > -3: trend = "ПАДЕНИЕ"
            else:          trend = "СИЛЬНОЕ ПАДЕНИЕ"
            hint = ("Бычий рынок — подтверждает LONG на альтах." if chg > 1 else
                    "Медвежий рынок — осторожно с LONG." if chg < -1 else
                    "Боковик BTC — альты движутся независимо.")
            _btc_cache = {"price": p, "change_pct": chg, "trend": trend,
                          "context": f"BTC ${p:,.0f} ({chg:+.2f}%) — {trend}. {hint}"}
            _btc_ts = time.time()
    except Exception as e:
        log.warning(f"[CONSENSUS] BTC: {e}")
        _btc_cache = {"trend": "?", "context": "BTC данные недоступны"}
    return _btc_cache


def _news_context(asset: str) -> str:
    global _news_cache, _news_ts
    key = asset.replace("-USDT", "").lower()
    if time.time() - _news_ts.get(key, 0) < 600 and key in _news_cache:
        return _news_cache[key]
    if not CP_KEY:
        return ""
    try:
        r = requests.get("https://cryptopanic.com/api/v1/posts/",
                         params={"auth_token": CP_KEY, "currencies": key,
                                 "filter": "hot", "public": "true"}, timeout=5)
        if r.ok:
            posts = r.json().get("results", [])[:3]
            if posts:
                titles = [p.get("title", "") for p in posts]
                bulls = sum(1 for t in titles if any(w in t.lower()
                            for w in ["bull","surge","pump","ath","rally"]))
                bears = sum(1 for t in titles if any(w in t.lower()
                            for w in ["bear","crash","dump","fall","drop"]))
                sent = "бычий" if bulls > bears else "медвежий" if bears > 0 else "нейтральный"
                ctx = f"Новости: {sent}. " + " | ".join(titles[:2])
                _news_cache[key] = ctx
                _news_ts[key]    = time.time()
                return ctx
    except Exception as e:
        log.warning(f"[CONSENSUS] News: {e}")
    return ""


def _signal_history(asset: str, db_path: str) -> str:
    try:
        import sqlite3
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT direction, confidence, created_at FROM signals "
                "WHERE asset=? ORDER BY created_at DESC LIMIT 5", (asset,)
            ).fetchall()
        if not rows:
            return "История: нет данных"
        parts = [f"{d}({c}%)" for d, c, _ in rows]
        return "Последние сигналы: " + " > ".join(parts)
    except Exception:
        return ""


def _parse_direction(text: str) -> str:
    u = text.upper()
    longs  = u.count("LONG")  + u.count("ПОКУПКА") + u.count("БЫЧИЙ")
    shorts = u.count("SHORT") + u.count("ПРОДАЖА") + u.count("МЕДВЕЖИЙ")
    if longs > shorts:  return "LONG"
    if shorts > longs:  return "SHORT"
    return "NEUTRAL"


def _parse_confidence(text: str) -> int:
    for pat in [r"(\d{1,3})%\s*уверен", r"уверенност\w+\s*:?\s*(\d{1,3})",
                r"доверие\s*:?\s*(\d{1,3})", r"(\d{1,3})%"]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            v = int(m.group(1))
            if 0 < v <= 100:
                return v
    return 50


def _parse_price(text: str, keyword: str) -> float:
    m = re.search(keyword + r"[:\s]+\$?([\d,\.]+)", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except Exception:
            pass
    return 0.0


def _conclusion(text: str, keyword: str) -> str:
    if not text:
        return "нет данных"
    idx = text.upper().find(keyword.upper())
    if idx >= 0:
        part = text[idx + len(keyword):].strip().lstrip(":").strip()
        return part.split("\n")[0][:180]
    sents = [s.strip() for s in text.split(".") if len(s.strip()) > 20]
    return sents[-1][:180] if sents else "нет данных"


def _technical_analyst(signal, asset, btc_ctx, news_ctx, history, session):
    if not GROQ_KEY:
        return {"error": "no_key", "direction": "NEUTRAL", "confidence": 0,
                "role": "Технический Аналитик", "text": ""}
    p      = signal.get("price", 0)
    rsi    = signal.get("rsi", 50)
    stoch  = signal.get("stochastic", {})
    obv    = signal.get("obv_trend", "нейтральный")
    pats   = signal.get("patterns", [])
    atr    = signal.get("atr", 0)
    tf     = signal.get("primary_tf", "1h").upper()
    lev    = signal.get("leverage", 20)
    vol_sp = signal.get("volume_spike", False)
    pat_s  = ", ".join(x["name"] for x in pats) if pats else "нет"

    prompt = (
        f"Ты — Технический Аналитик фьючерсного рынка. Только индикаторы.\n"
        f"Русский язык. Без markdown.\n\n"
        f"АКТИВ: {asset} | ТФ: {tf} | ЦЕНА: {p} | ПЛЕЧО: {lev}x\n\n"
        f"ИНДИКАТОРЫ:\n"
        f"RSI: {rsi:.1f} | Stoch K/D: {stoch.get('k',50):.0f}/{stoch.get('d',50):.0f}\n"
        f"OBV: {obv} | ATR: {atr:.6f} | Объём: {'ВСПЛЕСК!' if vol_sp else 'норма'}\n"
        f"Паттерны: {pat_s}\n\n"
        f"КОНТЕКСТ:\n{btc_ctx}\nСессия: {session}\n{history}\n"
        f"{('Новости: ' + news_ctx) if news_ctx else ''}\n\n"
        f"ФОРМАТ:\n"
        f"НАПРАВЛЕНИЕ: LONG или SHORT\n"
        f"УВЕРЕННОСТЬ: [0-100]%\n"
        f"ВЫВОД ТЕХНИЧЕСКОГО АНАЛИТИКА: [3 предложения]"
    )
    try:
        r = requests.post(GROQ_URL,
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1, "max_tokens": 400},
            headers={"Authorization": f"Bearer {GROQ_KEY}",
                     "Content-Type": "application/json"}, timeout=15)
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"]
        return {"role": "Технический Аналитик", "model": "llama-3.3-70b",
                "text": text, "direction": _parse_direction(text),
                "confidence": _parse_confidence(text)}
    except Exception as e:
        log.error(f"[CONSENSUS] Tech: {e}")
        return {"error": str(e), "direction": "NEUTRAL", "confidence": 0,
                "role": "Технический Аналитик", "text": ""}


def _smc_analyst(signal, asset, btc_ctx):
    if not GROQ_KEY:
        return {"error": "no_key", "direction": "NEUTRAL", "confidence": 0,
                "role": "SMC Аналитик", "text": ""}
    p      = signal.get("price", 0)
    smc    = signal.get("smart_money", "нет")
    liq    = signal.get("liquidity", "нет")
    sweep  = signal.get("liq_sweep", "нет")
    bos    = signal.get("bos", {})
    fibo   = signal.get("fibonacci", {})
    sr     = signal.get("sr_levels", {})
    tf     = signal.get("primary_tf", "1h").upper()
    lev    = signal.get("leverage", 20)
    fibo_s = " | ".join(f"{k}={v}" for k,v in fibo.items() if v) if fibo else "N/A"

    prompt = (
        f"Ты — SMC аналитик фьючерсного рынка. Только структура рынка.\n"
        f"Русский. Без markdown.\n\n"
        f"АКТИВ: {asset} | ТФ: {tf} | ЦЕНА: {p} | ПЛЕЧО: {lev}x\n\n"
        f"SMC ДАННЫЕ:\n"
        f"Smart Money: {smc} | Ликвидность: {liq}\n"
        f"Liquidity Sweep: {sweep}\n"
        f"BOS: {bos.get('bos','нет')} | ChoCH: {bos.get('choch','нет')}\n"
        f"Support: {sr.get('support','N/A')} | Resistance: {sr.get('resistance','N/A')}\n"
        f"FIBONACCI: {fibo_s}\n\n"
        f"BTC: {btc_ctx}\n\n"
        f"ФОРМАТ:\n"
        f"НАПРАВЛЕНИЕ: LONG или SHORT\n"
        f"УВЕРЕННОСТЬ: [0-100]%\n"
        f"ЗОНА ЦЕНЫ: [discount/fair value/premium]\n"
        f"ВЫВОД SMC АНАЛИТИКА: [3 предложения]"
    )
    try:
        r = requests.post(GROQ_URL,
            json={"model": "mixtral-8x7b-32768",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1, "max_tokens": 400},
            headers={"Authorization": f"Bearer {GROQ_KEY}",
                     "Content-Type": "application/json"}, timeout=15)
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"]
        return {"role": "SMC Аналитик", "model": "mixtral-8x7b",
                "text": text, "direction": _parse_direction(text),
                "confidence": _parse_confidence(text)}
    except Exception as e:
        log.error(f"[CONSENSUS] SMC: {e}")
        return {"error": str(e), "direction": "NEUTRAL", "confidence": 0,
                "role": "SMC Аналитик", "text": ""}


def _risk_manager(signal, asset, tech, smc):
    if not GEMINI_KEY:
        return {"error": "no_key", "direction": "NEUTRAL", "confidence": 0,
                "role": "Риск Менеджер", "text": ""}
    p    = signal.get("price", 0)
    sl   = signal.get("sl", 0)
    tp1  = signal.get("tp1", 0)
    tp2  = signal.get("tp2", 0)
    tp3  = signal.get("tp3", 0)
    atr  = signal.get("atr", 0)
    lev  = signal.get("leverage", 20)
    marg = signal.get("margin", 100)
    tf   = signal.get("primary_tf", "1h").upper()
    swh  = signal.get("swing_high", p)
    swl  = signal.get("swing_low",  p)
    t_d  = tech.get("direction", "?")
    t_c  = tech.get("confidence", 50)
    s_d  = smc.get("direction",  "?")
    s_c  = smc.get("confidence", 50)
    agree = "СОГЛАСНЫ" if t_d == s_d else "КОНФЛИКТ"

    prompt = (
        f"Ты — Риск Менеджер фьючерсного трейдинга. Оптимизируй SL/TP.\n"
        f"Русский. Только цифры.\n\n"
        f"АКТИВ: {asset} | ТФ: {tf} | ЦЕНА: {p}\n"
        f"ПЛЕЧО: {lev}x | МАРЖА: ${marg}\n\n"
        f"АНАЛИТИКИ:\n"
        f"Технический: {t_d} ({t_c}%) — {tech.get('text','')[:120]}\n"
        f"SMC: {s_d} ({s_c}%) — {smc.get('text','')[:120]}\n"
        f"Консенсус: {agree}\n\n"
        f"УРОВНИ АЛГО: SL={sl} TP1={tp1} TP2={tp2} TP3={tp3} ATR={atr:.6f}\n"
        f"Swing High={swh} | Swing Low={swl}\n\n"
        f"Правила: SL за свинг+буфер. TP1 мин 1.5:1. TP2 мин 3:1. TP3 мин 5:1.\n\n"
        f"ФОРМАТ:\n"
        f"ФИНАЛЬНОЕ НАПРАВЛЕНИЕ: LONG или SHORT\n"
        f"ИТОГОВАЯ УВЕРЕННОСТЬ: [0-100]%\n"
        f"СТОП-ЛОСС: [цена]\n"
        f"ТЕЙК-ПРОФИТ 1: [цена]\n"
        f"ТЕЙК-ПРОФИТ 2: [цена]\n"
        f"ТЕЙК-ПРОФИТ 3: [цена]\n"
        f"ВЫВОД РИСК МЕНЕДЖЕРА: [2-3 предложения]"
    )
    try:
        r = requests.post(f"{GEMINI_URL}?key={GEMINI_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"temperature": 0.05, "maxOutputTokens": 450}},
            timeout=20)
        r.raise_for_status()
        parts = r.json().get("candidates",[{}])[0].get("content",{}).get("parts",[])
        text  = parts[0].get("text","") if parts else ""
        return {"role": "Риск Менеджер", "model": "gemini-1.5-flash",
                "text": text, "direction": _parse_direction(text),
                "confidence": _parse_confidence(text)}
    except Exception as e:
        log.error(f"[CONSENSUS] Risk: {e}")
        return {"error": str(e), "direction": "NEUTRAL", "confidence": 0,
                "role": "Риск Менеджер", "text": ""}


def get_consensus(signal: dict, asset: str, db_path: str = "signals.db") -> dict:
    """
    Главная функция — запускает 3 AI роли и выдаёт консенсус.
    Роли 1+2 параллельно, роль 3 использует их результаты.
    """
    t0 = time.time()
    log.info(f"[CONSENSUS] Старт {asset}...")

    btc_ctx  = _btc_context().get("context", "")
    news_ctx = _news_context(asset)
    history  = _signal_history(asset, db_path)
    session  = _market_session()

    # Swing levels для риск менеджера
    highs = signal.get("highs_20", [])
    lows  = signal.get("lows_20",  [])
    p     = signal.get("price", 0)
    signal["swing_high"] = max(highs) if highs else p
    signal["swing_low"]  = min(lows)  if lows  else p

    # Параллельно роли 1 и 2
    tech = {"direction": "NEUTRAL", "confidence": 50, "text": "", "role": "Технический Аналитик"}
    smc  = {"direction": "NEUTRAL", "confidence": 50, "text": "", "role": "SMC Аналитик"}
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(_technical_analyst, signal, asset, btc_ctx, news_ctx, history, session)
        f2 = pool.submit(_smc_analyst, signal, asset, btc_ctx)
        try:
            tech = f1.result(timeout=18)
        except Exception as e:
            log.warning(f"[CONSENSUS] Tech failed: {e}")
        try:
            smc = f2.result(timeout=18)
        except Exception as e:
            log.warning(f"[CONSENSUS] SMC failed: {e}")

    # Роль 3
    risk = _risk_manager(signal, asset, tech, smc)

    # Голосование
    votes = [r.get("direction") for r in [tech, smc, risk]
             if r.get("direction") in ("LONG", "SHORT")]
    lv, sv = votes.count("LONG"), votes.count("SHORT")
    if lv > sv:   final_dir, vote_str = "LONG",  f"LONG {lv}/3"
    elif sv > lv: final_dir, vote_str = "SHORT", f"SHORT {sv}/3"
    else:
        final_dir = tech.get("direction", signal.get("direction", "LONG"))
        vote_str  = "НИЧЬЯ — технический приоритет"

    confs    = [r["confidence"] for r in [tech, smc, risk] if r.get("confidence",0) > 0]
    avg_conf = round(sum(confs)/len(confs)) if confs else 50
    mx = max(lv, sv)
    if mx == 3:
        avg_conf = min(avg_conf + 8, 100); label = "ПОЛНЫЙ КОНСЕНСУС 🟢"
    elif mx == 2:
        avg_conf = min(avg_conf + 3, 100); label = "БОЛЬШИНСТВО 🟡"
    else:
        avg_conf = max(avg_conf - 10, 0);  label = "КОНФЛИКТ МНЕНИЙ 🔴"

    # Уровни от риск менеджера
    rt   = risk.get("text", "")
    entry = p
    sl   = _parse_price(rt, "СТОП-ЛОСС")      or signal.get("sl", 0)
    tp1  = _parse_price(rt, "ТЕЙК-ПРОФИТ 1")  or signal.get("tp1", 0)
    tp2  = _parse_price(rt, "ТЕЙК-ПРОФИТ 2")  or signal.get("tp2", 0)
    tp3  = _parse_price(rt, "ТЕЙК-ПРОФИТ 3")  or signal.get("tp3", 0)

    elapsed = round(time.time() - t0, 1)
    log.info(f"[CONSENSUS] Done {elapsed}s: {final_dir} ({avg_conf}%) {label}")

    lev  = signal.get("leverage", 20)
    def pct(target, profit=True):
        if not entry or not target: return "—"
        raw = abs(target - entry) / entry * lev * 100
        return (f"+{raw:.1f}%" if profit else f"-{raw:.1f}%")

    dir_e = "🟢 LONG" if final_dir == "LONG" else "🔴 SHORT"
    text_out = "\n".join([
        "═══ SYNAPSE NEURAL CONSENSUS ═══════════════",
        "",
        "🧠 МНЕНИЯ АНАЛИТИКОВ:",
        f"  🔬 Технический:   {tech.get('direction','?')} ({tech.get('confidence',0)}%)",
        f"  📊 SMC:           {smc.get('direction','?')}  ({smc.get('confidence',0)}%)",
        f"  🎯 Риск Менеджер: {risk.get('direction','?')} ({risk.get('confidence',0)}%)",
        "",
        f"⚖️  ГОЛОСОВАНИЕ: {vote_str}",
        "",
        "════════════════════════════════════════════",
        f"  {dir_e} | {label}",
        f"  УВЕРЕННОСТЬ: {avg_conf}%",
        "════════════════════════════════════════════",
        "",
        "💰 ТОРГОВЫЙ ПЛАН:",
        f"  ВХОД:  {entry}",
        f"  TP1:   {tp1} ({pct(tp1)}) | 40%",
        f"  TP2:   {tp2} ({pct(tp2)}) | 35%",
        f"  TP3:   {tp3} ({pct(tp3)}) | 25%",
        f"  СТОП:  {sl} ({pct(sl, False)})",
        "",
        "🌐 КОНТЕКСТ:",
        f"  {btc_ctx[:80] if btc_ctx else 'BTC: нет данных'}",
        f"  ⏰ {session}",
        (f"  📰 {news_ctx[:100]}" if news_ctx else ""),
        "",
        "─────────────────────────────────────────────",
        f"🔬 ТА: {_conclusion(tech.get('text',''), 'ВЫВОД ТЕХНИЧЕСКОГО')}",
        f"📊 SMC: {_conclusion(smc.get('text',''), 'ВЫВОД SMC')}",
        f"🛡️ РИСК: {_conclusion(risk.get('text',''), 'ВЫВОД РИСК')}",
    ])

    return {
        "direction": final_dir, "confidence": avg_conf,
        "label": label, "vote": vote_str,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "technical": tech, "smc": smc, "risk": risk,
        "btc_context": btc_ctx, "news_context": news_ctx,
        "history": history, "session": session,
        "text": text_out,
        "asset": asset, "timeframe": signal.get("primary_tf","1h"),
        "elapsed_sec": elapsed,
    }
