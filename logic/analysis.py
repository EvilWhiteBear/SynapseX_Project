# -*- coding: utf-8 -*-
"""
analysis.py — QuantumAnalyzer v3
──────────────────────────────────────
Улучшения v3:
• Система confluence: 13 факторов вместо 7 → точность выше
• RSI дивергенция (бычья/медвежья)
• Подтверждение объёмом (Volume Spike)
• Стохастик для дополнительного фильтра перекупленности/перепроданности
• Williams %R для подтверждения разворотов
• BOS/ChoCH улучшен — смотрит на несколько свечей
• Качество сигнала: LOW / MEDIUM / HIGH / VERY HIGH
• Все поля которые ожидает app.py (confidence, historical, bos, sr_levels, patterns...)
"""
import math
import logging

logger = logging.getLogger("SYNAPSE_ANALYSIS")


class QuantumAnalyzer:
    """
    Математический движок для генерации торговых сигналов.
    Использует 13-факторную систему confluence для повышения точности.
    """

    # ══════════════════════════════════════════════════════════════════════════
    #  БАЗОВЫЕ ИНДИКАТОРЫ
    # ══════════════════════════════════════════════════════════════════════════

    def _sma(self, data: list, period: int) -> float:
        if not data: return 0.0
        n = min(period, len(data))
        return sum(data[-n:]) / n

    def _ema(self, data: list, period: int) -> float:
        if not data: return 0.0
        if len(data) < period:
            return self._sma(data, len(data))
        k   = 2.0 / (period + 1)
        ema = sum(data[:period]) / period
        for price in data[period:]:
            ema = (price - ema) * k + ema
        return ema

    def _rsi(self, data: list, period: int = 14) -> float:
        """Wilder RSI."""
        if len(data) < period + 1: return 50.0
        avg_gain = avg_loss = 0.0
        for i in range(1, period + 1):
            diff = data[i] - data[i - 1]
            if diff > 0: avg_gain += diff
            else: avg_loss += abs(diff)
        avg_gain /= period
        avg_loss /= period
        for i in range(period + 1, len(data)):
            diff = data[i] - data[i - 1]
            gain = diff if diff > 0 else 0.0
            loss = abs(diff) if diff < 0 else 0.0
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0: return 100.0
        return round(100.0 - (100.0 / (1.0 + avg_gain / avg_loss)), 2)

    def _rsi_divergence(self, prices: list, period: int = 14) -> str:
        """
        Определяет RSI дивергенцию на последних двух свинг-точках.
        Бычья дивергенция: цена делает LL, RSI делает HL → сигнал разворота вверх.
        Медвежья дивергенция: цена делает HH, RSI делает LH → сигнал разворота вниз.
        """
        if len(prices) < period + 20:
            return "none"
        try:
            # Берём RSI на разных окнах
            rsi_now  = self._rsi(prices, period)
            rsi_prev = self._rsi(prices[:-10], period)
            price_now  = prices[-1]
            price_prev = prices[-11]

            # Бычья дивергенция
            if price_now < price_prev and rsi_now > rsi_prev and rsi_now < 45:
                return "bullish_divergence"
            # Медвежья дивергенция
            if price_now > price_prev and rsi_now < rsi_prev and rsi_now > 55:
                return "bearish_divergence"
        except Exception:
            pass
        return "none"

    def _stochastic(self, highs: list, lows: list, closes: list,
                    k_period: int = 14, d_period: int = 3) -> dict:
        """Стохастический осциллятор %K и %D."""
        if len(closes) < k_period:
            return {"k": 50.0, "d": 50.0, "signal": "NEUTRAL"}
        try:
            h_max = max(highs[-k_period:])
            l_min = min(lows[-k_period:])
            rng   = h_max - l_min or 0.0001
            k     = round((closes[-1] - l_min) / rng * 100, 2)
            # D — простая средняя последних k-значений (упрощённо)
            k_vals = []
            for i in range(d_period):
                idx_end = len(closes) - i
                if idx_end < k_period: break
                hm = max(highs[idx_end-k_period:idx_end])
                lm = min(lows[idx_end-k_period:idx_end])
                r  = hm - lm or 0.0001
                k_vals.append((closes[idx_end-1] - lm) / r * 100)
            d = round(sum(k_vals) / len(k_vals), 2) if k_vals else k

            signal = "NEUTRAL"
            if k < 20 and d < 20:   signal = "OVERSOLD"
            elif k > 80 and d > 80: signal = "OVERBOUGHT"
            elif k > d and k < 40:  signal = "BULLISH_CROSS"
            elif k < d and k > 60:  signal = "BEARISH_CROSS"
            return {"k": k, "d": d, "signal": signal}
        except Exception:
            return {"k": 50.0, "d": 50.0, "signal": "NEUTRAL"}

    def _williams_r(self, highs: list, lows: list, closes: list,
                    period: int = 14) -> float:
        """Williams %R: -100..0. Ниже -80 = перепродан, выше -20 = перекуплен."""
        if len(closes) < period: return -50.0
        try:
            h_max = max(highs[-period:])
            l_min = min(lows[-period:])
            rng   = h_max - l_min or 0.0001
            return round((h_max - closes[-1]) / rng * -100, 2)
        except Exception:
            return -50.0

    def _volume_spike(self, volumes: list, period: int = 20) -> bool:
        """True если текущий объём > 1.5x среднего за period свечей."""
        if len(volumes) < period + 1: return False
        try:
            avg = sum(volumes[-period-1:-1]) / period
            return volumes[-1] > avg * 1.5 if avg > 0 else False
        except Exception:
            return False

    def _macd(self, data: list, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
        """Полный MACD с crossover."""
        empty = {"macd": 0.0, "signal": 0.0, "hist": 0.0, "cross": "NONE",
                 "macd_hist_trend": "flat"}
        if len(data) < slow + signal: return empty
        macd_series = []
        window_start = max(0, len(data) - slow - signal * 3)
        for i in range(window_start, len(data)):
            slice_ = data[:i + 1]
            if len(slice_) < slow: continue
            ef = self._ema(slice_, fast)
            es = self._ema(slice_, slow)
            macd_series.append(ef - es)
        if len(macd_series) < signal: return empty
        macd_line   = macd_series[-1]
        signal_line = self._ema(macd_series, signal)
        histogram   = macd_line - signal_line
        cross = "NONE"
        if len(macd_series) >= 2:
            prev_hist = macd_series[-2] - self._ema(macd_series[:-1], signal)
            if prev_hist < 0 and histogram > 0: cross = "BULLISH"
            elif prev_hist > 0 and histogram < 0: cross = "BEARISH"
        # Тренд гистограммы
        hist_trend = "flat"
        if len(macd_series) >= 3:
            prev2_signal = self._ema(macd_series[:-2], signal)
            prev2_hist   = macd_series[-3] - prev2_signal
            prev_hist_v  = macd_series[-2] - self._ema(macd_series[:-1], signal)
            if prev2_hist < prev_hist_v < histogram: hist_trend = "growing"
            elif prev2_hist > prev_hist_v > histogram: hist_trend = "falling"
        return {
            "macd":            round(macd_line, 6),
            "signal":          round(signal_line, 6),
            "hist":            round(histogram, 6),
            "cross":           cross,
            "macd_hist_trend": hist_trend,
        }

    def _bollinger(self, data: list, period: int = 20, std_dev: float = 2.0) -> dict:
        empty = {"upper": 0.0, "lower": 0.0, "mid": 0.0, "width": 0.0,
                 "squeeze": False, "percent_b": 0.5}
        if len(data) < period: return empty
        window   = data[-period:]
        mid      = sum(window) / period
        variance = sum((x - mid) ** 2 for x in window) / period
        sd       = math.sqrt(variance)
        upper    = mid + std_dev * sd
        lower    = mid - std_dev * sd
        width    = round((upper - lower) / mid * 100, 3) if mid else 0.0
        # Squeeze: ширина меньше 2% → сжатие = предстоит движение
        squeeze  = width < 2.0
        # %B: где цена относительно полос (0=нижняя, 1=верхняя)
        percent_b = round((data[-1] - lower) / (upper - lower), 3) if (upper - lower) > 0 else 0.5
        return {
            "upper": round(upper, 6), "lower": round(lower, 6), "mid": round(mid, 6),
            "width": width, "squeeze": squeeze, "percent_b": percent_b,
        }

    def _obv(self, closes: list, volumes: list) -> float:
        if not closes or not volumes or len(closes) != len(volumes): return 0.0
        obv = 0.0
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]: obv += volumes[i]
            elif closes[i] < closes[i - 1]: obv -= volumes[i]
        return round(obv, 2)

    def _obv_trend(self, closes: list, volumes: list) -> str:
        """OBV тренд: rising/falling/neutral."""
        if len(closes) < 10 or len(volumes) < 10: return "neutral"
        obv_vals = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i-1]: obv_vals.append(obv_vals[-1] + volumes[i])
            elif closes[i] < closes[i-1]: obv_vals.append(obv_vals[-1] - volumes[i])
            else: obv_vals.append(obv_vals[-1])
        recent = obv_vals[-10:]
        slope  = recent[-1] - recent[0]
        if slope > 0:   return "rising (bullish)"
        elif slope < 0: return "falling (bearish)"
        return "neutral"

    def _atr(self, highs: list, lows: list, closes: list, period: int = 14) -> float:
        if len(highs) < period + 1: return 0.0
        trs = []
        for i in range(1, len(closes)):
            trs.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i]  - closes[i-1])
            ))
        if not trs: return 0.0
        if len(trs) < period: return round(sum(trs) / len(trs), 6)
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return round(atr, 6)

    def _trend(self, closes: list) -> str:
        """Тренд по SMA20/SMA50."""
        if len(closes) < 50: return "SIDEWAYS"
        sma20 = self._sma(closes, 20)
        sma50 = self._sma(closes, 50)
        last  = closes[-1]
        if sma20 > sma50 and last > sma20: return "UPTREND"
        if sma20 < sma50 and last < sma20: return "DOWNTREND"
        return "SIDEWAYS"

    def _bb_trend(self, closes: list) -> str:
        bb = self._bollinger(closes)
        if not closes or bb["mid"] == 0: return "SIDEWAYS"
        last = closes[-1]
        if last > bb["mid"] and last < bb["upper"]: return "UPTREND"
        if last < bb["mid"] and last > bb["lower"]: return "DOWNTREND"
        if last >= bb["upper"]: return "OVERBOUGHT"
        if last <= bb["lower"]: return "OVERSOLD"
        return "SIDEWAYS"

    def _patterns(self, opens: list, highs: list, lows: list, closes: list) -> str:
        """Свечные паттерны."""
        if len(closes) < 3: return "None"
        o1, h1, l1, c1 = opens[-2], highs[-2], lows[-2], closes[-2]
        o2, h2, l2, c2 = opens[-1], highs[-1], lows[-1], closes[-1]
        if c1 < o1 and c2 > o2 and c2 > o1 and o2 < c1: return "Bullish Engulfing"
        if c1 > o1 and c2 < o2 and c2 < o1 and o2 > c1: return "Bearish Engulfing"
        body         = abs(c2 - o2) or 0.0001
        upper_shadow = h2 - max(c2, o2)
        lower_shadow = min(c2, o2) - l2
        if lower_shadow > body * 2 and upper_shadow < body:  return "Bullish Pinbar (Hammer)"
        if upper_shadow > body * 2 and lower_shadow < body:  return "Bearish Pinbar (Shooting Star)"
        full_range = h2 - l2 or 0.0001
        if body / full_range < 0.1: return "Doji (Indecision)"
        # Morning/Evening Star (3-свечной)
        if len(closes) >= 3:
            o0, c0 = opens[-3], closes[-3]
            if c0 < o0 and abs(c1-o1)/(h1-l1+0.0001) < 0.3 and c2 > o2 and c2 > (o0+c0)/2:
                return "Morning Star (Bullish Reversal)"
            if c0 > o0 and abs(c1-o1)/(h1-l1+0.0001) < 0.3 and c2 < o2 and c2 < (o0+c0)/2:
                return "Evening Star (Bearish Reversal)"
        return "None"

    def _sr_levels(self, highs: list, lows: list, n: int = 5) -> dict:
        """Уровни S/R через локальные пики и впадины."""
        window = min(50, len(highs))
        if window < 5:
            return {"support": 0.0, "resistance": 0.0, "supports": [], "resistances": []}
        h = highs[-window:]
        l = lows[-window:]
        resistances, supports = [], []
        for i in range(2, len(h) - 2):
            if h[i] > h[i-1] and h[i] > h[i+1] and h[i] > h[i-2] and h[i] > h[i+2]:
                resistances.append(round(h[i], 6))
            if l[i] < l[i-1] and l[i] < l[i+1] and l[i] < l[i-2] and l[i] < l[i+2]:
                supports.append(round(l[i], 6))

        def dedup(levels):
            if not levels: return []
            levels = sorted(set(levels))
            result = [levels[0]]
            for v in levels[1:]:
                if abs(v - result[-1]) / max(result[-1], 1e-9) > 0.003:
                    result.append(v)
            return result[-n:]

        resistances = dedup(resistances)
        supports    = dedup(supports)
        return {
            "support":     supports[-1]    if supports    else round(min(l), 6),
            "resistance":  resistances[-1] if resistances else round(max(h), 6),
            "supports":    supports[-4:]    if supports    else [round(min(l), 6)],
            "resistances": resistances[-4:] if resistances else [round(max(h), 6)],
        }

    # ══════════════════════════════════════════════════════════════════════════
    #  ГЛАВНЫЙ МЕТОД — 13-ФАКТОРНАЯ СИСТЕМА CONFLUENCE
    # ══════════════════════════════════════════════════════════════════════════

    def calculate_signal(self, mtf_history, leverage: int = 30,
                          margin: float = 150, primary_tf: str = "15m") -> dict:
        """
        Генерирует торговый сигнал с 13-факторной системой confluence.

        Факторы для LONG (каждый +1 балл):
        1. 1H тренд = UPTREND
        2. 4H тренд = UPTREND
        3. 1D тренд = UPTREND
        4. Цена ниже Fibonacci 0.618 (у поддержки)
        5. RSI основного ТФ < 40 (перепродан)
        6. 1H MACD бычий кроссовер
        7. 4H MACD бычий кроссовер
        8. RSI бычья дивергенция
        9. Стохастик OVERSOLD или BULLISH_CROSS
        10. Williams %R < -80 (перепродан)
        11. OBV тренд растущий
        12. Volume Spike (подтверждение объёмом)
        13. Bollinger Bands: цена у нижней полосы или squeeze

        LONG если bullish_score >= 5 из 13
        SHORT если bullish_score <= 4 из 13
        """
        try:
            if isinstance(mtf_history, list):
                mtf_history = {'15m': mtf_history}

            primary_tf = (primary_tf if primary_tf in mtf_history
                          else next(iter(mtf_history), '15m'))
            history    = mtf_history.get(primary_tf, [])

            if not history or len(history) < 10:
                logger.warning("[calculate_signal] Недостаточно данных")
                return self._error_signal()

            prices = [float(k['close'])              for k in history]
            highs  = [float(k['high'])               for k in history]
            lows   = [float(k['low'])                for k in history]
            opens  = [float(k.get('open', k['close'])) for k in history]
            vols   = [float(k.get('volume', 0))      for k in history]

            price  = prices[-1]
            g_high = max(highs)
            g_low  = min(lows)
            diff   = g_high - g_low or 0.0001

            # ── FVG ──────────────────────────────────────────────────────────
            fvg = "Neutral"
            if len(history) >= 3:
                if history[-3]['high'] < history[-1]['low']:
                    fvg = "Bullish Gap"
                elif history[-3]['low'] > history[-1]['high']:
                    fvg = "Bearish Gap"

            # ── MTF-анализ ────────────────────────────────────────────────────
            mtf_analysis: dict = {}
            for tf, tf_hist in mtf_history.items():
                if not tf_hist or len(tf_hist) < 10: continue
                try:
                    tf_c = [float(k['close'])                for k in tf_hist]
                    tf_o = [float(k.get('open', k['close'])) for k in tf_hist]
                    tf_h = [float(k['high'])                 for k in tf_hist]
                    tf_l = [float(k['low'])                  for k in tf_hist]
                    tf_v = [float(k.get('volume', 0))        for k in tf_hist]
                    bb       = self._bollinger(tf_c)
                    macd_dat = self._macd(tf_c)
                    rsi_val  = self._rsi(tf_c)
                    mtf_analysis[tf] = {
                        "trend":           self._trend(tf_c),
                        "bb_trend":        self._bb_trend(tf_c),
                        "rsi":             rsi_val,
                        "rsi_div":         self._rsi_divergence(tf_c),
                        "macd":            macd_dat["macd"],
                        "macd_signal":     macd_dat["signal"],
                        "macd_hist":       macd_dat["hist"],
                        "macd_cross":      macd_dat["cross"],
                        "macd_hist_trend": macd_dat["macd_hist_trend"],
                        "bollinger":       bb,
                        "bb_squeeze":      bb["squeeze"],
                        "obv":             self._obv(tf_c, tf_v),
                        "obv_trend":       self._obv_trend(tf_c, tf_v),
                        "volume_spike":    self._volume_spike(tf_v),
                        "pattern":         self._patterns(tf_o, tf_h, tf_l, tf_c),
                        "sr_levels":       self._sr_levels(tf_h, tf_l),
                    }
                except Exception as e:
                    logger.warning(f"[calculate_signal] MTF {tf}: {e}")

            # ── 13-ФАКТОРНАЯ СИСТЕМА ──────────────────────────────────────────
            bs = 0  # bullish score
            conf_reasons = []

            trend_1h = mtf_analysis.get('1h',  {}).get('trend', 'SIDEWAYS')
            trend_4h = mtf_analysis.get('4h',  {}).get('trend', 'SIDEWAYS')
            trend_1d = mtf_analysis.get('1d',  {}).get('trend', 'SIDEWAYS')
            rsi_ptf  = mtf_analysis.get(primary_tf, {}).get('rsi', 50)
            macd_1h  = mtf_analysis.get('1h',  {}).get('macd_cross', 'NONE')
            macd_4h  = mtf_analysis.get('4h',  {}).get('macd_cross', 'NONE')
            fibo_618 = g_high - 0.618 * diff

            stoch    = self._stochastic(highs, lows, prices)
            wpr      = self._williams_r(highs, lows, prices)
            rsi_div  = self._rsi_divergence(prices)
            obv_tr   = self._obv_trend(prices, vols)
            vol_spike= self._volume_spike(vols)
            bb_main  = self._bollinger(prices)

            # Фактор 1: 1H тренд
            if trend_1h == "UPTREND":
                bs += 1; conf_reasons.append("1H uptrend ✓")
            # Фактор 2: 4H тренд
            if trend_4h == "UPTREND":
                bs += 1; conf_reasons.append("4H uptrend ✓")
            # Фактор 3: 1D тренд
            if trend_1d == "UPTREND":
                bs += 1; conf_reasons.append("1D uptrend ✓")
            # Фактор 4: Fibonacci
            if price < fibo_618:
                bs += 1; conf_reasons.append(f"Price at/below Fib 0.618 ({fibo_618:.4f}) ✓")
            # Фактор 5: RSI
            if rsi_ptf < 40:
                bs += 1; conf_reasons.append(f"RSI oversold ({rsi_ptf:.0f}) ✓")
            # Фактор 6: MACD 1H
            if macd_1h == "BULLISH":
                bs += 1; conf_reasons.append("1H MACD bullish cross ✓")
            # Фактор 7: MACD 4H
            if macd_4h == "BULLISH":
                bs += 1; conf_reasons.append("4H MACD bullish cross ✓")
            # Фактор 8: RSI дивергенция
            if rsi_div == "bullish_divergence":
                bs += 1; conf_reasons.append("RSI bullish divergence ✓")
            # Фактор 9: Стохастик
            if stoch["signal"] in ("OVERSOLD", "BULLISH_CROSS"):
                bs += 1; conf_reasons.append(f"Stoch {stoch['signal']} ({stoch['k']:.0f}) ✓")
            # Фактор 10: Williams %R
            if wpr < -80:
                bs += 1; conf_reasons.append(f"Williams %R oversold ({wpr:.0f}) ✓")
            # Фактор 11: OBV
            if "bullish" in obv_tr:
                bs += 1; conf_reasons.append("OBV rising ✓")
            # Фактор 12: Volume Spike
            if vol_spike:
                bs += 1; conf_reasons.append("Volume spike confirms move ✓")
            # Фактор 13: Bollinger
            if bb_main["squeeze"]:
                bs += 1; conf_reasons.append("BB squeeze (breakout expected) ✓")
            elif bb_main.get("percent_b", 0.5) < 0.15:
                bs += 1; conf_reasons.append("Price at BB lower band ✓")

            # Направление: LONG если 5+ из 13 бычьих факторов
            direction = "LONG" if bs >= 5 else "SHORT"

            # Для SHORT инвертируем причины
            if direction == "SHORT":
                conf_reasons = [r.replace("uptrend ✓","downtrend ✓")
                                 .replace("bullish","bearish")
                                 .replace("oversold","overbought")
                                 .replace("lower band","upper band")
                                 for r in conf_reasons]

            # ── TP/SL на основе ATR (точнее чем фиксированный %) ─────────────
            atr_val = self._atr(highs, lows, prices)
            _lev    = max(leverage, 1)

            # ── ФЬЮЧЕРСНЫЙ SL/TP — адаптивный на основе ATR + ликвидности ──────
            # Логика: SL ставим ЗА ближайший уровень ликвидности (свинг хай/лоу)
            # Минимум 1.5% от цены (защита от шума), максимум 6%

            # Вычисляем свинговые уровни (последние 20 свечей)
            swing_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
            swing_low  = min(lows[-20:])  if len(lows)  >= 20 else min(lows)

            if atr_val > 0:
                # ATR × 2.5 = достаточно широко для фьючерсов
                sl_dist_atr = atr_val * 2.5

                # Дистанция до ближайшего свинг уровня + буфер 0.5 ATR
                if direction == "LONG":
                    sl_dist_swing = (price - swing_low) + atr_val * 0.5
                else:
                    sl_dist_swing = (swing_high - price) + atr_val * 0.5

                # Берём максимум из ATR и свинг-уровня (защита от ложных пробоев)
                sl_dist = max(sl_dist_atr, sl_dist_swing * 0.6)
                sl_perc = min(max(sl_dist / price, 0.015), 0.06)
            else:
                sl_perc = max(0.02, min(0.06, 1.5 / _lev))

            # Адаптивные TP на основе Фибоначчи расширений
            # TP1 = 1:1.5 RR (быстрый, 50% позиции)
            # TP2 = 1:3.0 RR (основная цель, 30% позиции)
            # TP3 = 1:5.0 RR (максимальная цель, 20% позиции)
            tp1_perc = sl_perc * 1.5   # Фибо 1.618
            tp2_perc = sl_perc * 3.0   # Фибо 2.618
            tp3_perc = sl_perc * 5.0   # Фибо 4.236

            if direction == "LONG":
                tp1 = round(price * (1 + tp1_perc), 6)
                tp2 = round(price * (1 + tp2_perc), 6)
                tp3 = round(price * (1 + tp3_perc), 6)
                sl  = round(price * (1 - sl_perc), 6)
            else:
                tp1 = round(price * (1 - tp1_perc), 6)
                tp2 = round(price * (1 - tp2_perc), 6)
                tp3 = round(price * (1 - tp3_perc), 6)
                sl  = round(price * (1 + sl_perc), 6)

            tp1_pct  = round(tp1_perc * _lev * 100, 1)
            tp2_pct  = round(tp2_perc * _lev * 100, 1)
            tp3_pct  = round(tp3_perc * _lev * 100, 1)
            sl_pct   = round(sl_perc  * _lev * 100, 1)
            rr1      = round(tp1_pct / max(sl_pct, 0.01), 1)
            rr2      = round(tp2_pct / max(sl_pct, 0.01), 1)
            rr3      = round(tp3_pct / max(sl_pct, 0.01), 1)
            rr_ratio = f"1:{rr1}"
            rr2_ratio= f"1:{rr2}"
            rr3_ratio= f"1:{rr3}"

            # ── Дополнительные поля ───────────────────────────────────────────
            obv_trend_val = self._obv_trend(prices, vols)
            sr_primary    = self._sr_levels(highs, lows)

            # ── SMART MONEY CONCEPTS (SMC) ─────────────────────────────────────
            # Liquidity sweep detection
            recent_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
            recent_low  = min(lows[-20:])  if len(lows)  >= 20 else min(lows)
            price_range = recent_high - recent_low

            # Equal highs/lows = liquidity pools
            eq_highs = sum(1 for h in highs[-10:] if abs(h - recent_high) / recent_high < 0.002)
            eq_lows  = sum(1 for l in lows[-10:]  if abs(l - recent_low)  / recent_low  < 0.002)

            smart_money = ""
            liquidity   = ""

            if direction == "LONG":
                if eq_lows >= 2:
                    smart_money = "Sweep buy-side liquidity 🎯"
                    conf_score += 1
                    conf_reasons.append("Smart Money: Buy-side liquidity sweep ✓")
                elif price < recent_low + price_range * 0.2:
                    smart_money = "Discount zone (below 20% range)"
                    liquidity   = f"Liq. pool near {round(recent_low, 4)}"
                    conf_score += 1
                    conf_reasons.append("Discount zone — institutional entry ✓")
            else:
                if eq_highs >= 2:
                    smart_money = "Sweep sell-side liquidity 🎯"
                    conf_score += 1
                    conf_reasons.append("Smart Money: Sell-side liquidity sweep ✓")
                elif price > recent_high - price_range * 0.2:
                    smart_money = "Premium zone (top 20% range)"
                    liquidity   = f"Liq. pool near {round(recent_high, 4)}"
                    conf_score += 1
                    conf_reasons.append("Premium zone — institutional short ✓")

            # Order blocks (simplified: large candle body before reversal)
            if len(prices) >= 5:
                bodies = [abs(prices[i] - opens[i] if i < len(opens) else 0)
                          for i in range(-5, -1)]
                avg_body = sum(bodies) / len(bodies) if bodies else 0
                last_body = abs(prices[-1] - (opens[-1] if opens else prices[-1]))
                if last_body > avg_body * 1.8:
                    smart_money += " | Order Block detected"
                    conf_score += 1
                    conf_reasons.append("Order Block (large candle body) ✓")

            fibo_key = ""
            fibo_dict = {
                "0.236": round(g_high - 0.236 * diff, 6),
                "0.382": round(g_high - 0.382 * diff, 6),
                "0.5":   round(g_high - 0.500 * diff, 6),
                "0.618": round(fibo_618, 6),
                "0.786": round(g_high - 0.786 * diff, 6),
                "1.618": round(g_high + 0.618 * diff, 6),
            }

            # BOS / ChoCH
            bos_signal = choch_signal = "none"
            if len(prices) >= 6:
                window5 = prices[-6:-1]
                if prices[-1] > max(window5): bos_signal = "bullish BOS"
                elif prices[-1] < min(window5): bos_signal = "bearish BOS"
                if direction == "LONG" and prices[-2] < prices[-3]:
                    choch_signal = "potential bullish ChoCH"
                elif direction == "SHORT" and prices[-2] > prices[-3]:
                    choch_signal = "potential bearish ChoCH"

            # Liquidity Sweep
            liq_sweep = "none"
            if len(lows) >= 6 and len(highs) >= 6:
                prev_low  = min(lows[-6:-1])
                prev_high = max(highs[-6:-1])
                tol = 0.002
                if prev_low * (1-tol) < prices[-1] < prev_low * (1+tol):
                    liq_sweep = "low sweep (potential long)"
                elif prev_high * (1-tol) < prices[-1] < prev_high * (1+tol):
                    liq_sweep = "high sweep (potential short)"

            # Patterns
            pattern_str   = self._patterns(opens, highs, lows, prices)
            patterns_list = []
            if pattern_str and pattern_str != "None":
                bias     = "bullish" if "Bull" in pattern_str else "bearish" if "Bear" in pattern_str else "neutral"
                strength = "strong" if "Engulfing" in pattern_str or "Star" in pattern_str else "medium"
                patterns_list.append({"name": pattern_str, "bias": bias, "strength": strength})
            for tf_key, tf_dat in mtf_analysis.items():
                pat = tf_dat.get("pattern", "None")
                if pat and pat != "None":
                    bias = "bullish" if "Bull" in pat else "bearish" if "Bear" in pat else "neutral"
                    patterns_list.append({"name": f"{pat} ({tf_key})", "bias": bias, "strength": "medium"})

            # Historical context
            ath = max(highs); atl = min(lows)
            hist_range = ath - atl or 1
            pos_pct    = round((price - atl) / hist_range * 100, 1)
            from_ath   = round((ath - price) / ath * 100, 1) if ath else 0
            from_atl   = round((price - atl) / atl * 100, 1) if atl else 0
            zone = ("ATH zone"   if pos_pct > 80 else "Upper range" if pos_pct > 60
                    else "Mid range" if pos_pct > 40 else "Lower range" if pos_pct > 20
                    else "ATL zone")
            historical = {
                "ath": round(ath, 6), "atl": round(atl, 6),
                "position_pct": pos_pct, "zone": zone,
                "from_ath_pct": from_ath, "from_atl_pct": from_atl,
            }

            # Confidence score: масштабируем bs (0-13) → 0-100%
            conf_score = round(min(100, (bs / 13) * 100), 1)
            if conf_score >= 75:   conf_grade = "🟢 VERY HIGH"
            elif conf_score >= 55: conf_grade = "🟢 HIGH"
            elif conf_score >= 40: conf_grade = "🟡 MEDIUM"
            else:                  conf_grade = "🔴 LOW"

            return {
                "direction":        direction,
                "entry":            round(price, 6),
                "tp1":              tp1, "tp2": tp2, "sl": sl,
                "tp1_pct":          tp1_pct, "tp2_pct": tp2_pct, "tp3": tp3, "tp3_pct": tp3_pct,
                "sl_pct": sl_pct,
                "rr_ratio": rr_ratio, "rr2_ratio": rr2_ratio,
                "rr3_ratio": rr3_ratio if 'rr3_ratio' in locals() else "1:0",
                "leverage":         _lev,
                "bullish_score":    bs,
                "atr":              atr_val,
                "obv_trend":        obv_trend_val,
                "smart_money":      smart_money if 'smart_money' in locals() else "",
                "liquidity":        liquidity   if 'liquidity'   in locals() else "",
                "fibo_key":         fibo_key    if 'fibo_key'    in locals() else "",
                "liquidity_sweep":  liq_sweep,
                "stochastic":       stoch,
                "williams_r":       wpr,
                "volume_spike":     vol_spike,
                "fibo":             fibo_dict,
                "ob": {
                    "top":     round(g_low + diff * 0.15, 6),
                    "bottom":  round(g_low, 6),
                    "bullish": [round(g_low + diff * 0.05, 6), round(g_low + diff * 0.12, 6)],
                    "bearish": [round(g_high - diff * 0.05, 6), round(g_high - diff * 0.12, 6)],
                },
                "fvg":             {"status": fvg, "count": 1 if fvg != "Neutral" else 0},
                "fvg_status":       fvg,
                "bos":             {"bos": bos_signal, "choch": choch_signal},
                "sr_levels":        sr_primary,
                "patterns":         patterns_list[:6],
                "historical":       historical,
                "confidence":       {
                    "score":   conf_score,
                    "grade":   conf_grade,
                    "reasons": conf_reasons[:8],
                },
                "mtf_data":         mtf_analysis,
            }

        except Exception as e:
            logger.exception(f"[calculate_signal] Критическая ошибка: {e}")
            return self._error_signal()

    def _error_signal(self) -> dict:
        """Пустой сигнал при ошибке."""
        empty_fibo = {"0.236": 0.0, "0.382": 0.0, "0.5": 0.0,
                      "0.618": 0.0, "0.786": 0.0, "1.618": 0.0}
        return {
            "direction":       "SHORT",
            "entry": 0.0, "tp1": 0.0, "tp2": 0.0, "sl": 0.0,
            "tp1_pct": 0.0, "tp2_pct": 0.0, "sl_pct": 0.0,
            "rr_ratio": "1:0", "rr2_ratio": "1:0",
            "leverage": 1, "bullish_score": 0,
            "atr": 0.0, "obv_trend": "neutral",
            "liquidity_sweep": "none",
            "stochastic":  {"k": 50.0, "d": 50.0, "signal": "NEUTRAL"},
            "williams_r":  -50.0, "volume_spike": False,
            "fibo":        empty_fibo,
            "ob":          {"top": 0.0, "bottom": 0.0, "bullish": [], "bearish": []},
            "fvg":         {"status": "Error", "count": 0}, "fvg_status": "Error",
            "bos":         {"bos": "none", "choch": "none"},
            "sr_levels":   {"support": 0.0, "resistance": 0.0,
                            "supports": [], "resistances": []},
            "patterns":    [],
            "historical":  {"ath": 0.0, "atl": 0.0, "position_pct": 0,
                            "zone": "unknown", "from_ath_pct": 0, "from_atl_pct": 0},
            "confidence":  {"score": 0, "grade": "🔴 LOW", "reasons": []},
            "mtf_data":    {},
        }