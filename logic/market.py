# -*- coding: utf-8 -*-
"""
market.py — Сбор рыночных данных v4
──────────────────────────────────────
• Основной источник: MEXC Spot API
• Fallback цепочка: MEXC → Gate.io → Binance → CoinGecko
• Все API бесплатные, без ключей
• Binance добавлен т.к. MEXC/Gate блокируются на некоторых хостах
• TTL-кэш предотвращает перегрузку API
• MATIC→POL исправлен
"""
import requests
import logging
import time
import threading

logger = logging.getLogger("SYNAPSE_MARKET")


# ══════════════════════════════════════════════════════════════════════════════
#  TTL-КЭШ (потокобезопасный)
# ══════════════════════════════════════════════════════════════════════════════

class _TTLCache:
    def __init__(self):
        self._store: dict = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            entry = self._store.get(key)
            if entry and time.time() < entry['exp']:
                return entry['val']
        return None

    def set(self, key: str, value, ttl: int = 30):
        with self._lock:
            self._store[key] = {'val': value, 'exp': time.time() + ttl}

    def invalidate(self, key: str):
        with self._lock:
            self._store.pop(key, None)

    def clear_expired(self):
        """Очистка протухших записей (вызывать периодически)."""
        now = time.time()
        with self._lock:
            expired = [k for k, v in self._store.items() if now >= v['exp']]
            for k in expired:
                del self._store[k]


_cache = _TTLCache()

# Периодическая очистка кэша
def _cache_cleaner():
    while True:
        time.sleep(300)
        _cache.clear_expired()
threading.Thread(target=_cache_cleaner, daemon=True, name="CacheCleaner").start()


# ══════════════════════════════════════════════════════════════════════════════
#  КОНСТАНТЫ TTL (секунды)
# ══════════════════════════════════════════════════════════════════════════════
TTL_PRICE    = 15    # живая цена — 15 сек
TTL_KLINES   = 30    # свечи 15m/1h — 30 сек
TTL_KLINES_D = 120   # дневные/недельные — 2 мин
TTL_TOP100   = 60    # топ-100 — 1 мин


class MarketDataCollector:
    """
    Сбор рыночных данных.
    Fallback цепочка: MEXC → Gate.io → Binance → CoinGecko
    Все источники бесплатны и не требуют API-ключей.
    """

    def __init__(self):
        self.mexc_base    = "https://api.mexc.com/api/v3"
        self.gate_base    = "https://data.gateapi.io/api2/1/ticker"
        self.binance_base = "https://api.binance.com/api/v3"
        self.headers = {
            'User-Agent':    'SynapseX/4.0',
            'Cache-Control': 'no-cache',
            'Accept':        'application/json',
        }

        # Маппинг symbol → биржевые коды (авторасширяется при get_top_100)
        self.symbol_map: dict = {
            "BTC-USDT":   {"mexc": "BTCUSDT",   "gate": "btc_usdt",  "cg": "bitcoin"},
            "ETH-USDT":   {"mexc": "ETHUSDT",   "gate": "eth_usdt",  "cg": "ethereum"},
            "SOL-USDT":   {"mexc": "SOLUSDT",   "gate": "sol_usdt",  "cg": "solana"},
            "BNB-USDT":   {"mexc": "BNBUSDT",   "gate": "bnb_usdt",  "cg": "binancecoin"},
            "XRP-USDT":   {"mexc": "XRPUSDT",   "gate": "xrp_usdt",  "cg": "ripple"},
            "ADA-USDT":   {"mexc": "ADAUSDT",   "gate": "ada_usdt",  "cg": "cardano"},
            "DOGE-USDT":  {"mexc": "DOGEUSDT",  "gate": "doge_usdt", "cg": "dogecoin"},
            "DOT-USDT":   {"mexc": "DOTUSDT",   "gate": "dot_usdt",  "cg": "polkadot"},
            "MATIC-USDT": {"mexc": "POLUSDT",   "gate": "pol_usdt",  "cg": "matic-network"},
            "POL-USDT":   {"mexc": "POLUSDT",   "gate": "pol_usdt",  "cg": "matic-network"},
            "AVAX-USDT":  {"mexc": "AVAXUSDT",  "gate": "avax_usdt", "cg": "avalanche-2"},
            "LINK-USDT":  {"mexc": "LINKUSDT",  "gate": "link_usdt", "cg": "chainlink"},
            "LTC-USDT":   {"mexc": "LTCUSDT",   "gate": "ltc_usdt",  "cg": "litecoin"},
            "TRX-USDT":   {"mexc": "TRXUSDT",   "gate": "trx_usdt",  "cg": "tron"},
            "XLM-USDT":   {"mexc": "XLMUSDT",   "gate": "xlm_usdt",  "cg": "stellar"},
            "NEAR-USDT":  {"mexc": "NEARUSDT",  "gate": "near_usdt", "cg": "near"},
            "APT-USDT":   {"mexc": "APTUSDT",   "gate": "apt_usdt",  "cg": "aptos"},
            "ARB-USDT":   {"mexc": "ARBUSDT",   "gate": "arb_usdt",  "cg": "arbitrum"},
            "OP-USDT":    {"mexc": "OPUSDT",    "gate": "op_usdt",   "cg": "optimism"},
            "INJ-USDT":   {"mexc": "INJUSDT",   "gate": "inj_usdt",  "cg": "injective-protocol"},
            "SUI-USDT":   {"mexc": "SUIUSDT",   "gate": "sui_usdt",  "cg": "sui"},
            "PEPE-USDT":  {"mexc": "PEPEUSDT",  "gate": "pepe_usdt", "cg": "pepe"},
            "WIF-USDT":   {"mexc": "WIFUSDT",   "gate": "wif_usdt",  "cg": "dogwifcoin"},
            "TON-USDT":   {"mexc": "TONUSDT",   "gate": "ton_usdt",  "cg": "the-open-network"},
            "HYPE-USDT":  {"mexc": "HYPEUSDT",  "gate": "hype_usdt", "cg": "hyperliquid"},
        }

    # ── Утилиты ───────────────────────────────────────────────────────────────

    def _mapping(self, symbol: str) -> dict:
        """Возвращает маппинг символа, создаёт дефолтный если нет."""
        if symbol not in self.symbol_map:
            base = symbol.replace('-USDT', '').replace('-USDC', '')
            self.symbol_map[symbol] = {
                "mexc": f"{base}USDT",
                "gate": f"{base.lower()}_usdt",
                "cg":   "",
            }
        return self.symbol_map[symbol]

    @staticmethod
    def _kline_ttl(interval: str) -> int:
        return TTL_KLINES_D if interval in ('1d', '1w', '1W') else TTL_KLINES

    def _get(self, url: str, timeout: int = 4) -> dict | None:
        """Безопасный GET запрос, возвращает None при любой ошибке."""
        try:
            r = requests.get(url, headers=self.headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    # ── Топ-100 пар ───────────────────────────────────────────────────────────

    def get_top_100_mexc(self) -> list:
        """Топ-100 USDT-пар MEXC по объёму. Fallback на Binance, потом CoinGecko."""
        cache_key = "top100"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        # 1. MEXC
        try:
            r = requests.get(f"{self.mexc_base}/ticker/24hr",
                             headers=self.headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                usdt_pairs = [d for d in data if d['symbol'].endswith('USDT')]
                sorted_pairs = sorted(usdt_pairs,
                    key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
                top_100 = []
                for p in sorted_pairs[:100]:
                    raw = p['symbol']
                    base = raw.replace('USDT', '')
                    fmt = f"{base}-USDT"
                    if fmt not in self.symbol_map:
                        self.symbol_map[fmt] = {
                            "mexc": raw, "gate": f"{base.lower()}_usdt", "cg": ""}
                    top_100.append({
                        "symbol": fmt,
                        "base":   base,
                        "change": f"{float(p.get('priceChangePercent', 0)):+.2f}%",
                        "price":  float(p.get('lastPrice', 0)),
                        "volume": float(p.get('quoteVolume', 0)),
                    })
                if top_100:
                    _cache.set(cache_key, top_100, TTL_TOP100)
                    return top_100
        except Exception as e:
            logger.warning(f"[get_top_100] MEXC error: {e}")

        # 2. Binance fallback
        try:
            r = requests.get(f"{self.binance_base}/ticker/24hr",
                             headers=self.headers, timeout=6)
            if r.status_code == 200:
                data = r.json()
                usdt_pairs = [d for d in data if d['symbol'].endswith('USDT')]
                sorted_pairs = sorted(usdt_pairs,
                    key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
                top_100 = []
                for p in sorted_pairs[:100]:
                    raw = p['symbol']
                    base = raw.replace('USDT', '')
                    fmt = f"{base}-USDT"
                    if fmt not in self.symbol_map:
                        self.symbol_map[fmt] = {
                            "mexc": raw, "gate": f"{base.lower()}_usdt", "cg": ""}
                    top_100.append({
                        "symbol": fmt,
                        "base":   base,
                        "change": f"{float(p.get('priceChangePercent', 0)):+.2f}%",
                        "price":  float(p.get('lastPrice', 0)),
                        "volume": float(p.get('quoteVolume', 0)),
                    })
                if top_100:
                    _cache.set(cache_key, top_100, TTL_TOP100)
                    logger.info(f"[get_top_100] Binance fallback: {len(top_100)} пар")
                    return top_100
        except Exception as e:
            logger.warning(f"[get_top_100] Binance fallback error: {e}")

        # 3. CoinGecko fallback (медленнее)
        try:
            r = requests.get(
                "https://api.coingecko.com/api/v3/coins/markets"
                "?vs_currency=usd&order=market_cap_desc&per_page=50&page=1",
                headers=self.headers, timeout=6)
            if r.status_code == 200:
                cg_top = []
                for coin in r.json():
                    sym = coin['symbol'].upper() + "-USDT"
                    pct = float(coin.get('price_change_percentage_24h') or 0)
                    if sym not in self.symbol_map:
                        self.symbol_map[sym] = {
                            "mexc": coin['symbol'].upper() + "USDT",
                            "gate": coin['symbol'].lower() + "_usdt",
                            "cg":   coin['id']}
                    cg_top.append({
                        "symbol": sym, "base": coin['symbol'].upper(),
                        "change": f"{pct:+.2f}%",
                        "price":  float(coin.get('current_price', 0)),
                        "volume": float(coin.get('total_volume', 0)),
                    })
                if cg_top:
                    _cache.set(cache_key, cg_top, TTL_TOP100)
                    return cg_top
        except Exception as eg:
            logger.warning(f"[get_top_100] CoinGecko error: {eg}")

        # Последний резерв — локальный маппинг
        return [{"symbol": k, "base": k.split('-')[0], "change": "+0.00%", "price": 0, "volume": 0}
                for k in list(self.symbol_map.keys())]

    # ── Живая цена ────────────────────────────────────────────────────────────

    def get_live_metrics(self, symbol: str = "SOL-USDT") -> dict:
        """
        Живая цена. Fallback: MEXC → Gate.io → Binance → CoinGecko.
        Кэш TTL_PRICE секунд.
        """
        cache_key = f"price:{symbol}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        mapping = self._mapping(symbol)

        # 1. MEXC
        try:
            r = requests.get(
                f"{self.mexc_base}/ticker/price?symbol={mapping['mexc']}",
                headers=self.headers, timeout=3)
            if r.status_code == 200:
                result = {"price": float(r.json()['price']), "status": "LIVE_MEXC"}
                _cache.set(cache_key, result, TTL_PRICE)
                return result
        except Exception as e:
            logger.warning(f"[get_live_metrics] MEXC failed {symbol}: {e}")

        # 2. Gate.io
        try:
            r = requests.get(f"{self.gate_base}/{mapping['gate']}",
                             headers=self.headers, timeout=3)
            if r.status_code == 200:
                price = float(r.json().get('last', 0))
                if price > 0:
                    result = {"price": price, "status": "LIVE_GATEIO"}
                    _cache.set(cache_key, result, TTL_PRICE)
                    return result
        except Exception as e:
            logger.warning(f"[get_live_metrics] Gate.io failed {symbol}: {e}")

        # 3. Binance
        try:
            r = requests.get(
                f"{self.binance_base}/ticker/price?symbol={mapping['mexc']}",
                headers=self.headers, timeout=4)
            if r.status_code == 200:
                price = float(r.json().get('price', 0))
                if price > 0:
                    result = {"price": price, "status": "LIVE_BINANCE"}
                    _cache.set(cache_key, result, TTL_PRICE)
                    return result
        except Exception as e:
            logger.warning(f"[get_live_metrics] Binance failed {symbol}: {e}")

        # 4. CoinGecko
        try:
            cg_id = mapping.get('cg', '')
            if cg_id:
                r = requests.get(
                    f"https://api.coingecko.com/api/v3/simple/price"
                    f"?ids={cg_id}&vs_currencies=usd",
                    headers=self.headers, timeout=5)
                if r.status_code == 200:
                    price = float(r.json().get(cg_id, {}).get('usd', 0))
                    if price > 0:
                        result = {"price": price, "status": "LIVE_COINGECKO"}
                        _cache.set(cache_key, result, TTL_PRICE)
                        return result
        except Exception as e:
            logger.warning(f"[get_live_metrics] CoinGecko failed {symbol}: {e}")

        logger.error(f"[get_live_metrics] Все источники недоступны для {symbol}")
        return {"price": 0.0, "status": "OFFLINE"}

    # ── Тикер 24h ─────────────────────────────────────────────────────────────

    def get_ticker_24h(self, symbol: str = "SOL-USDT") -> dict:
        """
        Тикер 24h: цена, изменение %, объём, high, low.
        Совместим со всеми вызовами в app.py (change_24h и change_pct — оба возвращаются).
        Fallback: MEXC → Binance → только цена.
        """
        cache_key = f"ticker24:{symbol}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        mapping = self._mapping(symbol)

        def _build_result(d: dict, status: str) -> dict:
            pct = float(d.get('priceChangePercent', 0))
            return {
                "price":       float(d.get('lastPrice', d.get('price', 0))),
                "change_24h":  pct,
                "change_pct":  pct,
                "high":        float(d.get('highPrice', 0)),
                "low":         float(d.get('lowPrice', 0)),
                "high_24h":    float(d.get('highPrice', 0)),
                "low_24h":     float(d.get('lowPrice', 0)),
                "volume":      float(d.get('volume', 0)),
                "volume_usdt": float(d.get('quoteVolume', 0)),
                "status":      status,
            }

        # 1. MEXC
        try:
            r = requests.get(
                f"{self.mexc_base}/ticker/24hr?symbol={mapping['mexc']}",
                headers=self.headers, timeout=4)
            if r.status_code == 200:
                result = _build_result(r.json(), "LIVE_MEXC")
                _cache.set(cache_key, result, TTL_PRICE)
                return result
        except Exception as e:
            logger.warning(f"[get_ticker_24h] MEXC failed {symbol}: {e}")

        # 2. Binance
        try:
            r = requests.get(
                f"{self.binance_base}/ticker/24hr?symbol={mapping['mexc']}",
                headers=self.headers, timeout=4)
            if r.status_code == 200:
                result = _build_result(r.json(), "LIVE_BINANCE")
                _cache.set(cache_key, result, TTL_PRICE)
                return result
        except Exception as e:
            logger.warning(f"[get_ticker_24h] Binance failed {symbol}: {e}")

        # Fallback — только цена
        live = self.get_live_metrics(symbol)
        result = {
            "price": live.get('price', 0), "change_24h": 0.0, "change_pct": 0.0,
            "high": 0.0, "low": 0.0, "high_24h": 0.0, "low_24h": 0.0,
            "volume": 0.0, "volume_usdt": 0.0,
            "status": live.get('status', 'OFFLINE'),
        }
        _cache.set(cache_key, result, TTL_PRICE)
        return result

    # ── Исторические свечи ────────────────────────────────────────────────────

    def get_historical_klines(self, symbol: str = "SOL-USDT",
                               interval: str = "15m", limit: int = 100) -> list:
        """
        Свечи OHLCV с MEXC. Fallback на Binance если MEXC недоступен.
        interval: '1m','5m','15m','30m','1h','2h','4h','1d','1w'
        """
        interval_mexc    = self._normalize_interval(interval)
        interval_binance = self._normalize_interval_binance(interval)
        cache_key = f"klines:{symbol}:{interval}:{limit}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        mapping = self._mapping(symbol)

        def _parse_klines(data: list) -> list:
            result = []
            for k in data:
                try:
                    result.append({
                        "open": float(k[1]), "high": float(k[2]),
                        "low":  float(k[3]), "close": float(k[4]),
                        "volume": float(k[5]),
                    })
                except (IndexError, ValueError, TypeError):
                    continue
            return result

        # 1. MEXC
        try:
            url = (f"{self.mexc_base}/klines"
                   f"?symbol={mapping['mexc']}&interval={interval_mexc}&limit={limit}")
            r = requests.get(url, headers=self.headers, timeout=6)
            r.raise_for_status()
            result = _parse_klines(r.json())
            if result:
                ttl = self._kline_ttl(interval)
                _cache.set(cache_key, result, ttl)
                return result
        except Exception as e:
            logger.warning(f"[get_historical_klines] MEXC failed {symbol} {interval}: {e}")

        # 2. Binance fallback
        try:
            url = (f"{self.binance_base}/klines"
                   f"?symbol={mapping['mexc']}&interval={interval_binance}&limit={limit}")
            r = requests.get(url, headers=self.headers, timeout=6)
            r.raise_for_status()
            result = _parse_klines(r.json())
            if result:
                ttl = self._kline_ttl(interval)
                _cache.set(cache_key, result, ttl)
                logger.info(f"[get_historical_klines] Binance fallback OK: {symbol} {interval}")
                return result
        except Exception as e:
            logger.warning(f"[get_historical_klines] Binance failed {symbol} {interval}: {e}")

        return []

    # ── Батч цены ─────────────────────────────────────────────────────────────

    def get_prices_batch(self, symbols: list) -> dict:
        """Цены нескольких символов одним запросом. MEXC → Binance fallback."""
        cache_key = f"batch:{','.join(sorted(symbols))}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        result = {sym: {"price": 0.0, "status": "PENDING"} for sym in symbols}

        # 1. MEXC batch
        try:
            r = requests.get(f"{self.mexc_base}/ticker/price",
                             headers=self.headers, timeout=4)
            if r.status_code == 200:
                all_prices = {item['symbol']: float(item['price']) for item in r.json()}
                for sym in symbols:
                    mexc_sym = self._mapping(sym)['mexc']
                    if mexc_sym in all_prices:
                        result[sym] = {"price": all_prices[mexc_sym], "status": "LIVE_MEXC"}
                _cache.set(cache_key, result, TTL_PRICE)
                return result
        except Exception as e:
            logger.warning(f"[get_prices_batch] MEXC error: {e}")

        # 2. Binance batch
        try:
            r = requests.get(f"{self.binance_base}/ticker/price",
                             headers=self.headers, timeout=4)
            if r.status_code == 200:
                all_prices = {item['symbol']: float(item['price']) for item in r.json()}
                for sym in symbols:
                    mexc_sym = self._mapping(sym)['mexc']
                    if mexc_sym in all_prices:
                        result[sym] = {"price": all_prices[mexc_sym], "status": "LIVE_BINANCE"}
                _cache.set(cache_key, result, TTL_PRICE)
                return result
        except Exception as e:
            logger.warning(f"[get_prices_batch] Binance error: {e}")

        # Fallback — по одному
        for sym in symbols:
            data = self.get_live_metrics(sym)
            result[sym] = {"price": data['price'], "status": data['status']}

        _cache.set(cache_key, result, TTL_PRICE)
        return result

    # ── Нормализация интервалов ────────────────────────────────────────────────

    @staticmethod
    def _normalize_interval(interval: str) -> str:
        """MEXC Spot API v3: 1m 5m 15m 30m 60m 4h 1d 1W 1M"""
        aliases = {
            '1h': '60m', '2h': '4h', '3h': '4h', '6h': '4h',
            '8h': '4h', '12h': '4h', '1w': '1W', '60m': '60m',
            '120m': '60m', '180m': '60m', '240m': '4h',
            '1440m': '1d', '10080m': '1W',
        }
        result = aliases.get(interval, interval)
        valid = {'1m', '5m', '15m', '30m', '60m', '4h', '1d', '1W', '1M'}
        return result if result in valid else '60m'

    @staticmethod
    def _normalize_interval_binance(interval: str) -> str:
        """Binance использует стандартные интервалы: 1m 5m 15m 30m 1h 2h 4h 1d 1w"""
        aliases = {
            '60m': '1h', '120m': '2h', '240m': '4h',
            '1440m': '1d', '10080m': '1w', '1W': '1w',
        }
        result = aliases.get(interval, interval)
        valid = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h',
                 '8h', '12h', '1d', '3d', '1w', '1M'}
        return result if result in valid else '1h'