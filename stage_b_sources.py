"""Public, read-only Spot market adapters used by Stage B.1."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from telemetry import record_api_request


TIMEFRAME_SECONDS = {"4h": 14_400, "1d": 86_400, "1w": 604_800}


def as_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and result not in (float("inf"), float("-inf")) else None


@dataclass(frozen=True)
class SpotMarket:
    exchange: str
    native_symbol: str
    base: str
    quote: str
    price: float
    quote_volume: float
    quote_volume_exact: bool = True


@dataclass(frozen=True)
class Candle:
    open_ms: int
    close: float
    quote_volume: float | None
    complete: bool
    high: float | None = None
    low: float | None = None


class PublicSpotAdapter:
    name = ""
    base_url = ""

    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urlencode({key: value for key, value in (params or {}).items() if value is not None})
        url = f"{self.base_url}{path}" + (f"?{query}" if query else "")
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "ALTCOIN_RADAR/Stage-B.1",
                "Cache-Control": "no-cache",
            },
        )
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                record_api_request(self.name)
                with urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt == 0:
                    time.sleep(0.5)
        raise RuntimeError(f"{self.name} request failed: {last_error}") from last_error

    def discover(self, target_symbols: set[str], quotes: set[str]) -> list[SpotMarket]:
        raise NotImplementedError

    def candles(self, market: SpotMarket, timeframe: str, limit: int = 120) -> list[Candle]:
        raise NotImplementedError

    @staticmethod
    def _complete_from_open(open_ms: int, timeframe: str, now_ms: int | None = None) -> bool:
        current = now_ms if now_ms is not None else int(time.time() * 1000)
        return open_ms + TIMEFRAME_SECONDS[timeframe] * 1000 <= current


class BinanceAdapter(PublicSpotAdapter):
    name = "Binance"
    base_url = "https://api.binance.com"

    def discover(self, target_symbols, quotes):
        info = self.get("/api/v3/exchangeInfo")
        tickers = self.get("/api/v3/ticker/24hr")
        ticker_map = {item.get("symbol"): item for item in tickers}
        markets = []
        for item in info.get("symbols", []):
            base, quote = item.get("baseAsset"), item.get("quoteAsset")
            if (
                base not in target_symbols
                or quote not in quotes
                or item.get("status") != "TRADING"
                or item.get("isSpotTradingAllowed") is False
            ):
                continue
            ticker = ticker_map.get(item.get("symbol"), {})
            price, volume = as_float(ticker.get("lastPrice")), as_float(ticker.get("quoteVolume"))
            if price and volume and volume > 0:
                markets.append(SpotMarket(self.name, item["symbol"], base, quote, price, volume))
        return markets

    def candles(self, market, timeframe, limit=120):
        rows = self.get(
            "/api/v3/klines",
            {"symbol": market.native_symbol, "interval": timeframe, "limit": limit},
        )
        now_ms = int(time.time() * 1000)
        result = []
        for row in rows:
            close, quote_volume = as_float(row[4]), as_float(row[7])
            if close is not None:
                result.append(
                    Candle(
                        int(row[0]), close, quote_volume, int(row[6]) <= now_ms,
                        high=as_float(row[2]), low=as_float(row[3])
                    )
                )
        return sorted(result, key=lambda item: item.open_ms)


class MexcAdapter(BinanceAdapter):
    name = "MEXC"
    base_url = "https://api.mexc.com"

    def discover(self, target_symbols, quotes):
        info = self.get("/api/v3/exchangeInfo")
        tickers = self.get("/api/v3/ticker/24hr")
        ticker_map = {item.get("symbol"): item for item in tickers}
        markets = []
        for item in info.get("symbols", []):
            base, quote = item.get("baseAsset"), item.get("quoteAsset")
            status = str(item.get("status", "")).upper()
            if base not in target_symbols or quote not in quotes or status not in {"1", "ENABLED", "TRADING"}:
                continue
            ticker = ticker_map.get(item.get("symbol"), {})
            price, volume = as_float(ticker.get("lastPrice")), as_float(ticker.get("quoteVolume"))
            if price and volume and volume > 0:
                markets.append(SpotMarket(self.name, item["symbol"], base, quote, price, volume))
        return markets


class OkxAdapter(PublicSpotAdapter):
    name = "OKX"
    base_url = "https://www.okx.com"

    def discover(self, target_symbols, quotes):
        instruments = self.get("/api/v5/public/instruments", {"instType": "SPOT"}).get("data", [])
        tickers = self.get("/api/v5/market/tickers", {"instType": "SPOT"}).get("data", [])
        ticker_map = {item.get("instId"): item for item in tickers}
        markets = []
        for item in instruments:
            base, quote = item.get("baseCcy"), item.get("quoteCcy")
            if base not in target_symbols or quote not in quotes or item.get("state") != "live":
                continue
            ticker = ticker_map.get(item.get("instId"), {})
            price, volume = as_float(ticker.get("last")), as_float(ticker.get("volCcy24h"))
            if price and volume and volume > 0:
                markets.append(SpotMarket(self.name, item["instId"], base, quote, price, volume))
        return markets

    def candles(self, market, timeframe, limit=120):
        bars = {"4h": "4Hutc", "1d": "1Dutc", "1w": "1Wutc"}
        rows = []
        after = None
        while len(rows) < limit:
            batch = self.get(
                "/api/v5/market/history-candles",
                {
                    "instId": market.native_symbol,
                    "bar": bars[timeframe],
                    "limit": min(limit - len(rows), 100),
                    "after": after,
                },
            ).get("data", [])
            if not batch:
                break
            existing = {str(item[0]) for item in rows}
            rows.extend(item for item in batch if str(item[0]) not in existing)
            oldest = min(int(item[0]) for item in batch)
            if after == oldest or len(batch) < min(limit - len(rows) + len(batch), 100):
                break
            after = oldest
        result = []
        for row in rows:
            close = as_float(row[4])
            if close is not None:
                quote_volume = as_float(row[7] if len(row) > 7 else row[6])
                confirmed = len(row) > 8 and str(row[8]) == "1"
                result.append(
                    Candle(
                        int(row[0]), close, quote_volume, confirmed,
                        high=as_float(row[2]), low=as_float(row[3])
                    )
                )
        return sorted(result, key=lambda item: item.open_ms)


class BybitAdapter(PublicSpotAdapter):
    name = "Bybit"
    base_url = "https://api.bybit.com"

    def discover(self, target_symbols, quotes):
        instruments = []
        cursor = None
        for _ in range(5):
            payload = self.get(
                "/v5/market/instruments-info",
                {"category": "spot", "limit": 1000, "cursor": cursor},
            ).get("result", {})
            instruments.extend(payload.get("list", []))
            cursor = payload.get("nextPageCursor")
            if not cursor:
                break
        tickers = self.get("/v5/market/tickers", {"category": "spot"}).get("result", {}).get("list", [])
        ticker_map = {item.get("symbol"): item for item in tickers}
        markets = []
        for item in instruments:
            base, quote = item.get("baseCoin"), item.get("quoteCoin")
            if base not in target_symbols or quote not in quotes or item.get("status") != "Trading":
                continue
            ticker = ticker_map.get(item.get("symbol"), {})
            price, volume = as_float(ticker.get("lastPrice")), as_float(ticker.get("turnover24h"))
            if price and volume and volume > 0:
                markets.append(SpotMarket(self.name, item["symbol"], base, quote, price, volume))
        return markets

    def candles(self, market, timeframe, limit=120):
        intervals = {"4h": "240", "1d": "D", "1w": "W"}
        rows = self.get(
            "/v5/market/kline",
            {
                "category": "spot",
                "symbol": market.native_symbol,
                "interval": intervals[timeframe],
                "limit": min(limit, 1000),
            },
        ).get("result", {}).get("list", [])
        result = []
        for row in rows:
            open_ms, close = int(row[0]), as_float(row[4])
            if close is not None:
                result.append(
                    Candle(
                        open_ms,
                        close,
                        as_float(row[6]),
                        self._complete_from_open(open_ms, timeframe),
                        high=as_float(row[2]),
                        low=as_float(row[3]),
                    )
                )
        return sorted(result, key=lambda item: item.open_ms)


class BitgetAdapter(PublicSpotAdapter):
    name = "Bitget"
    base_url = "https://api.bitget.com"

    def discover(self, target_symbols, quotes):
        instruments = self.get("/api/v2/spot/public/symbols").get("data", [])
        tickers = self.get("/api/v2/spot/market/tickers").get("data", [])
        ticker_map = {item.get("symbol"): item for item in tickers}
        markets = []
        for item in instruments:
            base, quote = item.get("baseCoin"), item.get("quoteCoin")
            if base not in target_symbols or quote not in quotes or item.get("status") != "online":
                continue
            ticker = ticker_map.get(item.get("symbol"), {})
            price = as_float(ticker.get("lastPr"))
            volume = as_float(ticker.get("quoteVolume") or ticker.get("usdtVolume"))
            if price and volume and volume > 0:
                markets.append(SpotMarket(self.name, item["symbol"], base, quote, price, volume))
        return markets

    def candles(self, market, timeframe, limit=120):
        intervals = {"4h": "4h", "1d": "1day", "1w": "1week"}
        rows = self.get(
            "/api/v2/spot/market/candles",
            {"symbol": market.native_symbol, "granularity": intervals[timeframe], "limit": min(limit, 1000)},
        ).get("data", [])
        result = []
        for row in rows:
            open_ms, close = int(row[0]), as_float(row[4])
            if close is not None:
                result.append(
                    Candle(
                        open_ms, close, as_float(row[6]), self._complete_from_open(open_ms, timeframe),
                        high=as_float(row[2]), low=as_float(row[3])
                    )
                )
        return sorted(result, key=lambda item: item.open_ms)


class GateAdapter(PublicSpotAdapter):
    name = "Gate"
    base_url = "https://api.gateio.ws/api/v4"

    def discover(self, target_symbols, quotes):
        instruments = self.get("/spot/currency_pairs")
        tickers = self.get("/spot/tickers")
        ticker_map = {item.get("currency_pair"): item for item in tickers}
        markets = []
        for item in instruments:
            base, quote = item.get("base"), item.get("quote")
            if base not in target_symbols or quote not in quotes or item.get("trade_status") != "tradable":
                continue
            ticker = ticker_map.get(item.get("id"), {})
            price, volume = as_float(ticker.get("last")), as_float(ticker.get("quote_volume"))
            if price and volume and volume > 0:
                markets.append(SpotMarket(self.name, item["id"], base, quote, price, volume))
        return markets

    def candles(self, market, timeframe, limit=120):
        intervals = {"4h": "4h", "1d": "1d", "1w": "7d"}
        rows = self.get(
            "/spot/candlesticks",
            {"currency_pair": market.native_symbol, "interval": intervals[timeframe], "limit": min(limit, 1000)},
        )
        result = []
        for row in rows:
            open_ms, close = int(row[0]) * 1000, as_float(row[2])
            if close is not None:
                result.append(
                    Candle(
                        open_ms, close, as_float(row[1]), self._complete_from_open(open_ms, timeframe),
                        high=as_float(row[3]), low=as_float(row[4])
                    )
                )
        return sorted(result, key=lambda item: item.open_ms)


class KucoinAdapter(PublicSpotAdapter):
    name = "KuCoin"
    base_url = "https://api.kucoin.com"

    def discover(self, target_symbols, quotes):
        instruments = self.get("/api/v2/symbols").get("data", [])
        tickers = self.get("/api/v1/market/allTickers").get("data", {}).get("ticker", [])
        ticker_map = {item.get("symbol"): item for item in tickers}
        markets = []
        for item in instruments:
            base, quote = item.get("baseCurrency"), item.get("quoteCurrency")
            if base not in target_symbols or quote not in quotes or item.get("enableTrading") is not True:
                continue
            ticker = ticker_map.get(item.get("symbol"), {})
            price, volume = as_float(ticker.get("last")), as_float(ticker.get("volValue"))
            if price and volume and volume > 0:
                markets.append(SpotMarket(self.name, item["symbol"], base, quote, price, volume))
        return markets

    def candles(self, market, timeframe, limit=120):
        intervals = {"4h": "4hour", "1d": "1day", "1w": "1week"}
        seconds = TIMEFRAME_SECONDS[timeframe]
        now = int(time.time())
        rows = self.get(
            "/api/v1/market/candles",
            {
                "symbol": market.native_symbol,
                "type": intervals[timeframe],
                "startAt": now - seconds * (limit + 2),
                "endAt": now,
            },
        ).get("data", [])
        result = []
        for row in rows:
            open_ms, close = int(row[0]) * 1000, as_float(row[2])
            if close is not None:
                result.append(
                    Candle(
                        open_ms, close, as_float(row[6]), self._complete_from_open(open_ms, timeframe),
                        high=as_float(row[3]), low=as_float(row[4])
                    )
                )
        return sorted(result, key=lambda item: item.open_ms)


class CoinbaseAdapter(PublicSpotAdapter):
    name = "Coinbase"
    base_url = "https://api.coinbase.com/api/v3/brokerage"

    def discover(self, target_symbols, quotes):
        products = self.get("/market/products", {"limit": 1000}).get("products", [])
        markets = []
        for item in products:
            base = item.get("base_currency_id") or item.get("base_name")
            quote = item.get("quote_currency_id") or item.get("quote_name")
            if base not in target_symbols or quote not in quotes:
                continue
            if item.get("trading_disabled") is True or item.get("view_only") is True:
                continue
            price = as_float(item.get("price"))
            quote_volume = as_float(item.get("approximate_quote_24h_volume"))
            exact = quote_volume is not None
            if quote_volume is None:
                base_volume = as_float(item.get("volume_24h"))
                quote_volume = base_volume * price if base_volume is not None and price is not None else None
            if price and quote_volume and quote_volume > 0:
                markets.append(
                    SpotMarket(self.name, item["product_id"], base, quote, price, quote_volume, exact)
                )
        return markets

    def _raw_candles(self, market, granularity, seconds, limit):
        result = []
        end = int(time.time())
        while len(result) < limit:
            batch_limit = min(limit - len(result), 300)
            start = end - seconds * (batch_limit + 2)
            payload = self.get(
                f"/market/products/{market.native_symbol}/candles",
                {
                    "start": start,
                    "end": end,
                    "granularity": granularity,
                    "limit": batch_limit,
                },
            )
            rows = payload.get("candles", [])
            if not rows:
                break
            known = {item.open_ms for item in result}
            for row in rows:
                open_ms = int(row["start"]) * 1000
                close, base_volume = as_float(row.get("close")), as_float(row.get("volume"))
                if close is not None and open_ms not in known:
                    quote_volume = close * base_volume if base_volume is not None else None
                    result.append(
                        Candle(
                            open_ms, close, quote_volume,
                            open_ms + seconds * 1000 <= int(time.time() * 1000),
                            high=as_float(row.get("high")), low=as_float(row.get("low"))
                        )
                    )
            oldest = min(int(row["start"]) for row in rows)
            if oldest >= end or len(rows) < batch_limit:
                break
            end = oldest - 1
        return sorted(result, key=lambda item: item.open_ms)[-limit:]

    def candles(self, market, timeframe, limit=120):
        if timeframe == "4h":
            return self._raw_candles(market, "FOUR_HOUR", TIMEFRAME_SECONDS["4h"], limit)
        if timeframe == "1d":
            return self._raw_candles(market, "ONE_DAY", TIMEFRAME_SECONDS["1d"], limit)
        daily = self._raw_candles(market, "ONE_DAY", TIMEFRAME_SECONDS["1d"], min(limit * 7 + 7, 300))
        buckets: dict[int, list[Candle]] = {}
        for candle in daily:
            week = candle.open_ms // (TIMEFRAME_SECONDS["1w"] * 1000)
            buckets.setdefault(week, []).append(candle)
        weekly = []
        for week, group in sorted(buckets.items()):
            if len(group) == 7 and all(item.complete for item in group):
                volumes = [item.quote_volume for item in group if item.quote_volume is not None]
                weekly.append(
                    Candle(
                        min(item.open_ms for item in group),
                        group[-1].close,
                        sum(volumes) if len(volumes) == 7 else None,
                        True,
                        high=max(item.high for item in group if item.high is not None),
                        low=min(item.low for item in group if item.low is not None),
                    )
                )
        return weekly[-limit:]


class KrakenAdapter(PublicSpotAdapter):
    name = "Kraken"
    base_url = "https://api.kraken.com"

    def discover(self, target_symbols, quotes):
        payload = self.get("/0/public/AssetPairs")
        if payload.get("error"):
            raise RuntimeError(f"Kraken error: {payload['error']}")
        candidates = []
        for key, item in payload.get("result", {}).items():
            wsname = item.get("wsname", "")
            if "/" not in wsname:
                continue
            base, quote = wsname.split("/", 1)
            base = {"XBT": "BTC", "XDG": "DOGE"}.get(base, base)
            if base in target_symbols and quote in quotes and item.get("status", "online") == "online":
                candidates.append((key, item.get("altname", key), base, quote))
        if not candidates:
            return []
        ticker_payload = self.get("/0/public/Ticker", {"pair": ",".join(item[1] for item in candidates)})
        if ticker_payload.get("error"):
            raise RuntimeError(f"Kraken error: {ticker_payload['error']}")
        ticker_values = list(ticker_payload.get("result", {}).items())
        markets = []
        for _, native, base, quote in candidates:
            match = next((value for key, value in ticker_values if key == native or key.endswith(native)), None)
            if match is None and len(candidates) == 1 and len(ticker_values) == 1:
                match = ticker_values[0][1]
            if match is None:
                continue
            price = as_float(match.get("c", [None])[0])
            base_volume = as_float(match.get("v", [None, None])[1])
            quote_volume = price * base_volume if price is not None and base_volume is not None else None
            if price and quote_volume and quote_volume > 0:
                markets.append(SpotMarket(self.name, native, base, quote, price, quote_volume, False))
        return markets

    def candles(self, market, timeframe, limit=120):
        intervals = {"4h": 240, "1d": 1440, "1w": 10080}
        payload = self.get("/0/public/OHLC", {"pair": market.native_symbol, "interval": intervals[timeframe]})
        if payload.get("error"):
            raise RuntimeError(f"Kraken error: {payload['error']}")
        values = [value for key, value in payload.get("result", {}).items() if key != "last"]
        rows = values[0] if values else []
        result = []
        for row in rows[-limit:]:
            open_ms, close = int(row[0]) * 1000, as_float(row[4])
            vwap, base_volume = as_float(row[5]), as_float(row[6])
            quote_volume = vwap * base_volume if vwap is not None and base_volume is not None else None
            if close is not None:
                result.append(
                    Candle(
                        open_ms, close, quote_volume, self._complete_from_open(open_ms, timeframe),
                        high=as_float(row[2]), low=as_float(row[3])
                    )
                )
        return sorted(result, key=lambda item: item.open_ms)


ADAPTER_TYPES = {
    "binance": BinanceAdapter,
    "coinbase": CoinbaseAdapter,
    "okx": OkxAdapter,
    "bybit": BybitAdapter,
    "bitget": BitgetAdapter,
    "gate": GateAdapter,
    "kucoin": KucoinAdapter,
    "mexc": MexcAdapter,
    "kraken": KrakenAdapter,
}


def build_adapters(config: dict[str, Any]) -> list[PublicSpotAdapter]:
    timeout = int(config.get("request_timeout_seconds", 20))
    excluded = {str(item).casefold() for item in config.get("excluded_exchanges", [])}
    adapters = []
    for item in config.get("allowed_exchanges", []):
        if not item.get("enabled", False):
            continue
        name = str(item.get("name", ""))
        if name.casefold() in excluded:
            raise ValueError(f"excluded exchange cannot be enabled: {name}")
        adapter_type = ADAPTER_TYPES.get(item.get("adapter"))
        if adapter_type is None:
            raise ValueError(f"unknown exchange adapter: {item.get('adapter')}")
        adapters.append(adapter_type(timeout=timeout))
    return adapters
