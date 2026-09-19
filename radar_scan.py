"""ALTCOIN RADAR Stage A: read-only CoinGecko price threshold scanner."""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from telemetry import record_api_request


PROJECT_ROOT = Path(__file__).resolve().parent
WATCHLIST_PATH = PROJECT_ROOT / "config" / "watchlist.json"
OUTPUT_DIR = PROJECT_ROOT / "output"
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
THRESHOLD_PERCENT = 10.0
CHUNK_SIZE = 100


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(path.name + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(temp_path, path)


def load_watchlist(path: Path = WATCHLIST_PATH) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        entries = json.load(handle)
    if not isinstance(entries, list):
        raise ValueError("watchlist.json must contain a JSON array")

    seen_symbols: set[str] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"watchlist entry {index} must be an object")
        symbol = entry.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(f"watchlist entry {index} has no valid symbol")
        symbol = symbol.upper()
        entry["symbol"] = symbol
        if symbol in seen_symbols:
            raise ValueError(f"duplicate watchlist symbol: {symbol}")
        seen_symbols.add(symbol)

        coin_id = entry.get("coingecko_id")
        if coin_id is not None and (not isinstance(coin_id, str) or not coin_id.strip()):
            raise ValueError(f"invalid coingecko_id for {symbol}")
        if coin_id is None and not entry.get("needs_review", False):
            raise ValueError(f"unmapped symbol {symbol} must set needs_review=true")
    return entries


def watchlist_stats(entries: Iterable[dict[str, Any]]) -> dict[str, int]:
    items = list(entries)
    enabled = [item for item in items if item.get("enabled", False)]
    mapped = [item for item in enabled if item.get("coingecko_id")]
    return {
        "watchlist_total": len(items),
        "enabled_total": len(enabled),
        "mapped_total": len(mapped),
        "unmapped_total": len(enabled) - len(mapped),
    }


def build_mapping_review(entries: Iterable[dict[str, Any]]) -> dict[str, Any]:
    review = []
    for item in entries:
        if item.get("enabled", False) and not item.get("coingecko_id"):
            review.append(
                {
                    "symbol": item["symbol"],
                    "coingecko_id": None,
                    "needs_review": True,
                    "reason": item.get("review_reason", "CoinGecko ID cannot be uniquely confirmed"),
                    "candidates": item.get("candidates", []),
                }
            )
    return {"generated_at": utc_now(), "count": len(review), "items": review}


def fetch_market_data(
    coin_ids: list[str],
    api_key: str | None = None,
    opener: Any = None,
) -> list[dict[str, Any]]:
    open_url = opener or urlopen
    headers = {"Accept": "application/json", "User-Agent": "ALTCOIN_RADAR/Stage-A"}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    records: list[dict[str, Any]] = []
    for start in range(0, len(coin_ids), CHUNK_SIZE):
        chunk = coin_ids[start : start + CHUNK_SIZE]
        params = {
            "vs_currency": "usd",
            "ids": ",".join(chunk),
            "price_change_percentage": "1h,24h",
            "precision": "full",
            "sparkline": "false",
        }
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                record_api_request("CoinGecko")
                request = Request(f"{API_URL}?{urlencode(params)}", headers=headers)
                with open_url(request, timeout=30) as response:
                    data = json.loads(response.read().decode("utf-8"))
                if not isinstance(data, list):
                    raise ValueError("CoinGecko response is not a list")
                records.extend(data)
                last_error = None
                break
            except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(2**attempt)
        if last_error is not None:
            raise RuntimeError(f"CoinGecko request failed: {last_error}") from last_error
    return records


def number_or_none(value: Any) -> float | int | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def enrich_record(raw: dict[str, Any], symbol: str, coin_id: str) -> dict[str, Any]:
    high = number_or_none(raw.get("high_24h"))
    low = number_or_none(raw.get("low_24h"))
    volume = number_or_none(raw.get("total_volume"))
    market_cap = number_or_none(raw.get("market_cap"))

    range_24h_pct = None
    if high is not None and low is not None and low > 0:
        range_24h_pct = (high - low) / low * 100

    turnover_ratio = None
    if volume is not None and market_cap is not None and market_cap > 0:
        turnover_ratio = volume / market_cap

    return {
        "symbol": symbol,
        "coingecko_id": coin_id,
        "current_price": number_or_none(raw.get("current_price")),
        "price_change_percentage_1h": number_or_none(raw.get("price_change_percentage_1h_in_currency")),
        "price_change_percentage_24h": number_or_none(raw.get("price_change_percentage_24h_in_currency")),
        "total_volume": volume,
        "market_cap": market_cap,
        "high_24h": high,
        "low_24h": low,
        "last_updated": raw.get("last_updated"),
        "range_24h_pct": range_24h_pct,
        "turnover_ratio": turnover_ratio,
    }


def trigger_flags(record: dict[str, Any]) -> dict[str, bool]:
    change_1h = record.get("price_change_percentage_1h")
    change_24h = record.get("price_change_percentage_24h")
    return {
        "trigger_1h_up": change_1h is not None and change_1h >= THRESHOLD_PERCENT,
        "trigger_1h_down": change_1h is not None and change_1h <= -THRESHOLD_PERCENT,
        "trigger_24h_up": change_24h is not None and change_24h >= THRESHOLD_PERCENT,
        "trigger_24h_down": change_24h is not None and change_24h <= -THRESHOLD_PERCENT,
    }


def build_triggers(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    triggers = []
    for record in records:
        flags = trigger_flags(record)
        if any(flags.values()):
            triggers.append({**record, **flags})
    return triggers


def _ranked(records: Iterable[dict[str, Any]], field: str, reverse: bool) -> list[dict[str, Any]]:
    available = [item for item in records if item.get(field) is not None]
    return sorted(available, key=lambda item: item[field], reverse=reverse)[:10]


def build_top_movers(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "gainers_1h": _ranked(records, "price_change_percentage_1h", True),
        "losers_1h": _ranked(records, "price_change_percentage_1h", False),
        "gainers_24h": _ranked(records, "price_change_percentage_24h", True),
        "losers_24h": _ranked(records, "price_change_percentage_24h", False),
    }


def sanity_check(records: Iterable[dict[str, Any]], triggers: Iterable[dict[str, Any]]) -> list[str]:
    trigger_symbols = {item["symbol"] for item in triggers}
    missing = []
    for item in records:
        changes = (
            item.get("price_change_percentage_1h"),
            item.get("price_change_percentage_24h"),
        )
        if any(value is not None and abs(value) >= THRESHOLD_PERCENT for value in changes):
            if item["symbol"] not in trigger_symbols:
                missing.append(item["symbol"])
    return sorted(set(missing))


def coverage_status(mapped_total: int, returned_total: int) -> tuple[str, float]:
    percent = 100.0 if mapped_total == 0 else returned_total / mapped_total * 100
    status = "MARKET_DATA_COMPLETE" if returned_total == mapped_total else "SCAN_INCOMPLETE"
    return status, percent


def format_price(value: Any) -> str:
    if value is None:
        return "N/A"
    if value >= 1000:
        return f"${value:,.2f}"
    if value >= 1:
        return f"${value:,.6f}".rstrip("0").rstrip(".")
    return f"${value:.10f}".rstrip("0").rstrip(".")


def format_change(value: Any) -> str:
    return "N/A" if value is None else f"{value:+.2f}%"


def format_volume(value: Any) -> str:
    if value is None:
        return "N/A"
    if value >= 1_000_000_000:
        compact = f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        compact = f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        compact = f"${value / 1_000:.2f}K"
    else:
        compact = f"${value:,.2f}"

    if value >= 100_000_000:
        chinese = f"約 {value / 100_000_000:,.2f} 億美元"
    elif value >= 10_000:
        chinese = f"約 {value / 10_000:,.0f} 萬美元"
    else:
        chinese = f"約 {value:,.0f} 美元"
    return f"{compact} ({value:,.0f} USD; {chinese})"


def print_console(stats: dict[str, int], status: dict[str, Any], triggers: list[dict[str, Any]]) -> None:
    print("ALTCOIN RADAR - STAGE A")
    print()
    print(f"Watchlist: {stats['watchlist_total']}")
    print(f"Mapped: {stats['mapped_total']}")
    print(f"Need review: {stats['unmapped_total']}")
    print(f"Returned: {status['market_data_returned']}")
    print(f"Coverage: {status['coverage_percent']:.2f}%")
    print(f"Scan status: {status['scan_status']}")
    print()
    print(f"Triggers: {len(triggers)}")
    if triggers:
        print("SYMBOL | PRICE | 1H | 24H | VOLUME USD")
        for item in triggers:
            print(
                f"{item['symbol']} | {format_price(item['current_price'])} | "
                f"{format_change(item['price_change_percentage_1h'])} | "
                f"{format_change(item['price_change_percentage_24h'])} | "
                f"{format_volume(item['total_volume'])}"
            )
    elif status["scan_status"] == "MARKET_DATA_COMPLETE":
        print("NO NEW PRICE THRESHOLD SIGNAL")
    else:
        print("Trigger conclusion withheld because market data coverage is incomplete.")


def run(run_id: str | None = None) -> int:
    run_id = run_id or create_run_id()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    scan_started = utc_now()
    entries = load_watchlist()
    stats = watchlist_stats(entries)
    mapping_review = build_mapping_review(entries)
    atomic_write_json(OUTPUT_DIR / "mapping_review.json", mapping_review)

    mapped_entries = [
        item for item in entries if item.get("enabled", False) and item.get("coingecko_id")
    ]
    id_to_entry = {item["coingecko_id"]: item for item in mapped_entries}
    if len(id_to_entry) != len(mapped_entries):
        raise ValueError("duplicate CoinGecko IDs are not allowed in enabled mappings")

    raw_data = fetch_market_data(list(id_to_entry), os.getenv("COINGECKO_API_KEY"))
    raw_by_id = {item.get("id"): item for item in raw_data if item.get("id") in id_to_entry}
    records = [
        enrich_record(raw_by_id[coin_id], id_to_entry[coin_id]["symbol"], coin_id)
        for coin_id in id_to_entry
        if coin_id in raw_by_id
    ]
    triggers = build_triggers(records)
    top_movers = build_top_movers(records)
    missing_trigger_symbols = sanity_check(records, triggers)

    missing_symbols = [
        item["symbol"] for item in mapped_entries if item["coingecko_id"] not in raw_by_id
    ]
    coverage_label, coverage_percent = coverage_status(stats["mapped_total"], len(records))
    scan_status = "FAILED_ASSERTION" if missing_trigger_symbols else coverage_label
    finished_at = utc_now()

    snapshot_payload = {
        "run_id": run_id,
        "generated_at": finished_at,
        "source": "CoinGecko /api/v3/coins/markets",
        "count": len(records),
        "items": records,
    }
    trigger_payload = {
        "run_id": run_id,
        "generated_at": finished_at,
        "threshold_percent": THRESHOLD_PERCENT,
        "count": len(triggers),
        "items": triggers,
    }
    movers_payload = {"run_id": run_id, "generated_at": finished_at, **top_movers}
    status_payload = {
        "run_id": run_id,
        "scan_started_at": scan_started,
        "scan_finished_at": finished_at,
        **stats,
        "market_data_returned": len(records),
        "coverage_percent": round(coverage_percent, 6),
        "missing_symbols": missing_symbols,
        "missing_trigger_symbols": missing_trigger_symbols,
        "scan_status": scan_status,
    }

    atomic_write_json(OUTPUT_DIR / "latest_snapshot.json", snapshot_payload)
    atomic_write_json(OUTPUT_DIR / "latest_triggers.json", trigger_payload)
    atomic_write_json(OUTPUT_DIR / "latest_top_movers.json", movers_payload)
    atomic_write_json(OUTPUT_DIR / "scan_status.json", status_payload)
    print_console(stats, status_payload, triggers)

    if missing_trigger_symbols:
        return 3
    if coverage_label != "MARKET_DATA_COMPLETE":
        return 2
    return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
    run_id = create_run_id()
    try:
        return run(run_id)
    except Exception as exc:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        failure = {
            "run_id": run_id,
            "scan_finished_at": utc_now(),
            "scan_status": "ERROR",
            "error": str(exc),
        }
        atomic_write_json(OUTPUT_DIR / "scan_status.json", failure)
        print(f"ALTCOIN RADAR - STAGE A\n\nERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
