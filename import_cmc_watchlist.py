"""Safely append CoinMarketCap watchlist assets without creating duplicates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from radar_scan import OUTPUT_DIR, WATCHLIST_PATH, atomic_write_json, load_watchlist, utc_now


IGNORE_DUPLICATE = "IGNORE_DUPLICATE"
NEW_ASSET = "NEW_ASSET"
NEEDS_REVIEW = "NEEDS_REVIEW"


def _text(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _identity(value: Any) -> str | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value).casefold()
    text = _text(value)
    return text.casefold() if text else None


def _symbol(item: dict[str, Any]) -> str:
    value = _text(item.get("symbol"))
    if not value:
        raise ValueError("every imported asset must have a symbol")
    return value.upper()


def _find_by(items: Iterable[dict[str, Any]], field: str, value: Any) -> dict[str, Any] | None:
    wanted = _identity(value)
    if wanted is None:
        return None
    return next((item for item in items if _identity(item.get(field)) == wanted), None)


def _same_named_asset(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    old_name = _identity(existing.get("name"))
    new_name = _identity(incoming.get("name"))
    return old_name is not None and old_name == new_name


def merge_cmc_assets(
    existing_items: list[dict[str, Any]],
    incoming_items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return an append-only merged watchlist and one classification per input item."""
    merged = [dict(item) for item in existing_items]
    results: list[dict[str, Any]] = []

    for index, incoming_source in enumerate(incoming_items):
        if not isinstance(incoming_source, dict):
            raise ValueError(f"import item {index} must be an object")
        incoming = dict(incoming_source)
        symbol = _symbol(incoming)
        coin_id = _text(incoming.get("coingecko_id"))
        canonical = _text(incoming.get("canonical_asset"))
        cmc_id = incoming.get("cmc_id")

        matched = _find_by(merged, "coingecko_id", coin_id)
        reason = "same CoinGecko ID"
        if matched is None:
            matched = _find_by(merged, "canonical_asset", canonical)
            reason = "same canonical asset"
        if matched is None and cmc_id is not None:
            matched = _find_by(merged, "cmc_id", str(cmc_id))
            reason = "same CoinMarketCap ID"

        if matched is not None:
            results.append(
                {
                    "input_index": index,
                    "symbol": symbol,
                    "status": IGNORE_DUPLICATE,
                    "matched_symbol": matched["symbol"],
                    "reason": reason,
                }
            )
            continue

        same_symbol = next(
            (item for item in merged if _symbol(item) == symbol),
            None,
        )
        if same_symbol is not None:
            old_coin_id = _identity(same_symbol.get("coingecko_id"))
            new_coin_id = _identity(coin_id)
            old_canonical = _identity(same_symbol.get("canonical_asset"))
            new_canonical = _identity(canonical)

            identities_conflict = (
                old_coin_id is not None
                and new_coin_id is not None
                and old_coin_id != new_coin_id
            ) or (
                old_canonical is not None
                and new_canonical is not None
                and old_canonical != new_canonical
            )
            if not identities_conflict and _same_named_asset(same_symbol, incoming):
                results.append(
                    {
                        "input_index": index,
                        "symbol": symbol,
                        "status": IGNORE_DUPLICATE,
                        "matched_symbol": same_symbol["symbol"],
                        "reason": "case-insensitive symbol and asset name match",
                    }
                )
            else:
                results.append(
                    {
                        "input_index": index,
                        "symbol": symbol,
                        "status": NEEDS_REVIEW,
                        "matched_symbol": same_symbol["symbol"],
                        "reason": "same symbol may refer to a different asset",
                    }
                )
            continue

        new_item: dict[str, Any] = {
            "symbol": symbol,
            "coingecko_id": coin_id,
            "enabled": True,
        }
        for field in ("name", "canonical_asset", "cmc_id"):
            if incoming.get(field) is not None:
                new_item[field] = incoming[field]
        if coin_id is None:
            new_item["needs_review"] = True
            new_item["review_reason"] = "New CMC asset has no confirmed CoinGecko ID"
        else:
            new_item["needs_review"] = False

        merged.append(new_item)
        results.append(
            {
                "input_index": index,
                "symbol": symbol,
                "status": NEW_ASSET,
                "matched_symbol": None,
                "reason": "new canonical asset",
            }
        )

    return merged, results


def load_import_items(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        raise ValueError("CMC import must be a JSON array or object containing an item list")

    for key in ("items", "watchlist", "cryptoCurrencyList"):
        if isinstance(payload.get(key), list):
            return payload[key]
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "watchlist", "cryptoCurrencyList"):
            if isinstance(data.get(key), list):
                return data[key]
    raise ValueError("could not find a CMC asset list in the import JSON")


def build_report(results: list[dict[str, Any]], dry_run: bool) -> dict[str, Any]:
    return {
        "generated_at": utc_now(),
        "dry_run": dry_run,
        "input_total": len(results),
        "ignore_duplicate": sum(item["status"] == IGNORE_DUPLICATE for item in results),
        "new_asset": sum(item["status"] == NEW_ASSET for item in results),
        "needs_review": sum(item["status"] == NEEDS_REVIEW for item in results),
        "items": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_json", type=Path, help="CMC watchlist JSON to import")
    parser.add_argument("--dry-run", action="store_true", help="classify without changing watchlist.json")
    args = parser.parse_args()

    existing = load_watchlist(WATCHLIST_PATH)
    incoming = load_import_items(args.input_json)
    merged, results = merge_cmc_assets(existing, incoming)
    report = build_report(results, args.dry_run)

    if not args.dry_run:
        atomic_write_json(WATCHLIST_PATH, merged)
    atomic_write_json(OUTPUT_DIR / "cmc_import_report.json", report)

    print("CMC WATCHLIST IMPORT")
    print(f"Input: {report['input_total']}")
    print(f"IGNORE_DUPLICATE: {report['ignore_duplicate']}")
    print(f"NEW_ASSET: {report['new_asset']}")
    print(f"NEEDS_REVIEW: {report['needs_review']}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'APPLIED'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
