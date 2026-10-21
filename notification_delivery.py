"""Durable at-most-once notification delivery state machine."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from notification_dispatcher import NotificationDispatcher, TelegramDispatcher
from notification_merger import notification_parts
from radar_scan import OUTPUT_DIR, PROJECT_ROOT
from run_pipeline import HEARTBEAT_PATH
from state_store import FileSystemStateStore, StateStore

DELIVERY_STATE_PATH = PROJECT_ROOT / "state" / "notification_delivery.json"
MERGED_PAYLOAD_PATH = OUTPUT_DIR / "latest_merged_notification.json"
RETRYABLE_STATES = {"PENDING", "FAILED_RETRYABLE"}
TERMINAL_STATES = {"SUCCESS", "FAILED_PERMANENT", "UNKNOWN_TIMEOUT"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def empty_delivery_state() -> dict[str, Any]:
    return {"schema_version": 1, "updated_at_utc": None, "notifications": {}}


def _timestamp(value: str) -> datetime:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("notification timestamp must have a timezone")
    return stamp


def _alert_ranges(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"symbol": item["symbol"],
             "direction": "UP" if item["change_pct"] >= 0 else "DOWN",
             "level": int(item["trigger_level"])} for item in items]


def _record_ranges(record: dict[str, Any]) -> list[dict[str, Any]]:
    if "alert_ranges" in record:
        return record["alert_ranges"]
    # Migrate counting from old delivered text without changing historical records.
    # Symbols appearing only in the old overflow summary did not receive details.
    found = []
    pattern = r"(?:^|\n)([A-Z0-9]+)｜[^\n]*\n觸發：([^\n]*)\n價格：[^\n]*\n幅度級距：([+-]\d+)%"
    for symbol, condition, signed in re.findall(pattern, record.get("message", "")):
        if "近24H" in condition:
            found.append({"symbol": symbol, "direction": "UP" if int(signed) > 0 else "DOWN",
                          "level": abs(int(signed))})
    return found


def _allowed_items(items, state, now, window_hours, maximum):
    cutoff = _timestamp(now) - timedelta(hours=window_hours)
    previous = []
    for record in state["notifications"].values():
        if record.get("delivery_status") == "FAILED_PERMANENT":
            continue
        # Pending/unknown records reserve quota to avoid replay after a crash.
        stamp = record.get("delivered_at_utc") or record.get("created_at_utc")
        if stamp and _timestamp(stamp) > cutoff:
            previous.extend(_record_ranges(record))
    result = []
    for item in items:
        bucket = _alert_ranges([item])[0]
        # UP and DOWN share one rolling quota for the same symbol and tier.
        # Higher-tier alerts also consume lower-tier quota, so falling back
        # cannot reset the 24-hour washout protection.
        count = sum(old["symbol"] == bucket["symbol"]
                    and old["level"] >= bucket["level"] for old in previous)
        # A fall back into a lower band cannot reset the daily quota.
        if count < maximum:
            result.append(item)
    return result


class NotificationDeliveryLedger:
    def __init__(self, *, store: StateStore | None = None,
                 path: Path = DELIVERY_STATE_PATH,
                 now: Callable[[], str] = _utc_now):
        self.store = store or FileSystemStateStore()
        self.path = path
        self.now = now

    def load(self) -> dict[str, Any]:
        state = self.store.load(self.path, empty_delivery_state())
        if not isinstance(state, dict) or not isinstance(state.get("notifications"), dict):
            raise ValueError("invalid notification delivery state")
        return state

    def _save(self, state: dict[str, Any]) -> None:
        state["schema_version"] = 1
        state["updated_at_utc"] = self.now()
        self.store.save_atomic(self.path, state)

    def prepare(self, merged: dict[str, Any]) -> dict[str, Any] | None:
        if merged.get("status") == "NO_NOTIFICATION":
            return None
        if merged.get("status") != "READY":
            raise ValueError("merged notification is not READY")
        notification_id = merged.get("notification_id")
        message = merged.get("message")
        run_id = merged.get("run_id")
        if not all(isinstance(value, str) and value for value in
                   (notification_id, message, run_id)):
            raise ValueError("merged notification is missing required fields")
        state = self.load()
        source_digest = hashlib.sha256(json.dumps(
            merged, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")).hexdigest()
        existing = state["notifications"].get(notification_id)
        if existing is not None:
            same_payload = (existing.get("source_digest") == source_digest
                            if existing.get("source_digest") else existing.get("message") == message)
            if not same_payload or existing.get("run_id") != run_id:
                raise ValueError("notification_id collision")
            return existing
        batch_records = [record for record in state["notifications"].values()
                         if record.get("batch_id") == notification_id]
        if batch_records:
            if any(record.get("source_digest") != source_digest for record in batch_records):
                raise ValueError("notification_id collision")
            return min(batch_records, key=lambda record: record["part_index"])
        now = self.now()
        items = merged.get("items", [])
        policy = merged.get("repeat_policy", {})
        window = policy.get("window_hours", 24)
        maximum = policy.get("max_per_range", 3)
        if any(isinstance(v, bool) or not isinstance(v, int) or v <= 0
               for v in (window, maximum)):
            raise ValueError("invalid repeat notification policy")
        if items:
            allowed = _allowed_items(items, state, now, window, maximum)
            if not allowed:
                return None
            parts = notification_parts(run_id, allowed)
        else:
            parts = [{"message": message, "items": []}]
        first = None
        for index, part in enumerate(parts, 1):
            part_id = notification_id if len(parts) == 1 else f"{notification_id}_p{index:04d}"
            record = self._new_record(part_id, run_id, part["message"], now)
            record.update({
                "batch_id": notification_id, "part_index": index, "part_count": len(parts),
                "source_digest": source_digest,
                "alert_ranges": _alert_ranges(part["items"]),
                "event_count": sum(len(item[category]) for item in part["items"]
                                   for category in ("price_events", "technical_events", "divergence_events")),
                "repeat_suppressed_count": len(items) - sum(len(p["items"]) for p in parts),
            })
            state["notifications"][part_id] = record
            if first is None:
                first = record
        self._save(state)
        return first

    @staticmethod
    def _new_record(notification_id, run_id, message, now):
        return {
            "notification_id": notification_id,
            "run_id": run_id,
            "created_at_utc": now,
            "delivery_status": "PENDING",
            "delivered_at_utc": None,
            "provider": "TELEGRAM",
            "provider_message_id": None,
            "error": None,
            "attempt_count": 0,
            "last_attempt_at_utc": None,
            "event_count": 0,
            "message": message,
        }

    def prepare_health_outage(self, heartbeat: dict[str, Any]) -> dict[str, Any]:
        """Queue one alert per period without a successful scan."""
        last_success_id = heartbeat.get("last_success_run_id") or "initial"
        notification_id = f"RADAR_OUTAGE_{last_success_id}"
        state = self.load()
        existing = state["notifications"].get(notification_id)
        if existing is not None:
            return existing
        last_success = heartbeat.get("last_success_at_utc") or "無紀錄"
        message = (
            "⚠️ ALTCOIN RADAR｜掃描故障\n"
            "本輪掃描失敗，行情資料或健康檢查未通過；期間可能漏掉價格通知。\n"
            f"上次成功：{last_success}（UTC）\n"
            "查看執行紀錄：https://github.com/str9gb7905-create/ALTCOIN_RADAR/actions/workflows/altcoin-radar.yml"
        )
        now = self.now()
        record = {
            "notification_id": notification_id,
            "run_id": last_success_id,
            "created_at_utc": now,
            "delivery_status": "PENDING",
            "delivered_at_utc": None,
            "provider": "TELEGRAM",
            "provider_message_id": None,
            "error": None,
            "attempt_count": 0,
            "last_attempt_at_utc": None,
            "event_count": 0,
            "message": message,
        }
        state["notifications"][notification_id] = record
        self._save(state)
        return record

    def claim_next(self, *, exclude_ids: set[str] | None = None) -> dict[str, Any] | None:
        """Durably claim one send; abandoned SENDING records become non-retryable."""
        state = self.load()
        changed = False
        for record in state["notifications"].values():
            if record.get("delivery_status") == "SENDING":
                record["delivery_status"] = "UNKNOWN_TIMEOUT"
                record["error"] = "INTERRUPTED_AFTER_DURABLE_CLAIM"
                changed = True
        candidates = [record for record in state["notifications"].values()
                      if record.get("delivery_status") in RETRYABLE_STATES
                      and record.get("notification_id") not in (exclude_ids or set())]
        if not candidates:
            if changed:
                self._save(state)
            return None
        record = min(candidates, key=lambda item: (
            str(item.get("created_at_utc", "")), str(item.get("notification_id", ""))
        ))
        record["delivery_status"] = "SENDING"
        record["attempt_count"] = int(record.get("attempt_count", 0)) + 1
        record["last_attempt_at_utc"] = self.now()
        record["error"] = None
        self._save(state)
        return dict(record)

    def dispatch_claimed(self, notification_id: str,
                         dispatcher: NotificationDispatcher) -> dict[str, Any]:
        state = self.load()
        record = state["notifications"].get(notification_id)
        if record is None:
            raise KeyError(f"unknown notification_id: {notification_id}")
        if record.get("delivery_status") != "SENDING":
            return record
        result = dispatcher.send(record["message"])
        if result.status not in {
            "SUCCESS", "FAILED_RETRYABLE", "FAILED_PERMANENT", "UNKNOWN_TIMEOUT"
        }:
            raise ValueError(f"invalid dispatcher status: {result.status}")
        record["delivery_status"] = result.status
        record["provider"] = result.provider
        record["provider_message_id"] = result.provider_message_id
        record["error"] = result.error
        record["delivered_at_utc"] = self.now() if result.status == "SUCCESS" else None
        self._save(state)
        return record


def _load_payload(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("merged payload must be a JSON object")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--payload", type=Path, default=MERGED_PAYLOAD_PATH)
    subparsers.add_parser("prepare-health")
    subparsers.add_parser("claim")
    dispatch = subparsers.add_parser("dispatch")
    dispatch.add_argument("--notification-id", required=True)
    args = parser.parse_args()

    ledger = NotificationDeliveryLedger()
    if args.command == "prepare":
        record = ledger.prepare(_load_payload(args.payload))
        print("NO_NOTIFICATION" if record is None else
              f"DELIVERY_PREPARED notification_id={record['notification_id']} "
              f"parts={record.get('part_count', 1)} "
              f"suppressed={record.get('repeat_suppressed_count', 0)}")
    elif args.command == "prepare-health":
        heartbeat = FileSystemStateStore().load(HEARTBEAT_PATH, {})
        record = ledger.prepare_health_outage(heartbeat)
        print(f"HEALTH_ALERT_PREPARED notification_id={record['notification_id']}")
    elif args.command == "claim":
        record = ledger.claim_next()
        print("NO_DELIVERY_CLAIM" if record is None else
              f"DELIVERY_CLAIMED notification_id={record['notification_id']}")
    else:
        dispatcher = TelegramDispatcher.from_env()
        record = ledger.dispatch_claimed(args.notification_id, dispatcher)
        print(f"DELIVERY_{record['delivery_status']} notification_id={record['notification_id']}")
        return 0 if record["delivery_status"] == "SUCCESS" else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

