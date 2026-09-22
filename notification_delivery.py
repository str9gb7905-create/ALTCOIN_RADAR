"""Durable at-most-once notification delivery state machine."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from notification_dispatcher import NotificationDispatcher, TelegramDispatcher
from radar_scan import OUTPUT_DIR, PROJECT_ROOT
from state_store import FileSystemStateStore, StateStore

DELIVERY_STATE_PATH = PROJECT_ROOT / "state" / "notification_delivery.json"
MERGED_PAYLOAD_PATH = OUTPUT_DIR / "latest_merged_notification.json"
RETRYABLE_STATES = {"PENDING", "FAILED_RETRYABLE"}
TERMINAL_STATES = {"SUCCESS", "FAILED_PERMANENT", "UNKNOWN_TIMEOUT"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def empty_delivery_state() -> dict[str, Any]:
    return {"schema_version": 1, "updated_at_utc": None, "notifications": {}}


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
        existing = state["notifications"].get(notification_id)
        if existing is not None:
            if existing.get("message") != message or existing.get("run_id") != run_id:
                raise ValueError("notification_id collision")
            return existing
        now = self.now()
        record = {
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
            "event_count": merged.get("event_count", 0),
            "message": message,
        }
        state["notifications"][notification_id] = record
        self._save(state)
        return record

    def claim_next(self) -> dict[str, Any] | None:
        """Durably claim one send; abandoned SENDING records become non-retryable."""
        state = self.load()
        changed = False
        for record in state["notifications"].values():
            if record.get("delivery_status") == "SENDING":
                record["delivery_status"] = "UNKNOWN_TIMEOUT"
                record["error"] = "INTERRUPTED_AFTER_DURABLE_CLAIM"
                changed = True
        candidates = [record for record in state["notifications"].values()
                      if record.get("delivery_status") in RETRYABLE_STATES]
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
    subparsers.add_parser("claim")
    dispatch = subparsers.add_parser("dispatch")
    dispatch.add_argument("--notification-id", required=True)
    args = parser.parse_args()

    ledger = NotificationDeliveryLedger()
    if args.command == "prepare":
        record = ledger.prepare(_load_payload(args.payload))
        print("NO_NOTIFICATION" if record is None else
              f"DELIVERY_PREPARED notification_id={record['notification_id']}")
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
