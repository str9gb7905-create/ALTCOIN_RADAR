"""Operational heartbeat check for ALTCOIN_RADAR."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from run_pipeline import HEARTBEAT_PATH, RUN_STATUS_PATH
from state_store import FileSystemStateStore, StateStore


MAX_HEARTBEAT_AGE_SECONDS = 1800


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def evaluate_health(
    *,
    store: StateStore | None = None,
    heartbeat_path: Path = HEARTBEAT_PATH,
    run_status_path: Path = RUN_STATUS_PATH,
    now: datetime | None = None,
    max_age_seconds: int = MAX_HEARTBEAT_AGE_SECONDS,
) -> dict[str, Any]:
    store = store or FileSystemStateStore()
    current = now or datetime.now(timezone.utc)
    run_status = store.load(run_status_path, {})
    heartbeat = store.load(heartbeat_path, {})

    if run_status.get("status") in {"FAILED", "SCAN_INCOMPLETE"}:
        return {
            "status": "RADAR_HEALTH_DEGRADED",
            "reason": f"last run status is {run_status.get('status')}",
            "last_run_id": run_status.get("run_id"),
        }

    timestamp = heartbeat.get("last_success_at_utc")
    if not isinstance(timestamp, str):
        return {
            "status": "RADAR_HEARTBEAT_MISSED",
            "reason": "successful heartbeat is missing",
        }
    try:
        age = max(0.0, (current - _parse_utc(timestamp)).total_seconds())
    except ValueError:
        return {
            "status": "RADAR_HEARTBEAT_MISSED",
            "reason": "heartbeat timestamp is invalid",
        }
    if age > max_age_seconds:
        return {
            "status": "RADAR_HEARTBEAT_MISSED",
            "reason": f"last successful heartbeat is {age:.0f} seconds old",
            "last_success_run_id": heartbeat.get("last_success_run_id"),
            "heartbeat_age_seconds": round(age, 3),
        }
    return {
        "status": "RADAR_HEALTH_OK",
        "reason": "last run is healthy and the successful heartbeat is current",
        "last_success_run_id": heartbeat.get("last_success_run_id"),
        "heartbeat_age_seconds": round(age, 3),
        "coverage_pct": heartbeat.get("coverage_pct"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-age-seconds", type=int, default=MAX_HEARTBEAT_AGE_SECONDS)
    args = parser.parse_args()
    result = evaluate_health(max_age_seconds=args.max_age_seconds)
    print(result["status"])
    print(f"Reason: {result['reason']}")
    if result.get("last_success_run_id"):
        print(f"Last success run ID: {result['last_success_run_id']}")
    if result.get("heartbeat_age_seconds") is not None:
        print(f"Heartbeat age: {result['heartbeat_age_seconds']:.3f}s")
    if result.get("coverage_pct") is not None:
        print(f"Coverage: {result['coverage_pct']:.2f}%")
    return {"RADAR_HEALTH_OK": 0, "RADAR_HEALTH_DEGRADED": 2}.get(
        result["status"], 3
    )


if __name__ == "__main__":
    raise SystemExit(main())
