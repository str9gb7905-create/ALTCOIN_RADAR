"""Single-entrypoint, fail-closed ALTCOIN_RADAR pipeline orchestration."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import radar_scan
import radar_state
import stage_b_divergence
import stage_b_enrich
import telemetry
from state_store import FileSystemStateStore, StateStore


PROJECT_ROOT = Path(__file__).resolve().parent
STATE_DIR = PROJECT_ROOT / "state"
RUN_STATUS_PATH = STATE_DIR / "run_status.json"
HEARTBEAT_PATH = STATE_DIR / "heartbeat.json"
LOCK_PATH = STATE_DIR / "pipeline.lock"
WATCHLIST_PATH = PROJECT_ROOT / "config" / "watchlist.json"
EXPECTED_WATCHLIST = 165
LOCK_STALE_SECONDS = 3600

StageARunner = Callable[[str], dict[str, Any]]
StageRunner = Callable[[], dict[str, Any]]


class PipelineStageError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate_canonical_watchlist(path: Path = WATCHLIST_PATH) -> dict[str, Any]:
    entries = radar_scan.load_watchlist(path)
    symbols = {str(item["symbol"]).upper() for item in entries}
    mapped = [item for item in entries if item.get("coingecko_id")]
    enabled = [item for item in entries if item.get("enabled") is True]
    needs_review = [item for item in entries if item.get("needs_review") is True]
    ids = [item["coingecko_id"] for item in mapped]
    mapping = {str(item["symbol"]).upper(): item.get("coingecko_id") for item in entries}
    checks = {
        "INSP_absent": "INSP" not in symbols,
        "MANA_present": "MANA" in symbols,
        "SKL_present": "SKL" in symbols,
        "PIXEL_present": "PIXEL" in symbols,
        "AI_present": "AI" in symbols,
        "ALICE_mapped": mapping.get("ALICE") == "my-neighbor-alice",
        "ARC_mapped": mapping.get("ARC") == "ai-rig-complex",
        "BIO_mapped": mapping.get("BIO") == "bio-protocol",
        "CC_mapped": mapping.get("CC") == "canton-network",
        "LSK_mapped": mapping.get("LSK") == "lisk",
        "QNT_mapped": mapping.get("QNT") == "quant-network",
        "STEEM_mapped": mapping.get("STEEM") == "steem",
        "WAVES_mapped": mapping.get("WAVES") == "waves",
        "AVA_is_Ava_AI": mapping.get("AVA") == "ava-ai",
        "A2Z_removed": "A2Z" not in symbols,
        "CETUS_removed": "CETUS" not in symbols,
        "DENT_removed": "DENT" not in symbols,
        "HOOK_removed": "HOOK" not in symbols,
        "NKN_removed": "NKN" not in symbols,
    }
    conflicts = []
    if len(entries) != EXPECTED_WATCHLIST:
        conflicts.append(f"asset_count={len(entries)} expected={EXPECTED_WATCHLIST}")
    if len(mapped) != EXPECTED_WATCHLIST:
        conflicts.append(f"mapped_count={len(mapped)} expected={EXPECTED_WATCHLIST}")
    if len(enabled) != EXPECTED_WATCHLIST:
        conflicts.append(f"enabled_count={len(enabled)} expected={EXPECTED_WATCHLIST}")
    if needs_review:
        conflicts.append(
            "needs_review=" + ",".join(str(item["symbol"]) for item in needs_review)
        )
    if len(ids) != len(set(ids)):
        conflicts.append("duplicate CoinGecko IDs")
    conflicts.extend(name for name, passed in checks.items() if not passed)
    return {
        "source": str(path),
        "asset_count": len(entries),
        "mapped_count": len(mapped),
        "enabled_count": len(enabled),
        "needs_review_count": len(needs_review),
        "critical_checks": checks,
        "conflicts": conflicts,
    }


def _lock_age_seconds(payload: dict[str, Any], now: datetime) -> float:
    created = payload.get("created_at_utc")
    if not isinstance(created, str):
        return float("inf")
    try:
        return max(0.0, (now - _parse_utc(created)).total_seconds())
    except ValueError:
        return float("inf")


def acquire_lock(
    path: Path,
    run_id: str,
    *,
    stale_after_seconds: int = LOCK_STALE_SECONDS,
    now: datetime | None = None,
) -> tuple[bool, bool]:
    """Acquire an exclusive local lock; return (acquired, stale_lock_recovered)."""
    current = now or datetime.now(timezone.utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    recovered = False
    for _ in range(2):
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                with path.open("r", encoding="utf-8") as handle:
                    existing = json.load(handle)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                existing = {}
            if _lock_age_seconds(existing, current) <= stale_after_seconds:
                return False, recovered
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            recovered = True
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                {
                    "run_id": run_id,
                    "pid": os.getpid(),
                    "created_at_utc": current.isoformat().replace("+00:00", "Z"),
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        return True, recovered
    return False, recovered


def release_lock(path: Path, run_id: str) -> None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("run_id") == run_id:
            path.unlink()
    except FileNotFoundError:
        pass


def _default_stage_a(run_id: str) -> dict[str, Any]:
    exit_code = radar_scan.run(run_id)
    status = FileSystemStateStore().load(radar_scan.OUTPUT_DIR / "scan_status.json", {})
    trigger_payload = FileSystemStateStore().load(
        radar_scan.OUTPUT_DIR / "latest_triggers.json", {}
    )
    return {"exit_code": exit_code, "trigger_count": trigger_payload.get("count", 0), **status}


def _assert_stage_result(stage: str, result: dict[str, Any]) -> None:
    if result.get("exit_code") != 0 or result.get("skipped") is True:
        reason = result.get("reason") or f"exit_code={result.get('exit_code')}"
        raise PipelineStageError(f"{stage}: {reason}")


def _base_status(run_id: str, started_at: str) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "started_at_utc": started_at,
        "completed_at_utc": None,
        "status": "RUNNING",
        "stage_a_status": None,
        "watchlist_expected": EXPECTED_WATCHLIST,
        "market_data_returned": 0,
        "coverage_pct": 0.0,
        "active_trigger_count": 0,
        "radar_assets_baselined": 0,
        "technical_assets_baselined": 0,
        "divergence_assets_baselined": 0,
        "notification_candidate_count": 0,
        "stage_b_attempted": 0,
        "divergence_attempted": 0,
        "failed_stage": None,
        "error_message": None,
        "elapsed_seconds": 0.0,
        "API_requests_total": 0,
        "requests_by_exchange": {},
        "elapsed_stage_a": 0.0,
        "elapsed_radar_state": 0.0,
        "elapsed_stage_b": 0.0,
        "elapsed_divergence": 0.0,
        "elapsed_total": 0.0,
        "asset_enrichment_seconds": {},
        "stale_lock_recovered": False,
    }


def _finalize(status: dict[str, Any], started: float) -> None:
    elapsed = round(time.perf_counter() - started, 6)
    status["completed_at_utc"] = utc_now()
    status["elapsed_seconds"] = elapsed
    status["elapsed_total"] = elapsed
    status.update(telemetry.snapshot())


def execute_pipeline(
    *,
    store: StateStore | None = None,
    watchlist_path: Path = WATCHLIST_PATH,
    run_status_path: Path = RUN_STATUS_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    lock_path: Path = LOCK_PATH,
    stale_after_seconds: int = LOCK_STALE_SECONDS,
    stage_a_runner: StageARunner | None = None,
    radar_state_runner: StageRunner | None = None,
    stage_b_runner: StageRunner | None = None,
    divergence_runner: StageRunner | None = None,
    bootstrap: bool = False,
) -> tuple[int, dict[str, Any]]:
    store = store or FileSystemStateStore()
    run_id = radar_scan.create_run_id()
    acquired, recovered = acquire_lock(
        lock_path, run_id, stale_after_seconds=stale_after_seconds
    )
    if not acquired:
        return 3, {"run_id": run_id, "status": "RUN_SKIPPED_ALREADY_RUNNING"}

    started_clock = time.perf_counter()
    telemetry.reset()
    status = _base_status(run_id, utc_now())
    status["stale_lock_recovered"] = recovered
    current_stage = "watchlist"
    try:
        store.save_atomic(run_status_path, status)
        canonical = validate_canonical_watchlist(watchlist_path)
        if canonical["conflicts"]:
            raise PipelineStageError("WATCHLIST_CONFLICT: " + "; ".join(canonical["conflicts"]))

        current_stage = "stage_a"
        clock = time.perf_counter()
        stage_a = (stage_a_runner or _default_stage_a)(run_id)
        status["elapsed_stage_a"] = round(time.perf_counter() - clock, 6)
        status["stage_a_status"] = stage_a.get("scan_status")
        status["watchlist_expected"] = int(stage_a.get("mapped_total", EXPECTED_WATCHLIST))
        status["market_data_returned"] = int(stage_a.get("market_data_returned", 0))
        status["coverage_pct"] = float(stage_a.get("coverage_percent", 0.0))
        status["active_trigger_count"] = int(stage_a.get("trigger_count", 0))
        if stage_a.get("scan_status") == "SCAN_INCOMPLETE" or stage_a.get("exit_code") == 2:
            status["status"] = "SCAN_INCOMPLETE"
            status["failed_stage"] = "stage_a"
            status["error_message"] = (
                "Stage A market coverage is incomplete; signal conclusion was withheld"
            )
            _finalize(status, started_clock)
            store.save_atomic(run_status_path, status)
            return 2, status
        if stage_a.get("exit_code") != 0 or stage_a.get("scan_status") != "MARKET_DATA_COMPLETE":
            raise PipelineStageError(
                f"Stage A status={stage_a.get('scan_status')} exit_code={stage_a.get('exit_code')}"
            )

        current_stage = "radar_state"
        clock = time.perf_counter()
        state_result = (
            radar_state_runner
            or (lambda: radar_state.run_state_update(bootstrap=bootstrap, state_store=store))
        )()
        status["elapsed_radar_state"] = round(time.perf_counter() - clock, 6)
        _assert_stage_result(current_stage, state_result)
        status["active_trigger_count"] = int(state_result.get("active", 0))
        status["radar_assets_baselined"] = int(state_result.get("assets_baselined", 0))

        current_stage = "stage_b"
        clock = time.perf_counter()
        stage_b = (
            stage_b_runner
            or (lambda: stage_b_enrich.run_stage_b(bootstrap=bootstrap, state_store=store))
        )()
        status["elapsed_stage_b"] = round(time.perf_counter() - clock, 6)
        _assert_stage_result(current_stage, stage_b)
        status["stage_b_attempted"] = int(stage_b.get("active", 0))
        status["technical_assets_baselined"] = int(stage_b.get("assets_baselined", 0))

        current_stage = "divergence"
        clock = time.perf_counter()
        divergence = (
            divergence_runner
            or (
                lambda: stage_b_divergence.run_divergence(
                    bootstrap=bootstrap, state_store=store
                )
            )
        )()
        status["elapsed_divergence"] = round(time.perf_counter() - clock, 6)
        _assert_stage_result(current_stage, divergence)
        status["divergence_attempted"] = int(divergence.get("active", 0))
        status["divergence_assets_baselined"] = int(
            divergence.get("assets_baselined", 0)
        )

        status["notification_candidate_count"] = (
            int(state_result.get("notification_candidates", 0))
            + int(stage_b.get("notification_events", 0))
            + int(divergence.get("new_events", 0))
            + int(divergence.get("escalation_events", 0))
        )
        status["status"] = "SUCCESS"
        _finalize(status, started_clock)
        store.save_atomic(
            heartbeat_path,
            {
                "last_success_run_id": run_id,
                "last_success_at_utc": status["completed_at_utc"],
                "coverage_pct": status["coverage_pct"],
            },
        )
        store.save_atomic(run_status_path, status)
        return 0, status
    except Exception as exc:
        status["status"] = "FAILED"
        status["failed_stage"] = current_stage
        status["error_message"] = str(exc)
        _finalize(status, started_clock)
        store.save_atomic(run_status_path, status)
        return 1, status
    finally:
        release_lock(lock_path, run_id)


def print_summary(status: dict[str, Any]) -> None:
    print("ALTCOIN RADAR - PIPELINE")
    print()
    print(f"Run ID: {status.get('run_id')}")
    print(f"Status: {status.get('status')}")
    if status.get("status") == "RUN_SKIPPED_ALREADY_RUNNING":
        return
    print(f"Watchlist expected: {status.get('watchlist_expected')}")
    print(f"Market data returned: {status.get('market_data_returned')}")
    print(f"Coverage: {status.get('coverage_pct', 0):.2f}%")
    print(f"Active triggers: {status.get('active_trigger_count')}")
    print(f"Radar assets baselined: {status.get('radar_assets_baselined')}")
    print(f"Technical assets baselined: {status.get('technical_assets_baselined')}")
    print(f"Divergence assets baselined: {status.get('divergence_assets_baselined')}")
    print(f"Stage B attempted: {status.get('stage_b_attempted')}")
    print(f"Divergence attempted: {status.get('divergence_attempted')}")
    print(f"Notification candidates: {status.get('notification_candidate_count')}")
    print(f"API requests: {status.get('API_requests_total')}")
    print(f"Elapsed total: {status.get('elapsed_total', 0):.3f}s")
    if status.get("error_message"):
        print(f"Error: {status['error_message']}")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", action="store_true")
    args = parser.parse_args()
    exit_code, status = execute_pipeline(bootstrap=args.bootstrap)
    print_summary(status)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
