import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from health_check import evaluate_health
from run_pipeline import PROJECT_ROOT, execute_pipeline
from state_store import FileSystemStateStore


def complete_stage_a(_run_id):
    return {
        "exit_code": 0,
        "scan_status": "MARKET_DATA_COMPLETE",
        "mapped_total": 162,
        "market_data_returned": 162,
        "coverage_percent": 100.0,
        "trigger_count": 4,
    }


def complete_state():
    return {
        "exit_code": 0,
        "skipped": False,
        "active": 4,
        "notification_candidates": 2,
    }


def complete_stage_b():
    return {
        "exit_code": 0,
        "skipped": False,
        "active": 4,
        "notification_events": 1,
    }


def complete_divergence():
    return {
        "exit_code": 0,
        "skipped": False,
        "active": 4,
        "new_events": 1,
        "escalation_events": 1,
    }


class CloudReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = FileSystemStateStore()
        self.status = self.root / "state" / "run_status.json"
        self.heartbeat = self.root / "state" / "heartbeat.json"
        self.lock = self.root / "state" / "pipeline.lock"

    def tearDown(self):
        self.temp.cleanup()

    def run_pipeline(self, **overrides):
        options = {
            "store": self.store,
            "run_status_path": self.status,
            "heartbeat_path": self.heartbeat,
            "lock_path": self.lock,
            "stage_a_runner": complete_stage_a,
            "radar_state_runner": complete_state,
            "stage_b_runner": complete_stage_b,
            "divergence_runner": complete_divergence,
        }
        options.update(overrides)
        return execute_pipeline(**options)

    def test_pipeline_success(self):
        code, result = self.run_pipeline()
        self.assertEqual(code, 0)
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["notification_candidate_count"], 5)
        self.assertEqual(result["stage_b_attempted"], 4)
        self.assertEqual(result["divergence_attempted"], 4)

    def test_stage_a_incomplete_stops_downstream_and_does_not_say_no_signal(self):
        called = []

        def incomplete(_run_id):
            return {
                "exit_code": 2,
                "scan_status": "SCAN_INCOMPLETE",
                "mapped_total": 162,
                "market_data_returned": 161,
                "coverage_percent": 99.382716,
                "trigger_count": 0,
            }

        def later():
            called.append(True)
            return complete_state()

        code, result = self.run_pipeline(
            stage_a_runner=incomplete,
            radar_state_runner=later,
            stage_b_runner=later,
            divergence_runner=later,
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["status"], "SCAN_INCOMPLETE")
        self.assertNotIn("NO_SIGNAL", result["error_message"])
        self.assertEqual(called, [])
        self.assertFalse(self.heartbeat.exists())

    def test_pipeline_failure_records_failed_stage(self):
        def failed_state():
            return {"exit_code": 2, "skipped": True, "reason": "bad state"}

        code, result = self.run_pipeline(radar_state_runner=failed_state)
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["failed_stage"], "radar_state")
        self.assertIn("bad state", result["error_message"])

    def test_success_updates_heartbeat(self):
        _, result = self.run_pipeline()
        heartbeat = self.store.load(self.heartbeat)
        self.assertEqual(heartbeat["last_success_run_id"], result["run_id"])
        self.assertEqual(heartbeat["coverage_pct"], 100.0)

    def test_heartbeat_missed_after_thirty_minutes(self):
        now = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
        self.store.save_atomic(
            self.heartbeat,
            {
                "last_success_run_id": "old",
                "last_success_at_utc": (now - timedelta(minutes=31)).isoformat(),
                "coverage_pct": 100.0,
            },
        )
        self.store.save_atomic(self.status, {"status": "SUCCESS", "run_id": "old"})
        result = evaluate_health(
            store=self.store,
            heartbeat_path=self.heartbeat,
            run_status_path=self.status,
            now=now,
        )
        self.assertEqual(result["status"], "RADAR_HEARTBEAT_MISSED")

    def test_stale_lock_recovery(self):
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        self.store.save_atomic(
            self.lock,
            {"run_id": "crashed", "pid": 1, "created_at_utc": old.isoformat()},
        )
        code, result = self.run_pipeline(stale_after_seconds=3600)
        self.assertEqual(code, 0)
        self.assertTrue(result["stale_lock_recovered"])
        self.assertFalse(self.lock.exists())

    def test_overlapping_run_is_blocked_without_state_mutation(self):
        now = datetime.now(timezone.utc)
        self.store.save_atomic(
            self.lock,
            {"run_id": "active", "pid": 1, "created_at_utc": now.isoformat()},
        )
        original = {"sentinel": "unchanged"}
        self.store.save_atomic(self.status, original)
        code, result = self.run_pipeline()
        self.assertEqual(code, 3)
        self.assertEqual(result["status"], "RUN_SKIPPED_ALREADY_RUNNING")
        self.assertEqual(self.store.load(self.status), original)

    def test_run_status_is_atomic_and_has_required_fields(self):
        self.run_pipeline()
        payload = self.store.load(self.status)
        required = {
            "run_id",
            "started_at_utc",
            "completed_at_utc",
            "status",
            "stage_a_status",
            "watchlist_expected",
            "market_data_returned",
            "coverage_pct",
            "active_trigger_count",
            "notification_candidate_count",
            "failed_stage",
            "error_message",
            "elapsed_seconds",
        }
        self.assertTrue(required.issubset(payload))
        self.assertEqual(list(self.status.parent.glob("*.tmp")), [])
        json.loads(self.status.read_text(encoding="utf-8"))

    def test_manifest_completeness(self):
        manifest = json.loads(
            (PROJECT_ROOT / "cloud_state_manifest.json").read_text(encoding="utf-8")
        )
        filenames = {item["filename"] for item in manifest["files"]}
        self.assertTrue(
            {
                "state/radar_state.json",
                "state/technical_state.json",
                "state/divergence_state.json",
                "state/heartbeat.json",
                "state/run_status.json",
            }.issubset(filenames)
        )
        for item in manifest["files"]:
            self.assertTrue(
                {"filename", "purpose", "required_for_correctness", "can_rebuild", "failure_impact"}.issubset(item)
            )

    def test_failed_last_run_is_degraded_not_no_signal(self):
        self.store.save_atomic(self.status, {"status": "FAILED", "run_id": "failed"})
        result = evaluate_health(
            store=self.store,
            heartbeat_path=self.heartbeat,
            run_status_path=self.status,
        )
        self.assertEqual(result["status"], "RADAR_HEALTH_DEGRADED")


if __name__ == "__main__":
    unittest.main()
