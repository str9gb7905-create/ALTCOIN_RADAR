import json
import tempfile
import unittest
from pathlib import Path

from radar_state import (
    evaluate_transition,
    run_state_update,
    signal_from_changes,
)


NOW = "2026-09-17T00:00:00Z"


def transition(previous, change_1h=0, change_24h=0):
    current = signal_from_changes("AAA", change_1h, change_24h, price=1.0)
    return evaluate_transition(previous, current, NOW)


def active_state(change_1h=0, change_24h=15, episode_id=1):
    state, _, _ = transition(None, change_1h, change_24h)
    state["episode_id"] = episode_id
    return state


class RadarStateTransitionTests(unittest.TestCase):
    def test_case_1_24h_trigger_is_recorded_without_notification(self):
        previous = {"active": False, "direction": "NONE", "severity_tier": "T0", "episode_id": 0}
        state, event, candidate = transition(previous, change_24h=11)
        self.assertEqual("NEW_TRIGGER", event["event"])
        self.assertIsNone(candidate)
        self.assertTrue(state["active"])

    def test_case_2_same_tier_is_continuing_without_notification(self):
        previous = active_state(change_24h=11)
        _, event, candidate = transition(previous, change_24h=17)
        self.assertEqual("CONTINUING", event["event"])
        self.assertIsNone(candidate)

    def test_case_3_24h_escalation_does_not_notify(self):
        previous = active_state(change_24h=11)
        _, event, candidate = transition(previous, change_24h=20.1)
        self.assertEqual("ESCALATION", event["event"])
        self.assertIsNone(candidate)

    def test_case_4_tier_decrease_does_not_notify(self):
        previous = active_state(change_24h=25)
        _, event, candidate = transition(previous, change_24h=18)
        self.assertEqual("CONTINUING", event["event"])
        self.assertIsNone(candidate)

    def test_case_5_24h_direction_change_does_not_notify(self):
        previous = active_state(change_24h=15)
        _, event, candidate = transition(previous, change_24h=-13)
        self.assertEqual("DIRECTION_CHANGE", event["event"])
        self.assertEqual("UP", event["previous_direction"])
        self.assertEqual("DOWN", event["current_direction"])
        self.assertIsNone(candidate)

    def test_case_6_active_to_below_threshold_is_exit_without_notification(self):
        previous = active_state(change_24h=15)
        state, event, candidate = transition(previous, change_24h=5)
        self.assertEqual("EXIT", event["event"])
        self.assertIsNone(candidate)
        self.assertFalse(state["active"])
        self.assertEqual(NOW, state["last_exit_at"])

    def test_case_7_24h_reentry_increments_episode_without_notification(self):
        previous = active_state(change_24h=15, episode_id=1)
        exited, exit_event, _ = transition(previous, change_24h=5)
        reentered, reentry_event, candidate = transition(exited, change_24h=12)
        self.assertEqual("EXIT", exit_event["event"])
        self.assertEqual("REENTRY", reentry_event["event"])
        self.assertEqual(2, reentered["episode_id"])
        self.assertIsNone(candidate)

    def test_case_10_mixed_direction_keeps_both_conditions(self):
        current = signal_from_changes("AAA", 12, -11)
        self.assertEqual("MIXED", current["direction"])
        self.assertEqual(["1H_UP", "24H_DOWN"], current["active_conditions"])

    def test_case_11_t4_95_to_t4_80_continues_without_notification(self):
        previous = active_state(change_24h=95)
        _, event, candidate = transition(previous, change_24h=80)
        self.assertEqual("CONTINUING", event["event"])
        self.assertIsNone(candidate)

    def test_case_12_24h_tier_increase_does_not_notify(self):
        previous = active_state(change_24h=95)
        _, event, candidate = transition(previous, change_24h=105)
        self.assertEqual("ESCALATION", event["event"])
        self.assertIsNone(candidate)

    def test_1h_crosses_threshold_while_24h_continues(self):
        previous = active_state(change_1h=0, change_24h=12)
        state, event, candidate = transition(previous, change_1h=11, change_24h=12)
        self.assertEqual("CONTINUING", event["event"])
        self.assertEqual("NEW_TRIGGER", candidate["event"])
        self.assertEqual(["1H_UP"], candidate["active_conditions"])
        self.assertEqual("T1", candidate["severity_tier"])
        self.assertTrue(state["notification_1h"]["active"])

    def test_24h_escalation_does_not_repeat_active_1h_alert(self):
        previous = active_state(change_1h=11, change_24h=12)
        _, event, candidate = transition(previous, change_1h=11, change_24h=45)
        self.assertEqual("ESCALATION", event["event"])
        self.assertIsNone(candidate)

    def test_1h_reentry_and_1h_escalation_are_independent_of_24h(self):
        previous = active_state(change_1h=-11, change_24h=22)
        exited, _, candidate = transition(previous, change_1h=-2, change_24h=22)
        self.assertIsNone(candidate)
        reentered, _, candidate = transition(exited, change_1h=12, change_24h=22)
        self.assertEqual("REENTRY", candidate["event"])
        self.assertEqual("UP", candidate["direction"])
        self.assertEqual(2, reentered["notification_1h"]["episode_id"])
        _, _, escalation = transition(reentered, change_1h=21, change_24h=22)
        self.assertEqual("ESCALATION", escalation["event"])
        self.assertEqual("T2", escalation["severity_tier"])

    def test_legacy_1h_active_is_baselined_without_repeat(self):
        previous = active_state(change_1h=12, change_24h=12)
        previous.pop("notification_1h")
        state, _, candidate = transition(previous, change_1h=13, change_24h=13)
        self.assertIsNone(candidate)
        self.assertTrue(state["notification_1h"]["active"])


class RadarStateFileSafetyTests(unittest.TestCase):
    def _paths(self, root):
        return {
            "scan_status_path": root / "scan_status.json",
            "snapshot_path": root / "snapshot.json",
            "triggers_path": root / "triggers.json",
            "state_path": root / "radar_state.json",
            "events_path": root / "events.json",
            "notifications_path": root / "notifications.json",
        }

    def test_existing_24h_state_does_not_hide_new_1h_alert(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            previous = active_state(change_1h=0, change_24h=12)
            previous["coingecko_id"] = "aaa-token"
            previous.pop("notification_1h")  # Existing persisted state before the rule change.
            paths["state_path"].write_text(
                json.dumps({"schema_version": 1, "assets": {"AAA": previous}}),
                encoding="utf-8",
            )
            generated_at = "2026-09-22T00:00:00Z"
            run_id = "first_1h_signal"
            snapshot = {
                "symbol": "AAA", "coingecko_id": "aaa-token", "current_price": 1.25,
                "price_change_percentage_1h": 11, "price_change_percentage_24h": 12,
            }
            documents = {
                paths["scan_status_path"]: {
                    "run_id": run_id, "scan_status": "MARKET_DATA_COMPLETE",
                    "market_data_returned": 1, "mapped_total": 1,
                    "scan_finished_at": generated_at,
                },
                paths["snapshot_path"]: {
                    "run_id": run_id, "generated_at": generated_at,
                    "count": 1, "items": [snapshot],
                },
                paths["triggers_path"]: {
                    "run_id": run_id, "generated_at": generated_at, "count": 1,
                    "items": [{**snapshot, "trigger_1h_up": True,
                               "trigger_1h_down": False, "trigger_24h_up": True,
                               "trigger_24h_down": False}],
                },
            }
            for path, payload in documents.items():
                path.write_text(json.dumps(payload), encoding="utf-8")

            summary = run_state_update(**paths)
            notifications = json.loads(paths["notifications_path"].read_text(encoding="utf-8"))
            state = json.loads(paths["state_path"].read_text(encoding="utf-8"))
            self.assertEqual(0, summary["exit_code"])
            self.assertEqual(1, summary["notification_candidates"])
            self.assertEqual("NEW_TRIGGER", notifications["assets"][0]["event"])
            self.assertEqual(["1H_UP"], notifications["assets"][0]["active_conditions"])
            self.assertTrue(state["assets"]["AAA"]["notification_1h"]["active"])

    def test_case_8_incomplete_scan_does_not_modify_state_or_create_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            original_state = '{"schema_version":1,"assets":{"AAA":{"active":true}}}\n'
            paths["state_path"].write_text(original_state, encoding="utf-8")
            paths["scan_status_path"].write_text(
                json.dumps(
                    {
                        "scan_status": "SCAN_INCOMPLETE",
                        "market_data_returned": 161,
                        "mapped_total": 162,
                    }
                ),
                encoding="utf-8",
            )
            summary = run_state_update(**paths)
            self.assertNotEqual(0, summary["exit_code"])
            self.assertEqual(original_state, paths["state_path"].read_text(encoding="utf-8"))
            events = json.loads(paths["events_path"].read_text(encoding="utf-8"))
            self.assertEqual("STATE_UPDATE_SKIPPED", events["status"])
            self.assertFalse(any(item.get("event") == "EXIT" for item in events["events"]))

    def test_case_9_bootstrap_162_assets_10_active_and_zero_notifications(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            generated_at = "2026-09-17T01:00:00Z"
            snapshot_items = []
            trigger_items = []
            for index in range(162):
                symbol = f"C{index:03d}"
                change = 11 if index < 10 else 1
                snapshot = {
                    "symbol": symbol,
                    "current_price": 1,
                    "price_change_percentage_1h": 0,
                    "price_change_percentage_24h": change,
                }
                snapshot_items.append(snapshot)
                if index < 10:
                    trigger_items.append(
                        {
                            **snapshot,
                            "trigger_1h_up": False,
                            "trigger_1h_down": False,
                            "trigger_24h_up": True,
                            "trigger_24h_down": False,
                        }
                    )
            documents = {
                paths["scan_status_path"]: {
                    "run_id": "test_run",
                    "scan_status": "MARKET_DATA_COMPLETE",
                    "market_data_returned": 162,
                    "mapped_total": 162,
                    "scan_finished_at": generated_at,
                },
                paths["snapshot_path"]: {
                    "run_id": "test_run",
                    "generated_at": generated_at,
                    "count": 162,
                    "items": snapshot_items,
                },
                paths["triggers_path"]: {
                    "run_id": "test_run",
                    "generated_at": generated_at,
                    "count": 10,
                    "items": trigger_items,
                },
            }
            for path, payload in documents.items():
                path.write_text(json.dumps(payload), encoding="utf-8")

            summary = run_state_update(bootstrap=True, **paths)
            state = json.loads(paths["state_path"].read_text(encoding="utf-8"))
            notifications = json.loads(paths["notifications_path"].read_text(encoding="utf-8"))
            self.assertEqual(0, summary["exit_code"])
            self.assertEqual(162, len(state["assets"]))
            self.assertEqual(10, sum(item["active"] for item in state["assets"].values()))
            self.assertEqual(0, notifications["count"])
            self.assertEqual("BOOTSTRAP_SUPPRESSED", notifications["status"])

    def test_selective_baseline_preserves_existing_state_and_resets_changed_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            generated_at = "2026-09-20T10:00:00Z"
            snapshot_items = [
                {
                    "symbol": symbol,
                    "coingecko_id": coin_id,
                    "current_price": 1,
                    "price_change_percentage_1h": 0,
                    "price_change_percentage_24h": 12,
                }
                for symbol, coin_id in (
                    ("KEEP", "keep-token"),
                    ("AVA", "ava-ai"),
                    ("NEW", "new-token"),
                )
            ]
            trigger_items = [
                {
                    **item,
                    "trigger_1h_up": False,
                    "trigger_1h_down": False,
                    "trigger_24h_up": True,
                    "trigger_24h_down": False,
                }
                for item in snapshot_items
            ]
            documents = {
                paths["scan_status_path"]: {
                    "run_id": "migration_run",
                    "scan_status": "MARKET_DATA_COMPLETE",
                    "market_data_returned": 3,
                    "mapped_total": 3,
                    "scan_finished_at": generated_at,
                },
                paths["snapshot_path"]: {
                    "run_id": "migration_run",
                    "generated_at": generated_at,
                    "count": 3,
                    "items": snapshot_items,
                },
                paths["triggers_path"]: {
                    "run_id": "migration_run",
                    "generated_at": generated_at,
                    "count": 3,
                    "items": trigger_items,
                },
                paths["state_path"]: {
                    "schema_version": 1,
                    "assets": {
                        "KEEP": {
                            "active": True,
                            "direction": "UP",
                            "severity_tier": "T1",
                            "abnormality_score": 11,
                            "episode_id": 7,
                        },
                        "AVA": {
                            "active": True,
                            "direction": "UP",
                            "severity_tier": "T1",
                            "abnormality_score": 11,
                            "episode_id": 4,
                        },
                        "REMOVED": {"active": False, "episode_id": 2},
                    },
                },
            }
            for path, payload in documents.items():
                path.write_text(json.dumps(payload), encoding="utf-8")

            summary = run_state_update(**paths)
            state = json.loads(paths["state_path"].read_text(encoding="utf-8"))
            notifications = json.loads(paths["notifications_path"].read_text(encoding="utf-8"))

            self.assertEqual(["AVA", "NEW"], summary["baselined_symbols"])
            self.assertEqual(0, notifications["count"])
            self.assertEqual(7, state["assets"]["KEEP"]["episode_id"])
            self.assertFalse(state["assets"]["KEEP"]["baseline_required"])
            self.assertEqual("ava-ai", state["assets"]["AVA"]["coingecko_id"])
            self.assertEqual(1, state["assets"]["AVA"]["episode_id"])
            self.assertTrue(state["assets"]["AVA"]["baseline_required"])
            self.assertNotIn("REMOVED", state["assets"])


if __name__ == "__main__":
    unittest.main()
