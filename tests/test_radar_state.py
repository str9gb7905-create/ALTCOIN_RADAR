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
    def test_case_1_inactive_to_threshold_is_new_trigger_and_notifies(self):
        previous = {"active": False, "direction": "NONE", "severity_tier": "T0", "episode_id": 0}
        state, event, candidate = transition(previous, change_24h=11)
        self.assertEqual("NEW_TRIGGER", event["event"])
        self.assertIsNotNone(candidate)
        self.assertTrue(state["active"])

    def test_case_2_same_tier_is_continuing_without_notification(self):
        previous = active_state(change_24h=11)
        _, event, candidate = transition(previous, change_24h=17)
        self.assertEqual("CONTINUING", event["event"])
        self.assertIsNone(candidate)

    def test_case_3_t1_to_t2_is_escalation_and_notifies(self):
        previous = active_state(change_24h=11)
        _, event, candidate = transition(previous, change_24h=20.1)
        self.assertEqual("ESCALATION", event["event"])
        self.assertIsNotNone(candidate)

    def test_case_4_tier_decrease_does_not_notify(self):
        previous = active_state(change_24h=25)
        _, event, candidate = transition(previous, change_24h=18)
        self.assertEqual("CONTINUING", event["event"])
        self.assertIsNone(candidate)

    def test_case_5_up_to_down_is_direction_change_and_notifies(self):
        previous = active_state(change_24h=15)
        _, event, candidate = transition(previous, change_24h=-13)
        self.assertEqual("DIRECTION_CHANGE", event["event"])
        self.assertEqual("UP", event["previous_direction"])
        self.assertEqual("DOWN", event["current_direction"])
        self.assertIsNotNone(candidate)

    def test_case_6_active_to_below_threshold_is_exit_without_notification(self):
        previous = active_state(change_24h=15)
        state, event, candidate = transition(previous, change_24h=5)
        self.assertEqual("EXIT", event["event"])
        self.assertIsNone(candidate)
        self.assertFalse(state["active"])
        self.assertEqual(NOW, state["last_exit_at"])

    def test_case_7_exit_then_reentry_increments_episode_and_notifies(self):
        previous = active_state(change_24h=15, episode_id=1)
        exited, exit_event, _ = transition(previous, change_24h=5)
        reentered, reentry_event, candidate = transition(exited, change_24h=12)
        self.assertEqual("EXIT", exit_event["event"])
        self.assertEqual("REENTRY", reentry_event["event"])
        self.assertEqual(2, reentered["episode_id"])
        self.assertIsNotNone(candidate)

    def test_case_10_mixed_direction_keeps_both_conditions(self):
        current = signal_from_changes("AAA", 12, -11)
        self.assertEqual("MIXED", current["direction"])
        self.assertEqual(["1H_UP", "24H_DOWN"], current["active_conditions"])

    def test_case_11_t4_95_to_t4_80_continues_without_notification(self):
        previous = active_state(change_24h=95)
        _, event, candidate = transition(previous, change_24h=80)
        self.assertEqual("CONTINUING", event["event"])
        self.assertIsNone(candidate)

    def test_case_12_t4_95_to_t5_105_escalates_and_notifies(self):
        previous = active_state(change_24h=95)
        _, event, candidate = transition(previous, change_24h=105)
        self.assertEqual("ESCALATION", event["event"])
        self.assertIsNotNone(candidate)


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


if __name__ == "__main__":
    unittest.main()
