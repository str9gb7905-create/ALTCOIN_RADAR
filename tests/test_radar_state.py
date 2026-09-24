import json
import tempfile
import unittest
from pathlib import Path

from radar_state import (
    evaluate_transition,
    extrema_price_move,
    one_hour_signal,
    run_state_update,
    signal_from_changes,
    update_price_history,
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

    def test_recent_range_uses_low_as_upward_reference(self):
        previous = {
            "price_history": [
                {"timestamp": "2026-09-23T07:54:00Z", "price": 0.00280},
                {"timestamp": "2026-09-23T08:09:00Z", "price": 0.002823264084782347},
                {"timestamp": "2026-09-23T08:24:00Z", "price": 0.00290},
            ]
        }
        history = update_price_history(
            previous, 0.003165022183509337, "2026-09-23T09:09:00Z"
        )
        move = extrema_price_move(history)
        self.assertEqual("UP", move["direction"])
        self.assertEqual("2026-09-23T07:54:00Z", move["reference_time"])
        self.assertAlmostEqual(13.036506, move["change_pct"], places=5)
        self.assertEqual(4, len(history))

    def test_recent_range_starts_at_zero_with_one_sample(self):
        history = update_price_history(
            None, 1.0, "2026-09-23T09:09:00Z"
        )
        move = extrema_price_move(history)
        self.assertEqual(1, len(history))
        self.assertEqual(0.0, move["change_pct"])
        signal = one_hour_signal({
            "symbol": "AAA", "price": 1.0,
            "trigger_change_pct": move["change_pct"],
            "trigger_direction": move["direction"],
        })
        self.assertFalse(signal["active"])
        self.assertEqual("NONE", signal["direction"])

    def test_five_percent_then_six_percent_more_triggers_negative_ten_level(self):
        history = update_price_history(None, 100.0, "2026-09-23T00:00:00Z")
        history = update_price_history(
            {"price_history": history}, 95.0, "2026-09-23T01:00:00Z"
        )
        first_move = extrema_price_move(history)
        first_signal = one_hour_signal({
            "symbol": "AAA", "price": 95.0,
            "trigger_change_pct": first_move["change_pct"],
            "trigger_direction": first_move["direction"],
        })
        self.assertFalse(first_signal["active"])

        history = update_price_history(
            {"price_history": history}, 89.3, "2026-09-23T02:00:00Z"
        )
        second_move = extrema_price_move(history)
        second_signal = one_hour_signal({
            "symbol": "AAA", "price": 89.3,
            "trigger_change_pct": second_move["change_pct"],
            "trigger_direction": second_move["direction"],
        })
        self.assertAlmostEqual(-10.7, second_move["change_pct"], places=6)
        self.assertTrue(second_signal["active"])
        self.assertEqual(10, second_signal["threshold_level"])
        self.assertEqual(["FROM_24H_HIGH_DOWN"], second_signal["active_conditions"])

    def test_each_new_ten_percent_level_escalates_without_same_level_repeat(self):
        first = signal_from_changes("AAA", 0, 0, price=1.35)
        first.update({
            "trigger_change_pct": 35.0,
            "trigger_direction": "UP",
            "trigger_reference_price": 1.0,
        })
        state, _, candidate = evaluate_transition(None, first, NOW)
        self.assertEqual("NEW_TRIGGER", candidate["event"])
        self.assertEqual(30, candidate["threshold_level"])

        same_level = signal_from_changes("AAA", 0, 0, price=1.39)
        same_level.update({"trigger_change_pct": 39.0, "trigger_direction": "UP"})
        state, _, candidate = evaluate_transition(state, same_level, NOW)
        self.assertIsNone(candidate)

        next_level = signal_from_changes("AAA", 0, 0, price=1.41)
        next_level.update({"trigger_change_pct": 41.0, "trigger_direction": "UP"})
        _, _, candidate = evaluate_transition(state, next_level, NOW)
        self.assertEqual("ESCALATION", candidate["event"])
        self.assertEqual(40, candidate["threshold_level"])

    def test_price_history_drops_samples_older_than_twenty_four_hours(self):
        previous = {
            "price_history": [
                {"timestamp": "2026-09-21T23:59:00Z", "price": 50.0},
                {"timestamp": "2026-09-22T12:00:00Z", "price": 90.0},
            ]
        }
        history = update_price_history(previous, 100.0, "2026-09-23T00:00:00Z")
        self.assertEqual(2, len(history))
        self.assertNotIn(50.0, [item["price"] for item in history])


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

    def test_existing_24h_state_does_not_hide_new_recent_range_alert(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            previous = active_state(change_1h=0, change_24h=12)
            previous["coingecko_id"] = "aaa-token"
            previous.pop("notification_1h")  # Existing persisted state before the rule change.
            previous["price_history"] = [
                {"timestamp": "2026-09-21T23:00:00Z", "price": 1.0}
            ]
            paths["state_path"].write_text(
                json.dumps({"schema_version": 1, "assets": {"AAA": previous}}),
                encoding="utf-8",
            )
            generated_at = "2026-09-22T00:00:00Z"
            run_id = "first_1h_signal"
            snapshot = {
                "symbol": "AAA", "coingecko_id": "aaa-token", "current_price": 1.25,
                "price_change_percentage_1h": 7.8, "price_change_percentage_24h": 12,
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
            self.assertEqual(
                ["FROM_24H_LOW_UP"], notifications["assets"][0]["active_conditions"]
            )
            self.assertEqual(25.0, notifications["assets"][0]["change_1h"])
            self.assertEqual(25.0, notifications["assets"][0]["change_pct"])
            self.assertEqual(7.8, notifications["assets"][0]["coingecko_change_1h"])
            self.assertEqual("RANGE_24H_EXTREMA", notifications["assets"][0]["calculation_method"])
            self.assertTrue(state["assets"]["AAA"]["notification_move"]["active"])

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
