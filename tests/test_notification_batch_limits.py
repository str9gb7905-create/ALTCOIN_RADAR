import copy
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from notification_delivery import NotificationDeliveryLedger
from notification_delivery_runner import drain_queue
from notification_dispatcher import DispatchResult
from notification_merger import notification_parts, telegram_length
from state_store import FileSystemStateStore
from test_notification_delivery import FakeDispatcher, ready_payload


class BatchLimitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.clock = datetime(2026, 10, 13, tzinfo=timezone.utc)
        self.store = FileSystemStateStore(Path(self.temp.name))
        self.path = Path("state/notification_delivery.json")
        self.ledger = self.restart()

    def restart(self):
        return NotificationDeliveryLedger(store=self.store, path=self.path,
                                          now=lambda: self.clock.isoformat())

    def payload(self, number, level=20, direction=-1, symbols=("AAA",)):
        source = ready_payload()
        source["run_id"] = f"run_{number}"
        source["notification_id"] = f"ntf_{number}"
        source["items"] = [dict(source["items"][0], symbol=symbol,
                                trigger_level=level, change_pct=direction * (level + 1),
                                active_conditions=["FROM_24H_LOW_UP" if direction > 0
                                                   else "FROM_24H_HIGH_DOWN"])
                           for symbol in symbols]
        source["parts"] = notification_parts(source["run_id"], source["items"])
        source["message"] = source["parts"][0]["message"]
        return source

    def send(self, number, **kwargs):
        record = self.ledger.prepare(self.payload(number, **kwargs))
        if record is not None:
            claimed = self.ledger.claim_next()
            self.ledger.dispatch_claimed(claimed["notification_id"], FakeDispatcher(
                DispatchResult("SUCCESS", "TELEGRAM", str(number))))
        return record

    def test_three_then_suppress_across_restart_and_at_24h_expiry(self):
        for number in range(3):
            self.assertIsNotNone(self.send(number))
            self.clock += timedelta(minutes=15)
            self.ledger = self.restart()
        self.assertIsNone(self.send(3))
        self.clock = datetime(2026, 10, 14, tzinfo=timezone.utc)
        self.assertIsNotNone(self.send(4))

    def test_lower_band_does_not_reset_quota_new_high_band_and_direction_do(self):
        for number in range(3):
            self.send(number, level=20)
        self.assertIsNone(self.send(3, level=10))
        self.assertIsNotNone(self.send(4, level=30))
        self.assertIsNotNone(self.send(5, level=10, direction=1))
        self.assertIsNotNone(self.send(6, symbols=("BBB",)))

    def test_filtering_one_asset_keeps_others_and_replay_is_idempotent(self):
        for number in range(3):
            self.send(number)
        payload = self.payload(4, symbols=("AAA", "BBB"))
        record = self.ledger.prepare(payload)
        self.assertNotIn("AAA｜", record["message"])
        self.assertIn("BBB｜", record["message"])
        self.assertEqual(1, record["repeat_suppressed_count"])
        self.assertEqual(record["notification_id"], self.ledger.prepare(payload)["notification_id"])

    def test_old_success_records_count_but_overflow_symbols_do_not(self):
        for number in range(3):
            self.send(number)
        state = self.ledger.load()
        for record in state["notifications"].values():
            del record["alert_ranges"]
            record["message"] += "\n其他同批觸發（1 種）：\nBBB"
        self.store.save_atomic(self.path, state)
        self.assertIsNone(self.send(4))
        self.assertIsNotNone(self.send(5, symbols=("BBB",)))

    def test_pending_and_unknown_reserve_quota_permanent_failure_does_not(self):
        for number in range(3):
            self.ledger.prepare(self.payload(number))
        self.assertIsNone(self.ledger.prepare(self.payload(4)))
        state = self.ledger.load()
        records = list(state["notifications"].values())
        records[0]["delivery_status"] = "UNKNOWN_TIMEOUT"
        self.store.save_atomic(self.path, state)
        self.assertIsNone(self.ledger.prepare(self.payload(5)))
        records[1]["delivery_status"] = "FAILED_PERMANENT"
        self.store.save_atomic(self.path, state)
        self.assertIsNotNone(self.ledger.prepare(self.payload(6)))

    def test_configurable_limit_and_no_midnight_reset(self):
        self.clock = self.clock.replace(hour=23, minute=59)
        payload = self.payload(0)
        payload["repeat_policy"] = {"window_hours": 24, "max_per_range": 1}
        self.ledger.prepare(payload)
        self.clock += timedelta(minutes=2)
        next_payload = self.payload(1)
        next_payload["repeat_policy"] = payload["repeat_policy"]
        self.assertIsNone(self.ledger.prepare(next_payload))

    def test_all_parts_are_durable_and_sent_same_run_with_claim_before_send(self):
        payload = self.payload(1, symbols=tuple(f"C{i:03}" for i in range(175)))
        self.ledger.prepare(payload)
        count = len(payload["parts"])
        events = []
        dispatcher = FakeDispatcher(*[DispatchResult("SUCCESS", "TELEGRAM", str(i))
                                      for i in range(count)])
        original_send = dispatcher.send
        def send(message):
            self.assertEqual("claim", events[-1])
            self.assertLessEqual(telegram_length(message), 4096)
            events.append("send")
            return original_send(message)
        dispatcher.send = send
        result = drain_queue(self.ledger, dispatcher, events.append, pause=lambda _: None)
        self.assertEqual(count, result["sent"])
        self.assertEqual("SUCCESS", result["status"])
        self.assertEqual(count, len(dispatcher.messages))
        self.assertEqual(["claim", "send", "result"] * count + ["claim"], events)
        self.ledger = self.restart()
        self.ledger.prepare(payload)
        self.assertIsNone(self.ledger.claim_next())

    def test_failed_part_does_not_block_other_parts_or_repeat_in_same_run(self):
        payload = self.payload(1, symbols=tuple(f"C{i:03}" for i in range(18)))
        self.ledger.prepare(payload)
        count = len(payload["parts"])
        self.assertGreater(count, 1)
        dispatcher = FakeDispatcher(DispatchResult("FAILED_RETRYABLE", "TELEGRAM", error="HTTP_429"),
                                    *[DispatchResult("SUCCESS", "TELEGRAM", str(i))
                                      for i in range(count - 1)])
        result = drain_queue(self.ledger, dispatcher, lambda _: None, pause=lambda _: None)
        self.assertEqual("FAILED", result["status"])
        self.assertEqual(count, len(dispatcher.messages))
        retry = self.restart().claim_next()
        self.assertEqual(2, retry["attempt_count"])

    def test_failed_claim_persistence_never_sends(self):
        self.ledger.prepare(self.payload(1))
        dispatcher = FakeDispatcher()
        def fail(_phase):
            raise RuntimeError("persistence unavailable")
        with self.assertRaises(RuntimeError):
            drain_queue(self.ledger, dispatcher, fail, pause=lambda _: None)
        self.assertEqual([], dispatcher.messages)

    def test_restart_after_result_persistence_failure_does_not_resend_unknown(self):
        self.ledger.prepare(self.payload(1))
        durable = None
        def persist(phase):
            nonlocal durable
            if phase == "claim":
                durable = copy.deepcopy(self.ledger.load())
            else:
                raise RuntimeError("push failed")
        dispatcher = FakeDispatcher(DispatchResult("SUCCESS", "TELEGRAM", "1"))
        with self.assertRaises(RuntimeError):
            drain_queue(self.ledger, dispatcher, persist, pause=lambda _: None)
        self.store.save_atomic(self.path, durable)
        self.assertIsNone(self.restart().claim_next())
        self.assertEqual(1, len(dispatcher.messages))


if __name__ == "__main__":
    unittest.main()
