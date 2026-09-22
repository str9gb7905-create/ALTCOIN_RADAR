import json
import os
import socket
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from notification_delivery import NotificationDeliveryLedger
from notification_dispatcher import (
    DispatchResult,
    NotificationConfigurationError,
    NotificationDispatcher,
    TelegramDispatcher,
)
from notification_merger import merge_payloads
from notification_merger import POLICY_PATH
from state_store import FileSystemStateStore


RUN_ID = "20260919T000000Z_test"


def inputs(*, price=None, technical=None, divergence=None):
    market = {
        "symbol": "AAA", "coingecko_id": "aaa-token", "current_price": 1.25,
        "price_change_percentage_1h": 3.0, "price_change_percentage_24h": 12.0,
    }
    stage_b = {
        "symbol": "AAA", "price": 1.25, "change_1h": 3.0, "change_24h": 12.0,
        "rsi_4h": 72.0, "rsi_1d": 61.0, "rsi_1w": 55.0,
        "volume_ratio": 2.4, "volume_status": "STRONG", "volume_pattern": "NONE",
        "volume_note": "NONE", "live_source": "Binance", "history_source": "Binance",
        "cross_exchange_prices": [
            {"exchange": "Binance", "pair": "AAAUSDT", "quote": "USDT", "price": 1.25}
        ],
    }
    div_asset = {
        "symbol": "AAA", "daily": {"bullish": {"confirmed": True}, "bearish": None},
        "weekly": {"bullish": None, "bearish": None},
    }
    return (
        {"run_id": RUN_ID, "assets": price or []},
        {"run_id": RUN_ID, "notification_events": technical or []},
        {"run_id": RUN_ID, "events": divergence or []},
        {"run_id": RUN_ID, "assets": [market]},
        {"run_id": RUN_ID, "assets": [stage_b]},
        {"run_id": RUN_ID, "assets": [div_asset]},
    )


def ready_payload():
    return merge_payloads(*inputs(
        price=[{"symbol": "AAA", "event": "NEW_TRIGGER"}],
        technical=[{"symbol": "AAA", "event": "VOLUME_STRONG_NEW", "notify": True}],
        divergence=[{"symbol": "AAA", "event": "DIVERGENCE_NEW", "timeframe": "daily"}],
    ))


class FakeDispatcher(NotificationDispatcher):
    def __init__(self, *results):
        self.results = list(results)
        self.messages = []

    def send(self, message):
        self.messages.append(message)
        return self.results.pop(0)

    def send_health_alert(self, message):
        return self.send(f"HEALTH: {message}")


class FakeResponse:
    status = 200

    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.body


class NotificationDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = Path("state/notification_delivery.json")
        self.times = iter([
            "2026-09-19T00:00:00Z", "2026-09-19T00:00:01Z",
            "2026-09-19T00:00:02Z", "2026-09-19T00:00:03Z",
            "2026-09-19T00:00:04Z", "2026-09-19T00:00:05Z",
            "2026-09-19T00:00:06Z", "2026-09-19T00:00:07Z",
            "2026-09-19T00:00:08Z", "2026-09-19T00:00:09Z",
        ])
        self.ledger = NotificationDeliveryLedger(
            store=FileSystemStateStore(self.root), path=self.path, now=lambda: next(self.times)
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_no_event_means_no_send_and_no_ledger_entry(self):
        merged = merge_payloads(*inputs())
        self.assertEqual(merged["status"], "NO_NOTIFICATION")
        self.assertIsNone(self.ledger.prepare(merged))
        self.assertFalse((self.root / self.path).exists())

    def test_merged_events_send_exactly_once_and_share_symbol_block(self):
        merged = ready_payload()
        self.assertEqual(merged["event_count"], 3)
        self.assertEqual(len(merged["items"]), 1)
        self.ledger.prepare(merged)
        claimed = self.ledger.claim_next()
        dispatcher = FakeDispatcher(DispatchResult("SUCCESS", "TELEGRAM", "123"))
        result = self.ledger.dispatch_claimed(claimed["notification_id"], dispatcher)
        self.assertEqual(result["delivery_status"], "SUCCESS")
        self.assertEqual(len(dispatcher.messages), 1)

    def test_continuing_is_suppressed(self):
        merged = merge_payloads(*inputs(
            price=[{"symbol": "AAA", "event": "CONTINUING"}]
        ))
        self.assertEqual(merged["status"], "NO_NOTIFICATION")

    def test_standalone_technical_event_does_not_send(self):
        merged = merge_payloads(*inputs(
            technical=[{"symbol": "AAA", "event": "VOLUME_STRONG_NEW", "notify": True}]
        ))
        self.assertEqual(merged["status"], "NO_NOTIFICATION")

    def test_mainstream_price_event_is_excluded_from_immediate_notification(self):
        merged = merge_payloads(*inputs(
            price=[{"symbol": "BTC", "event": "NEW_TRIGGER"}]
        ))
        self.assertEqual(merged["status"], "NO_NOTIFICATION")

    def test_production_policy_has_the_confirmed_fifteen_mainstream_assets(self):
        policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            {
                "BTC", "UNI", "SOL", "XRP", "AVAX", "SUI", "HBAR", "DOT",
                "APT", "ETH", "DOGE", "POL", "LINK", "ADA", "XLM",
            },
            set(policy["immediate_price_excluded_symbols"]),
        )
        self.assertFalse(policy["standalone_technical_alerts_enabled"])
        self.assertFalse(policy["standalone_divergence_alerts_enabled"])

    def test_production_message_is_chinese_and_contains_clickable_sources(self):
        merged = ready_payload()
        self.assertIn("🚨 ALTCOIN RADAR｜即時價格異動", merged["message"])
        self.assertIn("https://www.coingecko.com/en/coins/aaa-token", merged["message"])
        self.assertIn(
            "https://www.tradingview.com/chart/?symbol=BINANCE%3AAAAUSDT",
            merged["message"],
        )

    def test_broad_market_move_keeps_every_symbol_within_telegram_limit(self):
        payloads = list(inputs())
        price_assets = []
        snapshot_assets = []
        for index in range(165):
            symbol = f"C{index:03d}"
            price_assets.append({
                "symbol": symbol,
                "event": "NEW_TRIGGER",
                "change_1h": -11.0,
                "change_24h": -20.0 - index,
                "abnormality_score": 20.0 + index,
                "severity_tier": "T2",
                "active_conditions": ["24H_DOWN"],
            })
            snapshot_assets.append({
                "symbol": symbol,
                "coingecko_id": f"coin-{index}",
                "current_price": 1.0,
                "price_change_percentage_1h": -11.0,
                "price_change_percentage_24h": -20.0 - index,
            })
        payloads[0] = {"run_id": RUN_ID, "assets": price_assets}
        payloads[3] = {"run_id": RUN_ID, "items": snapshot_assets}
        merged = merge_payloads(*payloads, policy={
            "immediate_price_excluded_symbols": [],
        })
        self.assertLessEqual(len(merged["message"]), 4096)
        self.assertIn("其他同批觸發", merged["message"])
        for item in price_assets:
            self.assertIn(item["symbol"], merged["message"])

    def test_duplicate_workflow_retry_does_not_duplicate_delivery(self):
        merged = ready_payload()
        self.ledger.prepare(merged)
        claimed = self.ledger.claim_next()
        dispatcher = FakeDispatcher(DispatchResult("SUCCESS", "TELEGRAM", "123"))
        self.ledger.dispatch_claimed(claimed["notification_id"], dispatcher)
        self.ledger.prepare(merged)
        self.assertIsNone(self.ledger.claim_next())
        self.assertEqual(len(dispatcher.messages), 1)

    def test_confirmed_success_saves_message_id_and_delivery_time(self):
        merged = ready_payload()
        self.ledger.prepare(merged)
        claimed = self.ledger.claim_next()
        dispatcher = FakeDispatcher(DispatchResult("SUCCESS", "TELEGRAM", "456"))
        result = self.ledger.dispatch_claimed(claimed["notification_id"], dispatcher)
        self.assertEqual(result["provider_message_id"], "456")
        self.assertIsNotNone(result["delivered_at_utc"])

    def test_confirmed_failure_is_retryable_then_can_succeed(self):
        merged = ready_payload()
        self.ledger.prepare(merged)
        claimed = self.ledger.claim_next()
        dispatcher = FakeDispatcher(
            DispatchResult("FAILED_RETRYABLE", "TELEGRAM", error="HTTP_503"),
            DispatchResult("SUCCESS", "TELEGRAM", "789"),
        )
        failed = self.ledger.dispatch_claimed(claimed["notification_id"], dispatcher)
        self.assertEqual(failed["delivery_status"], "FAILED_RETRYABLE")
        retry = self.ledger.claim_next()
        success = self.ledger.dispatch_claimed(retry["notification_id"], dispatcher)
        self.assertEqual(success["delivery_status"], "SUCCESS")
        self.assertEqual(success["attempt_count"], 2)

    def test_timeout_becomes_unknown_and_never_auto_retries(self):
        merged = ready_payload()
        self.ledger.prepare(merged)
        claimed = self.ledger.claim_next()
        dispatcher = FakeDispatcher(DispatchResult("UNKNOWN_TIMEOUT", "TELEGRAM", error="TIMEOUT"))
        result = self.ledger.dispatch_claimed(claimed["notification_id"], dispatcher)
        self.assertEqual(result["delivery_status"], "UNKNOWN_TIMEOUT")
        self.assertIsNone(self.ledger.claim_next())
        self.assertEqual(len(dispatcher.messages), 1)

    def test_abandoned_sending_becomes_unknown_without_send(self):
        self.ledger.prepare(ready_payload())
        self.ledger.claim_next()
        self.assertIsNone(self.ledger.claim_next())
        record = next(iter(self.ledger.load()["notifications"].values()))
        self.assertEqual(record["delivery_status"], "UNKNOWN_TIMEOUT")

    def test_secret_absent_names_variables_without_values(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(NotificationConfigurationError, "TELEGRAM_BOT_TOKEN"):
                TelegramDispatcher.from_env()

    def test_dispatcher_redacts_secret_from_network_error(self):
        token = "secret-token-value"
        chat_id = "secret-chat-id"

        def failing_opener(*_args, **_kwargs):
            raise urllib.error.URLError(f"network failed {token} {chat_id}")

        result = TelegramDispatcher(token, chat_id, opener=failing_opener).send("hello")
        self.assertEqual(result.status, "UNKNOWN_TIMEOUT")
        self.assertNotIn(token, str(result.to_dict()))
        self.assertNotIn(chat_id, str(result.to_dict()))

    def test_telegram_timeout_is_unknown(self):
        def timeout_opener(*_args, **_kwargs):
            raise urllib.error.URLError(socket.timeout("late response"))

        result = TelegramDispatcher("token", "chat", opener=timeout_opener).send("hello")
        self.assertEqual(result.status, "UNKNOWN_TIMEOUT")

    def test_telegram_confirmed_response_returns_message_id(self):
        opener = lambda *_args, **_kwargs: FakeResponse(
            b'{"ok":true,"result":{"message_id":321}}'
        )
        result = TelegramDispatcher("token", "chat", opener=opener).send("hello")
        self.assertEqual(result, DispatchResult("SUCCESS", "TELEGRAM", "321"))

    def test_telegram_confirmed_rejection_is_permanent(self):
        def rejected(*_args, **_kwargs):
            raise urllib.error.HTTPError("redacted", 401, "unauthorized", {}, None)

        result = TelegramDispatcher("token", "chat", opener=rejected).send("hello")
        self.assertEqual(result.status, "FAILED_PERMANENT")
        self.assertEqual(result.error, "HTTP_401")

    def test_notification_id_is_deterministic_across_input_order(self):
        first = merge_payloads(*inputs(technical=[
            {"symbol": "AAA", "event": "VOLUME_STRONG_NEW", "notify": True, "value": 2.4},
            {"symbol": "AAA", "event": "RSI_4H_EXTREME_HOT_NEW", "notify": True, "value": 82},
        ]))
        second = merge_payloads(*inputs(technical=[
            {"symbol": "AAA", "event": "RSI_4H_EXTREME_HOT_NEW", "notify": True, "value": 82},
            {"symbol": "AAA", "event": "VOLUME_STRONG_NEW", "notify": True, "value": 2.4},
        ]))
        self.assertEqual(first["notification_id"], second["notification_id"])
        self.assertEqual(first["message"], second["message"])

    def test_health_alert_uses_dispatcher_interface(self):
        opener = lambda *_args, **_kwargs: FakeResponse(
            b'{"ok":true,"result":{"message_id":99}}'
        )
        result = TelegramDispatcher("token", "chat", opener=opener).send_health_alert("degraded")
        self.assertEqual(result.status, "SUCCESS")


if __name__ == "__main__":
    unittest.main()
