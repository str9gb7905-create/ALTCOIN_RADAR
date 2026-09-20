import json
import tempfile
import unittest
from pathlib import Path

from stage_b_enrich import (
    cross_exchange_confirmation,
    enrich_asset,
    evaluate_technical_events,
    rsi_4h_state,
    rsi_from_candles,
    run_stage_b,
    technical_state_entry,
    volume_metrics,
    volume_status,
)
from stage_b_sources import Candle, PublicSpotAdapter, SpotMarket, build_adapters


def technical_record(rsi=None, ratio=None, change_24h=0, pattern="NONE", note="NONE"):
    record = {
        "live_source": "Gate",
        "history_source": "Gate",
        "rsi_4h": rsi,
        "rsi_1d": 50,
        "rsi_1w": 50,
        "rsi_4h_state": rsi_4h_state(rsi),
        "volume_ratio": ratio,
        "volume_status": volume_status(ratio),
        "volume_pattern": pattern,
        "volume_note": note,
        "range_24h_pct": 10,
        "turnover_ratio": 0.1,
        "cross_exchange_status": "CONFIRMED",
        "data_quality": "COMPLETE",
        "change_24h": change_24h,
    }
    return technical_state_entry(record, "2026-09-17T00:00:00Z", 1)


def event_names(previous, current):
    return [item["event"] for item in evaluate_technical_events("AAA", previous, current)]


class TechnicalEventTests(unittest.TestCase):
    def test_case_1_rsi_79_to_81_notifies_hot_new(self):
        names = event_names(technical_record(79, 1.0), technical_record(81, 1.0))
        self.assertIn("RSI_4H_EXTREME_HOT_NEW", names)

    def test_case_2_rsi_81_to_87_does_not_repeat(self):
        names = event_names(technical_record(81, 1.0), technical_record(87, 1.0))
        self.assertNotIn("RSI_4H_EXTREME_HOT_NEW", names)

    def test_case_3_rsi_clears_then_reenters_hot(self):
        hot = technical_record(81, 1.0)
        normal = technical_record(74, 1.0)
        cleared = event_names(hot, normal)
        reentered = event_names(normal, technical_record(82, 1.0))
        self.assertIn("TECHNICAL_SIGNAL_CLEARED", cleared)
        self.assertIn("RSI_4H_EXTREME_HOT_NEW", reentered)

    def test_case_4_rsi_22_to_19_notifies_cold_new(self):
        names = event_names(technical_record(22, 1.0), technical_record(19, 1.0))
        self.assertIn("RSI_4H_EXTREME_COLD_NEW", names)

    def test_case_5_volume_normal_to_elevated_escalates(self):
        names = event_names(technical_record(50, 1.2), technical_record(50, 1.7))
        self.assertIn("VOLUME_ESCALATION", names)

    def test_case_6_volume_elevated_to_strong_escalates(self):
        names = event_names(technical_record(50, 1.7), technical_record(50, 2.3))
        self.assertIn("VOLUME_ESCALATION", names)

    def test_case_7_volume_strong_to_strong_does_not_repeat(self):
        names = event_names(technical_record(50, 2.3), technical_record(50, 2.7))
        self.assertNotIn("VOLUME_STRONG_NEW", names)
        self.assertNotIn("VOLUME_ESCALATION", names)

    def test_case_8_volume_strong_to_normal_does_not_notify(self):
        events = evaluate_technical_events("AAA", technical_record(50, 2.3), technical_record(50, 1.3))
        self.assertFalse(any(item["notify"] for item in events))

    def test_case_9_price_up_without_volume_is_new(self):
        previous = technical_record(50, 1.2, change_24h=5)
        current = technical_record(50, 0.7, change_24h=15, note="PRICE_UP_WITHOUT_CLEAR_VOLUME")
        self.assertIn("PRICE_UP_WITHOUT_VOLUME_NEW", event_names(previous, current))


class TechnicalCalculationTests(unittest.TestCase):
    def test_case_10_fewer_than_7_windows_is_insufficient(self):
        candles = [Candle(index * 86_400_000, 1, 100, True) for index in range(6)]
        ratio, status, pattern = volume_metrics(200, candles)
        self.assertIsNone(ratio)
        self.assertEqual("DATA_INSUFFICIENT", status)
        self.assertEqual("NONE", pattern)

    def test_case_15_incomplete_4h_candle_is_not_used_for_rsi(self):
        complete = [Candle(index * 14_400_000, float(index + 1), 100, True) for index in range(20)]
        with_partial = complete + [Candle(20 * 14_400_000, -1000.0, 100, False)]
        self.assertEqual(rsi_from_candles(complete), rsi_from_candles(with_partial))

    def test_case_16_close_exchange_prices_are_confirmed(self):
        markets = [
            SpotMarket("A", "AAAUSDT", "AAA", "USDT", 100, 1000),
            SpotMarket("B", "AAAUSDT", "AAA", "USDT", 100.5, 900),
            SpotMarket("C", "AAAUSDT", "AAA", "USDT", 100.8, 800),
        ]
        status, _ = cross_exchange_confirmation(markets)
        self.assertEqual("CONFIRMED", status)

    def test_case_17_wide_exchange_prices_diverge(self):
        markets = [
            SpotMarket("A", "AAAUSDT", "AAA", "USDT", 100, 1000),
            SpotMarket("B", "AAAUSDT", "AAA", "USDT", 104, 900),
        ]
        status, _ = cross_exchange_confirmation(markets)
        self.assertEqual("EXCHANGE_DIVERGENCE", status)


class FakeAdapter(PublicSpotAdapter):
    def __init__(self, name, volume=1000, weekly_count=100):
        super().__init__(timeout=1)
        self.name = name
        self.volume = volume
        self.weekly_count = weekly_count

    def discover(self, target_symbols, quotes):
        return [
            SpotMarket(self.name, f"{symbol}USDT", symbol, "USDT", 1.0, self.volume)
            for symbol in sorted(target_symbols)
        ]

    def candles(self, market, timeframe, limit=120):
        count = self.weekly_count if timeframe == "1w" else 100
        step = {"4h": 14_400_000, "1d": 86_400_000, "1w": 604_800_000}[timeframe]
        return [
            Candle(index * step, 1 + index * 0.01, 100 + index, True)
            for index in range(min(count, limit))
        ]


class StageBWorkflowTests(unittest.TestCase):
    def _paths(self, root):
        return {
            "config_path": root / "config.json",
            "scan_status_path": root / "scan_status.json",
            "snapshot_path": root / "snapshot.json",
            "triggers_path": root / "triggers.json",
            "radar_state_path": root / "radar_state.json",
            "technical_state_path": root / "technical_state.json",
            "stage_b_path": root / "stage_b.json",
            "events_path": root / "events.json",
        }

    def _write_json(self, path, payload):
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _base_documents(self, paths, count=2):
        generated = "2026-09-17T01:00:00Z"
        symbols = [f"C{index}" for index in range(count)]
        config = {
            "allowed_quotes": ["USDT", "USD", "USDC"],
            "tie_break_order": ["Fake"],
            "excluded_exchanges": ["HTX", "Upbit"],
            "rsi_period": 14,
            "candle_target": 100,
            "volume_baseline_minimum": 7,
            "volume_baseline_target": 20,
        }
        snapshot_items = [
            {
                "symbol": symbol,
                "current_price": 1,
                "price_change_percentage_1h": 1,
                "price_change_percentage_24h": 12,
                "range_24h_pct": 15,
                "turnover_ratio": 0.2,
            }
            for symbol in symbols
        ]
        documents = {
            paths["config_path"]: config,
            paths["scan_status_path"]: {
                "run_id": "test_run",
                "scan_status": "MARKET_DATA_COMPLETE",
                "market_data_returned": count,
                "mapped_total": count,
                "scan_finished_at": generated,
            },
            paths["snapshot_path"]: {
                "run_id": "test_run",
                "generated_at": generated,
                "count": count,
                "items": snapshot_items,
            },
            paths["triggers_path"]: {
                "run_id": "test_run",
                "generated_at": generated,
                "count": count,
                "items": snapshot_items,
            },
            paths["radar_state_path"]: {
                "schema_version": 1,
                "last_run_id": "test_run",
                "assets": {symbol: {"active": True, "episode_id": 1} for symbol in symbols},
            },
        }
        for path, payload in documents.items():
            self._write_json(path, payload)

    def test_case_11_incomplete_stage_a_skips_without_modifying_technical_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            self._base_documents(paths)
            status = json.loads(paths["scan_status_path"].read_text(encoding="utf-8"))
            status["scan_status"] = "SCAN_INCOMPLETE"
            status["market_data_returned"] = 1
            self._write_json(paths["scan_status_path"], status)
            original = '{"schema_version":1,"assets":{"C0":{"live_source":"Gate"}}}\n'
            paths["technical_state_path"].write_text(original, encoding="utf-8")
            summary = run_stage_b(adapters_override=[], **paths)
            self.assertNotEqual(0, summary["exit_code"])
            self.assertEqual(original, paths["technical_state_path"].read_text(encoding="utf-8"))

    def test_case_12_bootstrap_creates_state_and_suppresses_notifications(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            self._base_documents(paths)
            summary = run_stage_b(bootstrap=True, adapters_override=[FakeAdapter("Fake")], **paths)
            state = json.loads(paths["technical_state_path"].read_text(encoding="utf-8"))
            events = json.loads(paths["events_path"].read_text(encoding="utf-8"))
            self.assertEqual(0, summary["exit_code"])
            self.assertEqual(2, len(state["assets"]))
            self.assertEqual(0, events["notification_count"])
            self.assertEqual("BOOTSTRAP_SUPPRESSED", events["status"])

    def test_case_13_live_and_history_sources_can_differ(self):
        config = {
            "allowed_quotes": ["USDT", "USD", "USDC"],
            "tie_break_order": ["High", "History"],
            "rsi_period": 14,
            "candle_target": 100,
            "volume_baseline_minimum": 7,
            "volume_baseline_target": 20,
        }
        high, history = FakeAdapter("High", 10_000, 10), FakeAdapter("History", 1_000, 100)
        markets = high.discover({"AAA"}, {"USDT"}) + history.discover({"AAA"}, {"USDT"})
        snapshot = {
            "current_price": 1,
            "price_change_percentage_1h": 1,
            "price_change_percentage_24h": 12,
            "range_24h_pct": 15,
            "turnover_ratio": 0.2,
        }
        record = enrich_asset(
            "AAA", snapshot, {"episode_id": 1}, markets,
            {"High": high, "History": history}, config, {}
        )
        self.assertEqual("High", record["live_source"])
        self.assertEqual("History", record["history_source"])

    def test_case_14_htx_and_upbit_can_never_be_formal_sources(self):
        config = json.loads(Path("config/exchange_priority.json").read_text(encoding="utf-8"))
        names = {adapter.name for adapter in build_adapters(config)}
        self.assertNotIn("HTX", names)
        self.assertNotIn("Upbit", names)
        bad = {
            "excluded_exchanges": ["HTX", "Upbit"],
            "allowed_exchanges": [{"name": "HTX", "adapter": "binance", "enabled": True}],
        }
        with self.assertRaises(ValueError):
            build_adapters(bad)

    def test_run_id_mismatch_skips_without_modifying_technical_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            self._base_documents(paths)
            snapshot = json.loads(paths["snapshot_path"].read_text(encoding="utf-8"))
            snapshot["run_id"] = "different_run"
            self._write_json(paths["snapshot_path"], snapshot)
            original = '{"schema_version":1,"assets":{"C0":{}}}\n'
            paths["technical_state_path"].write_text(original, encoding="utf-8")
            summary = run_stage_b(adapters_override=[], **paths)
            self.assertNotEqual(0, summary["exit_code"])
            self.assertIn("STAGE_INPUT_MISMATCH", summary["reason"])
            self.assertEqual(original, paths["technical_state_path"].read_text(encoding="utf-8"))

    def test_selective_baseline_suppresses_technical_events_and_prunes_stale_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._paths(root)
            self._base_documents(paths)
            radar = json.loads(paths["radar_state_path"].read_text(encoding="utf-8"))
            radar["assets"]["C0"].update(
                {"baseline_required": True, "coingecko_id": "new-c0"}
            )
            radar["assets"]["C1"].update(
                {"baseline_required": False, "coingecko_id": "c1"}
            )
            self._write_json(paths["radar_state_path"], radar)
            self._write_json(
                paths["technical_state_path"],
                {
                    "schema_version": 1,
                    "assets": {
                        "C0": {"active_technical_signals": ["RSI_4H_EXTREME_COLD"]},
                        "REMOVED": {"active_technical_signals": ["VOLUME_STRONG"]},
                    },
                },
            )

            summary = run_stage_b(adapters_override=[FakeAdapter("Fake")], **paths)
            state = json.loads(paths["technical_state_path"].read_text(encoding="utf-8"))
            events = json.loads(paths["events_path"].read_text(encoding="utf-8"))

            self.assertEqual(1, summary["assets_baselined"])
            self.assertFalse(any(item["symbol"] == "C0" for item in events["events"]))
            self.assertEqual("new-c0", state["assets"]["C0"]["coingecko_id"])
            self.assertNotIn("REMOVED", state["assets"])


if __name__ == "__main__":
    unittest.main()
