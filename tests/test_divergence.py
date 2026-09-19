import json
import math
import tempfile
import unittest
from pathlib import Path

from stage_b_divergence import (
    bearish_pair,
    bullish_pair,
    detect_timeframe,
    divergence_events,
    find_pivots,
    run_divergence,
    signal_record,
    signals_from_pivots,
    timeframe_state,
)
from stage_b_sources import Candle, PublicSpotAdapter, SpotMarket


PAIR_CONFIG = {
    "min_price_difference_pct": 1.0,
    "min_rsi_difference": 2.0,
    "min_pivot_separation_bars": 5,
}


def pivot(date, price, rsi, index):
    return {
        "timestamp": date,
        "price": price,
        "rsi": rsi,
        "confirmed": True,
        "bar_index": index,
    }


class DivergenceDefinitionTests(unittest.TestCase):
    def test_case_1_daily_bullish(self):
        lows = [pivot("2026-01-01", 100, 25, 10), pivot("2026-02-01", 90, 31, 20)]
        signals = signals_from_pivots("AAA", "DAILY", "1D", lows, [], PAIR_CONFIG)
        self.assertIn("DAILY_BULLISH", [item["type"] for item in signals])

    def test_case_2_lower_price_and_lower_rsi_is_not_bullish(self):
        lows = [pivot("2026-01-01", 100, 25, 10), pivot("2026-02-01", 90, 23, 20)]
        self.assertFalse(bullish_pair(lows[0], lows[1], PAIR_CONFIG))

    def test_case_3_daily_bearish(self):
        highs = [pivot("2026-01-01", 100, 75, 10), pivot("2026-02-01", 110, 68, 20)]
        signals = signals_from_pivots("AAA", "DAILY", "1D", [], highs, PAIR_CONFIG)
        self.assertIn("DAILY_BEARISH", [item["type"] for item in signals])

    def test_case_4_bullish_consecutive(self):
        lows = [
            pivot("2026-01-01", 100, 20, 10),
            pivot("2026-02-01", 90, 25, 20),
            pivot("2026-03-01", 80, 31, 30),
        ]
        signals = signals_from_pivots("AAA", "DAILY", "1D", lows, [], PAIR_CONFIG)
        self.assertIn("DAILY_BULLISH_CONSECUTIVE", [item["type"] for item in signals])

    def test_case_5_unconfirmed_last_pivot_is_pending_only(self):
        candles = [
            Candle(0, 10, 1, True, high=11, low=10),
            Candle(86_400_000, 9, 1, True, high=10, low=9),
            Candle(172_800_000, 8, 1, True, high=9, low=8),
        ]
        confirmed, pending = find_pivots(candles, [30, 32, 35], "low", 1, 1)
        self.assertEqual([], confirmed)
        self.assertEqual(1, len(pending))
        self.assertFalse(pending[0]["confirmed"])

    def test_case_6_price_difference_below_one_percent_is_filtered(self):
        first, second = pivot("2026-01-01", 100, 25, 10), pivot("2026-02-01", 99.7, 31, 20)
        self.assertFalse(bullish_pair(first, second, PAIR_CONFIG))

    def test_case_7_rsi_difference_below_two_points_is_filtered(self):
        first, second = pivot("2026-01-01", 100, 25, 10), pivot("2026-02-01", 90, 25.5, 20)
        self.assertFalse(bullish_pair(first, second, PAIR_CONFIG))

    def test_case_8_pivot_separation_too_close_is_filtered(self):
        first, second = pivot("2026-01-01", 100, 25, 10), pivot("2026-01-03", 90, 31, 12)
        self.assertFalse(bullish_pair(first, second, PAIR_CONFIG))

    def test_case_9_same_signature_does_not_repeat(self):
        pivots = [pivot("2026-01-01", 100, 25, 10), pivot("2026-02-01", 90, 31, 20)]
        signal = signal_record("AAA", "DAILY", "1D", "BULLISH", pivots)
        previous = timeframe_state(None, [signal])
        self.assertEqual([], divergence_events("AAA", "daily", previous, [signal]))

    def test_case_10_consecutive_after_regular_is_escalation(self):
        pivots = [
            pivot("2026-01-01", 100, 20, 10),
            pivot("2026-02-01", 90, 25, 20),
            pivot("2026-03-01", 80, 31, 30),
        ]
        regular = signal_record("AAA", "DAILY", "1D", "BULLISH", pivots[:2])
        consecutive = signal_record("AAA", "DAILY", "1D", "BULLISH", pivots, True)
        previous = timeframe_state(None, [regular])
        events = divergence_events("AAA", "daily", previous, [consecutive])
        self.assertEqual("DIVERGENCE_ESCALATION", events[0]["event"])

    def test_case_12_weekly_bullish(self):
        lows = [pivot("2026-01-05", 100, 25, 2), pivot("2026-03-02", 90, 31, 5)]
        config = {**PAIR_CONFIG, "min_pivot_separation_bars": 2}
        signals = signals_from_pivots("AAA", "WEEKLY", "1W", lows, [], config)
        self.assertIn("WEEKLY_BULLISH", [item["type"] for item in signals])

    def test_case_13_weekly_bearish(self):
        highs = [pivot("2026-01-05", 100, 75, 2), pivot("2026-03-02", 110, 68, 5)]
        config = {**PAIR_CONFIG, "min_pivot_separation_bars": 2}
        signals = signals_from_pivots("AAA", "WEEKLY", "1W", [], highs, config)
        self.assertIn("WEEKLY_BEARISH", [item["type"] for item in signals])

    def test_case_14_insufficient_history(self):
        config = {
            "timeframe": "1D",
            "pivot_left": 3,
            "pivot_right": 3,
            "min_pivot_separation_bars": 5,
            "min_price_difference_pct": 1.0,
            "min_rsi_difference": 2.0,
        }
        candles = [Candle(index * 86_400_000, index + 1, 1, True, high=index + 2, low=index) for index in range(10)]
        result = detect_timeframe("AAA", candles, "daily", config, 14)
        self.assertEqual("DATA_INSUFFICIENT", result["quality"])

    def test_case_16_unfinished_daily_candle_is_excluded(self):
        config = {
            "timeframe": "1D", "pivot_left": 1, "pivot_right": 1,
            "min_pivot_separation_bars": 1, "min_price_difference_pct": 1.0,
            "min_rsi_difference": 2.0,
        }
        candles = self._oscillating_candles(40, 86_400_000)
        partial = Candle(40 * 86_400_000, 1, 1, False, high=1000, low=-1000)
        before = detect_timeframe("AAA", candles, "daily", config, 14)
        after = detect_timeframe("AAA", candles + [partial], "daily", config, 14)
        self.assertEqual(before["signals"], after["signals"])

    def test_case_17_unfinished_weekly_candle_is_excluded(self):
        config = {
            "timeframe": "1W", "pivot_left": 1, "pivot_right": 1,
            "min_pivot_separation_bars": 1, "min_price_difference_pct": 1.0,
            "min_rsi_difference": 2.0,
        }
        candles = self._oscillating_candles(40, 604_800_000)
        partial = Candle(40 * 604_800_000, 1, 1, False, high=1000, low=-1000)
        before = detect_timeframe("AAA", candles, "weekly", config, 14)
        after = detect_timeframe("AAA", candles + [partial], "weekly", config, 14)
        self.assertEqual(before["signals"], after["signals"])

    @staticmethod
    def _oscillating_candles(count, step):
        result = []
        for index in range(count):
            close = 100 + math.sin(index / 2) * 10 + index * 0.1
            result.append(Candle(index * step, close, 100, True, high=close + 2, low=close - 2))
        return result


class FakeHistoryAdapter(PublicSpotAdapter):
    name = "Fake"

    def discover(self, target_symbols, quotes):
        return [SpotMarket(self.name, f"{symbol}USDT", symbol, "USDT", 1, 1000) for symbol in target_symbols]

    def candles(self, market, timeframe, limit=120):
        step = 86_400_000 if timeframe == "1d" else 604_800_000
        count = min(limit, 370 if timeframe == "1d" else 110)
        result = []
        for index in range(count):
            close = 100 + math.sin(index / 3) * 8 + index * 0.02
            result.append(Candle(index * step, close, 100, True, high=close + 2, low=close - 2))
        return result


class DivergenceWorkflowTests(unittest.TestCase):
    def _paths(self, root):
        return {
            "technical_config_path": root / "technical_config.json",
            "exchange_config_path": root / "exchange_config.json",
            "scan_status_path": root / "scan_status.json",
            "snapshot_path": root / "snapshot.json",
            "triggers_path": root / "triggers.json",
            "radar_state_path": root / "radar_state.json",
            "technical_state_path": root / "technical_state.json",
            "divergence_state_path": root / "divergence_state.json",
            "output_path": root / "latest_divergence.json",
            "events_path": root / "latest_divergence_events.json",
        }

    @staticmethod
    def _write(path, payload):
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _documents(self, paths, run_id="run_one"):
        tech_config = {
            "rsi_period": 14,
            "min_price_difference_pct": 1.0,
            "min_rsi_difference": 2.0,
            "daily": {"timeframe": "1D", "history_target_completed_candles": 365, "pivot_left": 3, "pivot_right": 3, "min_pivot_separation_bars": 5},
            "weekly": {"timeframe": "1W", "history_target_completed_candles": 104, "pivot_left": 2, "pivot_right": 2, "min_pivot_separation_bars": 2},
        }
        exchange_config = {"allowed_quotes": ["USDT"], "tie_break_order": ["Fake"], "excluded_exchanges": ["HTX", "Upbit"]}
        generated = "2026-09-17T00:00:00Z"
        docs = {
            paths["technical_config_path"]: tech_config,
            paths["exchange_config_path"]: exchange_config,
            paths["scan_status_path"]: {"run_id": run_id, "scan_status": "MARKET_DATA_COMPLETE", "market_data_returned": 2, "mapped_total": 2, "scan_finished_at": generated},
            paths["snapshot_path"]: {"run_id": run_id, "generated_at": generated, "count": 2, "items": []},
            paths["triggers_path"]: {"run_id": run_id, "generated_at": generated, "count": 2, "items": []},
            paths["radar_state_path"]: {"schema_version": 1, "last_run_id": run_id, "assets": {"AAA": {"active": True}, "BBB": {"active": True}}},
            paths["technical_state_path"]: {"schema_version": 1, "last_run_id": run_id, "assets": {"AAA": {"history_source": "Fake"}, "BBB": {"history_source": "Fake"}}},
        }
        for path, payload in docs.items():
            self._write(path, payload)

    def test_case_11_bootstrap_suppresses_existing_divergences(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = self._paths(Path(directory))
            self._documents(paths)
            summary = run_divergence(bootstrap=True, adapters_override=[FakeHistoryAdapter()], **paths)
            state = json.loads(paths["divergence_state_path"].read_text(encoding="utf-8"))
            events = json.loads(paths["events_path"].read_text(encoding="utf-8"))
            self.assertEqual(0, summary["exit_code"])
            self.assertEqual(2, len(state["assets"]))
            self.assertEqual(0, events["count"])
            self.assertEqual("BOOTSTRAP_SUPPRESSED", events["status"])

    def test_case_15_excluded_history_source_is_rejected(self):
        class HtxAdapter(FakeHistoryAdapter):
            name = "HTX"

        with tempfile.TemporaryDirectory() as directory:
            paths = self._paths(Path(directory))
            self._documents(paths)
            summary = run_divergence(bootstrap=True, adapters_override=[HtxAdapter()], **paths)
            self.assertNotEqual(0, summary["exit_code"])
            self.assertFalse(paths["divergence_state_path"].exists())

    def test_case_18_run_id_mismatch_does_not_modify_state(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = self._paths(Path(directory))
            self._documents(paths)
            snapshot = json.loads(paths["snapshot_path"].read_text(encoding="utf-8"))
            snapshot["run_id"] = "different_run"
            self._write(paths["snapshot_path"], snapshot)
            original = '{"schema_version":1,"assets":{"AAA":{}}}\n'
            paths["divergence_state_path"].write_text(original, encoding="utf-8")
            summary = run_divergence(adapters_override=[], **paths)
            self.assertNotEqual(0, summary["exit_code"])
            self.assertIn("STAGE_INPUT_MISMATCH", summary["reason"])
            self.assertEqual(original, paths["divergence_state_path"].read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

