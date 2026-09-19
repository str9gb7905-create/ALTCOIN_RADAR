import unittest

from radar_scan import (
    build_top_movers,
    build_triggers,
    coverage_status,
    sanity_check,
    watchlist_stats,
)


def record(symbol, change_1h, change_24h):
    return {
        "symbol": symbol,
        "price_change_percentage_1h": change_1h,
        "price_change_percentage_24h": change_24h,
    }


class StageATests(unittest.TestCase):
    def test_dgb_24h_drop_triggers(self):
        self.assertEqual(["DGB"], [x["symbol"] for x in build_triggers([record("DGB", -2.3, -17.7)])])

    def test_btc_small_moves_do_not_trigger(self):
        self.assertEqual([], build_triggers([record("BTC", -1, -4)]))

    def test_positive_1h_threshold_triggers(self):
        result = build_triggers([record("AAA", 10.01, 0)])
        self.assertTrue(result[0]["trigger_1h_up"])

    def test_negative_1h_threshold_triggers(self):
        result = build_triggers([record("AAA", -10.01, 0)])
        self.assertTrue(result[0]["trigger_1h_down"])

    def test_sanity_check_fails_if_threshold_coin_missing_from_triggers(self):
        movers_source = [record("AAA", 1, 11)]
        top_movers = build_top_movers(movers_source)
        self.assertEqual("AAA", top_movers["gainers_24h"][0]["symbol"])
        self.assertEqual(["AAA"], sanity_check(movers_source, []))

    def test_incomplete_api_coverage(self):
        status, percentage = coverage_status(100, 99)
        self.assertEqual("SCAN_INCOMPLETE", status)
        self.assertEqual(99.0, percentage)

    def test_ambiguous_symbol_is_not_auto_mapped(self):
        watchlist = [
            {
                "symbol": "ID",
                "coingecko_id": None,
                "enabled": True,
                "needs_review": True,
            }
        ]
        stats = watchlist_stats(watchlist)
        self.assertEqual(0, stats["mapped_total"])
        self.assertEqual(1, stats["unmapped_total"])


if __name__ == "__main__":
    unittest.main()
