import unittest

from import_cmc_watchlist import (
    IGNORE_DUPLICATE,
    NEEDS_REVIEW,
    NEW_ASSET,
    merge_cmc_assets,
)


class CmcWatchlistImportTests(unittest.TestCase):
    def setUp(self):
        self.existing = [
            {
                "symbol": "MANA",
                "name": "Decentraland",
                "coingecko_id": "decentraland",
                "canonical_asset": "decentraland",
                "cmc_id": 1966,
                "enabled": True,
            },
            {
                "symbol": "S",
                "name": "Sonic",
                "coingecko_id": "sonic-3",
                "enabled": True,
            },
        ]

    def test_existing_canonical_asset_is_ignored(self):
        merged, result = merge_cmc_assets(
            self.existing,
            [{"symbol": "MANA", "canonical_asset": "DECENTRALAND"}],
        )
        self.assertEqual(IGNORE_DUPLICATE, result[0]["status"])
        self.assertEqual(len(self.existing), len(merged))

    def test_same_coingecko_id_is_ignored_even_with_different_symbol(self):
        merged, result = merge_cmc_assets(
            self.existing,
            [{"symbol": "OTHER", "coingecko_id": "decentraland"}],
        )
        self.assertEqual(IGNORE_DUPLICATE, result[0]["status"])
        self.assertEqual(len(self.existing), len(merged))

    def test_same_cmc_id_is_ignored(self):
        merged, result = merge_cmc_assets(
            self.existing,
            [{"symbol": "mana", "cmc_id": 1966}],
        )
        self.assertEqual(IGNORE_DUPLICATE, result[0]["status"])
        self.assertEqual(len(self.existing), len(merged))

    def test_symbol_case_difference_for_same_asset_is_ignored(self):
        merged, result = merge_cmc_assets(
            self.existing,
            [{"symbol": "mana", "name": "decentraland"}],
        )
        self.assertEqual(IGNORE_DUPLICATE, result[0]["status"])
        self.assertEqual(len(self.existing), len(merged))

    def test_new_asset_is_appended(self):
        merged, result = merge_cmc_assets(
            self.existing,
            [{"symbol": "PIXEL", "name": "Pixels", "coingecko_id": "pixels"}],
        )
        self.assertEqual(NEW_ASSET, result[0]["status"])
        self.assertEqual(len(self.existing) + 1, len(merged))
        self.assertEqual("PIXEL", merged[-1]["symbol"])

    def test_same_symbol_different_asset_needs_review_without_overwrite(self):
        merged, result = merge_cmc_assets(
            self.existing,
            [{"symbol": "S", "name": "Another S", "coingecko_id": "agent-s"}],
        )
        self.assertEqual(NEEDS_REVIEW, result[0]["status"])
        self.assertEqual(self.existing, merged)

    def test_reimport_does_not_duplicate_or_delete(self):
        incoming = [
            {"symbol": "AI", "name": "Sleepless AI", "coingecko_id": "sleepless-ai"},
            {"symbol": "ai", "name": "Sleepless AI", "coingecko_id": "sleepless-ai"},
        ]
        merged, result = merge_cmc_assets(self.existing, incoming)
        self.assertEqual([NEW_ASSET, IGNORE_DUPLICATE], [item["status"] for item in result])
        self.assertEqual(len(self.existing) + 1, len(merged))
        self.assertTrue(all(item in merged for item in self.existing))


if __name__ == "__main__":
    unittest.main()
