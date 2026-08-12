from __future__ import annotations

import unittest

from app.tracker import SpectraWatchManager, StatsTracker


class WatcherHelperTests(unittest.TestCase):
    def test_extracts_first_player_puuid(self) -> None:
        value = SpectraWatchManager._first_player_puuid(
            {
                "teams": [
                    {"players": []},
                    {"players": [{"playerId": "puuid-123"}]},
                ]
            }
        )
        self.assertEqual(value, "puuid-123")

    def test_missing_roster_returns_none(self) -> None:
        self.assertIsNone(SpectraWatchManager._first_player_puuid({"teams": []}))

    def test_tracker_key_includes_endpoint(self) -> None:
        eu = StatsTracker._key("a", "https://eu.valospectra.com:5200/")
        na = StatsTracker._key("A", "https://na.valospectra.com:5200")
        self.assertEqual(eu[0], "A")
        self.assertNotEqual(eu, na)


if __name__ == "__main__":
    unittest.main()
