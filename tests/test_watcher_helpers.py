from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from app.storage import AppConfig, StatsStore
from app.tracker import SpectraWatchManager, StatsTracker


EU = "https://eu.valospectra.com:5200"


class FakeHenrik:
    def __init__(self) -> None:
        self.region_calls = 0
        self.match_calls = 0

    async def resolve_region(self, player_puuid: str) -> str:
        self.region_calls += 1
        return "eu"

    async def fetch_match(self, region: str, match_id: str):
        self.match_calls += 1
        return {
            "status": 200,
            "data": {
                "metadata": {"is_completed": True},
                "players": [{"name": "Player"}],
            },
        }


class WatcherHelperTests(unittest.TestCase):
    def test_extracts_first_player_puuid_from_player_id(self) -> None:
        value = SpectraWatchManager._first_player_puuid(
            {
                "teams": [
                    {"players": []},
                    {"players": [{"playerId": "puuid-123"}]},
                ]
            }
        )
        self.assertEqual(value, "puuid-123")

    def test_extracts_first_player_puuid_from_spectra_riot_id(self) -> None:
        value = SpectraWatchManager._first_player_puuid(
            {"teams": [{"players": [{"riotId": "puuid-spectra-456"}]}]}
        )
        self.assertEqual(value, "puuid-spectra-456")

    def test_player_id_takes_precedence_over_riot_id(self) -> None:
        value = SpectraWatchManager._first_player_puuid(
            {
                "teams": [
                    {
                        "players": [
                            {
                                "playerId": "puuid-new-field",
                                "riotId": "puuid-current-spectra-field",
                            }
                        ]
                    },
                ]
            }
        )
        self.assertEqual(value, "puuid-new-field")

    def test_missing_roster_returns_none(self) -> None:
        self.assertIsNone(SpectraWatchManager._first_player_puuid({"teams": []}))

    def test_tracker_key_includes_endpoint(self) -> None:
        eu = StatsTracker._key("a", "https://eu.valospectra.com:5200/")
        na = StatsTracker._key("A", "https://na.valospectra.com:5200")
        self.assertEqual(eu[0], "A")
        self.assertNotEqual(eu, na)

    def test_bomb_completion_candidates(self) -> None:
        def packet(left: int, right: int):
            return {
                "matchType": "bomb",
                "teams": [{"roundsWon": left}, {"roundsWon": right}],
            }

        self.assertFalse(SpectraWatchManager._match_may_be_complete(packet(12, 11)))
        self.assertTrue(SpectraWatchManager._match_may_be_complete(packet(13, 11)))
        # 13-12 is a candidate too. The fresh Spectra probe, not the score,
        # decides whether the group is actually finished.
        self.assertTrue(SpectraWatchManager._match_may_be_complete(packet(13, 12)))
        self.assertTrue(SpectraWatchManager._match_may_be_complete(packet(14, 12)))
        self.assertTrue(SpectraWatchManager._match_may_be_complete(packet(15, 14)))
        self.assertTrue(SpectraWatchManager._match_may_be_complete(packet(16, 14)))

    def test_swift_completion_candidate(self) -> None:
        packet = {
            "matchType": "swift",
            "teams": [{"roundsWon": 5}, {"roundsWon": 4}],
        }
        self.assertTrue(SpectraWatchManager._match_may_be_complete(packet))


class CompletionTriggeredFetchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.store = StatsStore(Path(self.temp.name) / "stats.sqlite3")
        self.henrik = FakeHenrik()
        self.config = AppConfig(
            allowed_spectra_endpoints=(EU,),
            post_match_delay_seconds=0,
            poll_interval_seconds=10,
            retry_error_seconds=15,
        )
        self.tracker = StatsTracker(self.store, self.henrik, self.config)

    async def asyncTearDown(self) -> None:
        tasks = list(self.tracker._fetch_tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.temp.cleanup()

    async def test_live_match_makes_zero_henrik_calls(self) -> None:
        self.tracker.observe_match("ABC", "match-1", EU, "puuid-1")
        await asyncio.sleep(0)
        self.assertEqual(self.store.get("ABC", EU)["state"], "live")
        self.assertEqual(self.henrik.region_calls, 0)
        self.assertEqual(self.henrik.match_calls, 0)

    async def test_confirmed_completion_fetches_once(self) -> None:
        self.tracker.observe_match("ABC", "match-1", EU, "puuid-1")
        self.store.mark_awaiting_end("ABC", EU, "match-1")
        self.tracker.mark_match_complete("ABC", EU, "match-1")

        task = self.tracker._fetch_tasks[self.tracker._key("ABC", EU)]
        await asyncio.wait_for(task, timeout=2)

        row = self.store.get("ABC", EU)
        self.assertEqual(row["state"], "ready")
        self.assertEqual(self.henrik.region_calls, 1)
        self.assertEqual(self.henrik.match_calls, 1)


if __name__ == "__main__":
    unittest.main()
