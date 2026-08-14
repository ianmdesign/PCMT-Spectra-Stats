from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.storage import STATS_LIFETIME_HOURS, StatsStore


EU = "https://eu.valospectra.com:5200"
NA = "https://na.valospectra.com:5200"


class StatsStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "stats.sqlite3"
        self.store = StatsStore(self.db_path)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_new_match_is_live_and_makes_no_fetch_state(self) -> None:
        row, created = self.store.observe_match("abc", "match-1", EU, "p1", 1000)
        self.assertTrue(created)
        self.assertEqual(row["groupCode"], "ABC")
        self.assertEqual(row["spectraEndpoint"], EU)
        self.assertEqual(row["state"], "live")
        self.assertIsNone(row["stats"])

    def test_completion_state_progression(self) -> None:
        self.store.observe_match("ABC", "match-1", EU, "p1", 1000)
        self.assertTrue(self.store.mark_awaiting_end("ABC", EU, "match-1"))
        self.assertEqual(self.store.get("ABC", EU)["state"], "awaiting_end")
        self.assertTrue(self.store.mark_match_complete("ABC", EU, "match-1"))
        self.assertEqual(self.store.get("ABC", EU)["state"], "awaiting_stats")
        self.assertEqual(len(self.store.list_awaiting_stats()), 1)

    def test_same_match_is_idempotent_per_server(self) -> None:
        self.store.observe_match("ABC", "match-1", EU, "p1", 1000)
        _, created = self.store.observe_match("abc", "match-1", EU + "/", "p1", 2000)
        self.assertFalse(created)

    def test_same_match_can_fill_missing_puuid(self) -> None:
        self.store.observe_match("ABC", "match-1", EU, None, 1000)
        row, _ = self.store.observe_match("ABC", "match-1", EU, "later", 2000)
        self.assertEqual(row["playerPuuid"], "later")

    def test_legacy_pending_becomes_live_on_next_spectra_packet(self) -> None:
        self.store.observe_match("ABC", "match-1", EU, "p1", 1000)
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "UPDATE group_stats SET state = 'pending' WHERE spectra_endpoint = ? AND group_code = 'ABC'",
                (EU,),
            )
            db.commit()
        row, created = self.store.observe_match("ABC", "match-1", EU, "p1", 2000)
        self.assertFalse(created)
        self.assertEqual(row["state"], "live")

    def test_same_group_code_on_different_servers_is_independent(self) -> None:
        eu_row, eu_created = self.store.observe_match("A", "eu-match", EU, "eu-p", 1000)
        na_row, na_created = self.store.observe_match("A", "na-match", NA, "na-p", 1100)
        self.assertTrue(eu_created)
        self.assertTrue(na_created)
        self.assertEqual(eu_row["matchId"], "eu-match")
        self.assertEqual(na_row["matchId"], "na-match")
        self.assertEqual(self.store.get("A", EU)["matchId"], "eu-match")
        self.assertEqual(self.store.get("A", NA)["matchId"], "na-match")
        self.assertEqual(len(self.store.list_for_group("A")), 2)

    def test_new_match_only_clears_previous_stats_on_same_server(self) -> None:
        payload_eu = {"status": 200, "data": {"players": [{"name": "EU old"}]}}
        payload_na = {"status": 200, "data": {"players": [{"name": "NA current"}]}}
        self.store.observe_match("ABC", "eu-1", EU, "p1", 1000)
        self.store.observe_match("ABC", "na-1", NA, "p2", 1000)
        self.store.save_stats("ABC", EU, "eu-1", payload_eu, 2000)
        self.store.save_stats("ABC", NA, "na-1", payload_na, 2000)

        self.store.observe_match("ABC", "eu-2", EU, "p3", 3000)

        self.assertIsNone(self.store.get_published_stats("ABC", EU, 3001))
        self.assertEqual(self.store.get_published_stats("ABC", NA, 3001), payload_na)
        self.assertEqual(self.store.get("ABC", EU)["matchId"], "eu-2")
        self.assertEqual(self.store.get("ABC", NA)["matchId"], "na-1")

    def test_stats_expire_exactly_after_24_hours_per_server(self) -> None:
        self.assertEqual(STATS_LIFETIME_HOURS, 24)
        stored_at = 10_000
        payload = {"status": 200, "data": {"players": [{}]}}
        self.store.observe_match("ABC", "match-1", EU, "p1", 1000)
        self.store.save_stats("ABC", EU, "match-1", payload, stored_at)
        expires = stored_at + 24 * 60 * 60 * 1000
        self.assertIsNotNone(self.store.get_published_stats("ABC", EU, expires - 1))
        self.assertIsNone(self.store.get_published_stats("ABC", EU, expires))

    def test_old_worker_cannot_store_into_reused_server_group(self) -> None:
        self.store.observe_match("ABC", "match-1", EU, "p1", 1000)
        self.store.observe_match("ABC", "match-2", EU, "p2", 2000)
        saved = self.store.save_stats(
            "ABC", EU, "match-1", {"status": 200, "data": {"players": [{}]}}, 3000
        )
        self.assertFalse(saved)
        self.assertEqual(self.store.get("ABC", EU)["matchId"], "match-2")

    def test_worker_on_one_server_cannot_write_other_server(self) -> None:
        self.store.observe_match("ABC", "same-id", EU, "p1", 1000)
        self.store.observe_match("ABC", "same-id", NA, "p2", 1000)
        payload = {"status": 200, "data": {"players": [{"name": "EU"}]}}
        self.assertTrue(self.store.save_stats("ABC", EU, "same-id", payload, 3000))
        self.assertEqual(self.store.get_published_stats("ABC", EU, 3001), payload)
        self.assertIsNone(self.store.get_published_stats("ABC", NA, 3001))

    def test_region_update_is_server_and_match_scoped(self) -> None:
        self.store.observe_match("ABC", "match-2", EU, "p2", 2000)
        self.store.observe_match("ABC", "match-2", NA, "p3", 2000)
        self.assertFalse(self.store.set_region("ABC", EU, "match-1", "na"))
        self.assertTrue(self.store.set_region("ABC", EU, "match-2", "eu"))
        self.assertEqual(self.store.get("ABC", EU)["region"], "eu")
        self.assertIsNone(self.store.get("ABC", NA)["region"])

    def test_region_cache_is_keyed_by_player_puuid(self) -> None:
        self.assertIsNone(self.store.get_cached_region("player-1"))
        self.store.cache_region("player-1", "NA")
        self.store.cache_region("player-2", "eu")
        self.assertEqual(self.store.get_cached_region("player-1"), "na")
        self.assertEqual(self.store.get_cached_region("player-2"), "eu")

    def test_v01_database_migrates_to_composite_key(self) -> None:
        legacy_path = Path(self.temp.name) / "legacy.sqlite3"
        with sqlite3.connect(legacy_path) as db:
            db.execute(
                """
                CREATE TABLE group_stats (
                    group_code TEXT PRIMARY KEY COLLATE NOCASE,
                    match_id TEXT NOT NULL,
                    spectra_endpoint TEXT NOT NULL,
                    player_puuid TEXT,
                    region TEXT,
                    state TEXT NOT NULL,
                    stats_json TEXT,
                    tracked_at INTEGER NOT NULL,
                    stored_at INTEGER,
                    expires_at INTEGER,
                    last_attempt_at INTEGER,
                    last_error TEXT
                )
                """
            )
            db.execute(
                """
                INSERT INTO group_stats (
                    group_code, match_id, spectra_endpoint, player_puuid, region,
                    state, stats_json, tracked_at, stored_at, expires_at,
                    last_attempt_at, last_error
                ) VALUES ('ABC', 'legacy-match', ?, 'p1', 'eu', 'pending', NULL, 1000, NULL, NULL, NULL, NULL)
                """,
                (EU,),
            )
            db.commit()

        migrated = StatsStore(legacy_path)
        self.assertEqual(migrated.get("ABC", EU)["matchId"], "legacy-match")
        migrated.observe_match("ABC", "na-match", NA, "p2", 2000)
        self.assertEqual(len(migrated.list_for_group("ABC")), 2)


if __name__ == "__main__":
    unittest.main()
