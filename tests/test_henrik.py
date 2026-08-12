from __future__ import annotations

import unittest

from app.henrik import HenrikClient, HenrikError, normalize_match_payload


class HenrikNormalizationTests(unittest.TestCase):

    def test_basic_key_default_paces_requests_conservatively(self) -> None:
        client = HenrikClient("https://api.henrikdev.xyz", "test", max_requests_per_minute=12)
        self.assertEqual(client.max_requests_per_minute, 12)
        self.assertEqual(client._request_interval, 5.0)

    def test_normalizes_ability_aliases(self) -> None:
        payload = {
            "data": {
                "metadata": {"is_completed": True},
                "players": [{
                    "name": "Player",
                    "tag": "NA1",
                    "team_id": "red",
                    "ability_casts": {"grenade": 1, "ability_1": 2, "ability_2": 3, "ultimate": 4},
                }],
                "teams": [], "rounds": [], "kills": [], "observers": [], "coaches": []
            }
        }
        result = normalize_match_payload(payload)
        player = result["data"]["players"][0]
        self.assertEqual(player["team_id"], "Red")
        self.assertEqual(player["ability_casts"]["ability1"], 2)
        self.assertEqual(player["ability_casts"]["ability2"], 3)

    def test_normalizes_round_and_kill_team_casing(self) -> None:
        payload = {
            "data": {
                "metadata": {"is_completed": True}, "players": [], "observers": [], "coaches": [],
                "teams": [{"team_id": "blue"}],
                "rounds": [{
                    "winning_team": "red",
                    "plant": {"player": {"team": "blue"}, "player_locations": [{"team": "red"}]},
                    "defuse": None,
                    "stats": [{"ability_casts": {}, "player": {"team": "blue"}, "damage_events": [{"team": "red"}]}],
                }],
                "kills": [{
                    "killer": {"team": "red"}, "victim": {"team": "blue"},
                    "assistants": [{"team": "red"}], "player_locations": [{"team": "blue"}]
                }]
            }
        }
        data = normalize_match_payload(payload)["data"]
        self.assertEqual(data["teams"][0]["team_id"], "Blue")
        self.assertEqual(data["rounds"][0]["winning_team"], "Red")
        self.assertEqual(data["kills"][0]["victim"]["team"], "Blue")

    def test_adds_missing_arrays(self) -> None:
        result = normalize_match_payload({"data": {"metadata": {"is_completed": True}}})
        for key in ("players", "observers", "coaches", "teams", "rounds", "kills"):
            self.assertEqual(result["data"][key], [])

    def test_rejects_invalid_match_payload(self) -> None:
        with self.assertRaises(HenrikError):
            normalize_match_payload({"status": 200, "data": None})


if __name__ == "__main__":
    unittest.main()
