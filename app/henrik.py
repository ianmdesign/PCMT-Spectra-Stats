from __future__ import annotations

import asyncio
import copy
import time
from typing import Any
from urllib.parse import quote

import aiohttp

VALID_REGIONS = {"eu", "na", "latam", "br", "ap", "kr"}


class HenrikError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, retry_after: int | None = None):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


class HenrikClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout_seconds: int = 15,
        max_requests_per_minute: int = 12,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.timeout_seconds = timeout_seconds
        self.max_requests_per_minute = max(1, int(max_requests_per_minute))
        # Henrik's documented limiter can count both the public API call and
        # Riot requests performed behind it. Pace our HTTP calls instead of
        # merely counting them so several simultaneous matches cannot burst a
        # basic key. 12 calls/min leaves headroom under a 30-unit/min key even
        # when a typical account or match lookup costs ~2 rate-limit units.
        self._request_interval = 60.0 / self.max_requests_per_minute
        self._rate_lock = asyncio.Lock()
        self._last_request_started = 0.0

    async def _wait_for_request_slot(self) -> None:
        async with self._rate_lock:
            now = time.monotonic()
            wait_for = self._request_interval - (now - self._last_request_started)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last_request_started = time.monotonic()

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    async def _get_json(self, path: str) -> dict[str, Any]:
        if not self.api_key:
            raise HenrikError("HENRIK_API_KEY is not configured")
        await self._wait_for_request_slot()
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        headers = {"Authorization": self.api_key, "Accept": "application/json"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(f"{self.base_url}/{path.lstrip('/')}") as response:
                retry_after = None
                if response.headers.get("Retry-After"):
                    try:
                        retry_after = int(response.headers["Retry-After"])
                    except ValueError:
                        retry_after = None
                try:
                    payload = await response.json(content_type=None)
                except Exception:
                    payload = None
                if response.status != 200:
                    detail = None
                    if isinstance(payload, dict):
                        errors = payload.get("errors")
                        if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                            detail = errors[0].get("message")
                    raise HenrikError(
                        detail or f"Henrik API returned HTTP {response.status}",
                        status=response.status,
                        retry_after=retry_after,
                    )
                if not isinstance(payload, dict):
                    raise HenrikError("Henrik API returned an invalid JSON payload", status=response.status)
                return payload

    async def resolve_region(self, player_puuid: str) -> str:
        payload = await self._get_json(
            f"valorant/v2/by-puuid/account/{quote(player_puuid, safe='')}"
        )
        data = payload.get("data")
        region = str(data.get("region", "")).lower() if isinstance(data, dict) else ""
        if region not in VALID_REGIONS:
            raise HenrikError(f"Unable to resolve a supported region for player {player_puuid}")
        return region

    async def fetch_match(self, region: str, match_id: str) -> dict[str, Any]:
        region = region.lower()
        if region not in VALID_REGIONS:
            raise HenrikError(f"Unsupported region: {region}")
        return await self._get_json(
            f"valorant/v4/match/{quote(region, safe='')}/{quote(match_id, safe='')}"
        )


def _canonical_team(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    lowered = value.lower()
    if lowered == "red":
        return "Red"
    if lowered == "blue":
        return "Blue"
    return value


def _normalize_abilities(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    return {
        "grenade": source.get("grenade"),
        "ability1": source.get("ability1", source.get("ability_1")),
        "ability2": source.get("ability2", source.get("ability_2")),
        "ultimate": source.get("ultimate"),
    }


def _normalize_round_player(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    value["team"] = _canonical_team(value.get("team"))
    return value


def normalize_match_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize Henrik v4 match data to the contract used by Spectra breakdowns.

    Spectra's StatsApiMapping mirrors Henrik v4 closely. This function keeps the
    upstream payload intact while normalizing the known naming/casing differences
    that have existed across v4 revisions.
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise HenrikError("Match response did not contain an object in data")

    result = copy.deepcopy(data)
    for key in ("players", "observers", "coaches", "teams", "rounds", "kills"):
        if not isinstance(result.get(key), list):
            result[key] = []

    for player in result["players"]:
        if not isinstance(player, dict):
            continue
        player["team_id"] = _canonical_team(player.get("team_id", player.get("team")))
        player["ability_casts"] = _normalize_abilities(player.get("ability_casts"))

    for team in result["teams"]:
        if isinstance(team, dict):
            team["team_id"] = _canonical_team(team.get("team_id", team.get("team")))

    for round_data in result["rounds"]:
        if not isinstance(round_data, dict):
            continue
        round_data["winning_team"] = _canonical_team(round_data.get("winning_team"))
        for event_key in ("plant", "defuse"):
            event = round_data.get(event_key)
            if isinstance(event, dict):
                event["player"] = _normalize_round_player(event.get("player"))
                for location in event.get("player_locations") or []:
                    _normalize_round_player(location)
        for stat in round_data.get("stats") or []:
            if not isinstance(stat, dict):
                continue
            stat["ability_casts"] = _normalize_abilities(stat.get("ability_casts"))
            stat["player"] = _normalize_round_player(stat.get("player"))
            for damage in stat.get("damage_events") or []:
                _normalize_round_player(damage)

    for kill in result["kills"]:
        if not isinstance(kill, dict):
            continue
        kill["killer"] = _normalize_round_player(kill.get("killer"))
        kill["victim"] = _normalize_round_player(kill.get("victim"))
        for assistant in kill.get("assistants") or []:
            _normalize_round_player(assistant)
        for location in kill.get("player_locations") or []:
            _normalize_round_player(location)

    return {"status": 200, "data": result}
