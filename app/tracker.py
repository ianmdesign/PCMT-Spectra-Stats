from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from .henrik import HenrikClient, HenrikError, normalize_match_payload
from .storage import AppConfig, StatsStore, normalize_endpoint, normalize_group_code

try:
    import socketio  # type: ignore
except ImportError:  # pragma: no cover - dependency is installed in Docker/CI
    socketio = None


class StatsTracker:
    def __init__(self, store: StatsStore, henrik: HenrikClient, config: AppConfig):
        self.store = store
        self.henrik = henrik
        self.config = config
        self._fetch_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}

    @staticmethod
    def _key(group_code: str, spectra_endpoint: str) -> tuple[str, str]:
        return (normalize_group_code(group_code), normalize_endpoint(spectra_endpoint))

    def resume_pending(self) -> None:
        for row in self.store.list_pending():
            if row.get("playerPuuid"):
                self.ensure_fetch(row["groupCode"], row["spectraEndpoint"], row["matchId"])

    def observe_match(
        self,
        group_code: str,
        match_id: str,
        spectra_endpoint: str,
        player_puuid: str | None,
    ) -> None:
        row, is_new = self.store.observe_match(
            group_code, match_id, spectra_endpoint, player_puuid
        )
        group_code = row["groupCode"]
        spectra_endpoint = row["spectraEndpoint"]
        key = self._key(group_code, spectra_endpoint)
        if is_new:
            old = self._fetch_tasks.pop(key, None)
            if old and not old.done():
                old.cancel()
        if row.get("playerPuuid"):
            self.ensure_fetch(group_code, spectra_endpoint, match_id)

    def ensure_fetch(self, group_code: str, spectra_endpoint: str, match_id: str) -> None:
        group_code = normalize_group_code(group_code)
        spectra_endpoint = normalize_endpoint(spectra_endpoint)
        key = self._key(group_code, spectra_endpoint)
        current = self._fetch_tasks.get(key)
        if current and not current.done():
            return
        self._fetch_tasks[key] = asyncio.create_task(
            self._fetch_until_complete(group_code, spectra_endpoint, match_id),
            name=f"pcmt-stats-fetch-{group_code}-{abs(hash(spectra_endpoint)) % 100000}",
        )

    async def _fetch_until_complete(
        self, group_code: str, spectra_endpoint: str, match_id: str
    ) -> None:
        while True:
            row = self.store.get(group_code, spectra_endpoint)
            if not row or row["matchId"] != match_id or row["state"] != "pending":
                return
            puuid = row.get("playerPuuid")
            if not puuid:
                await asyncio.sleep(self.config.retry_error_seconds)
                continue

            delay = self.config.poll_interval_seconds
            try:
                region = row.get("region")
                if not region:
                    region = self.store.get_cached_region(puuid)
                if not region:
                    region = await self.henrik.resolve_region(puuid)
                    self.store.cache_region(puuid, region)
                if row.get("region") != region:
                    if not self.store.set_region(
                        group_code, spectra_endpoint, match_id, region
                    ):
                        return

                raw = await self.henrik.fetch_match(region, match_id)
                data = raw.get("data") if isinstance(raw, dict) else None
                metadata = data.get("metadata") if isinstance(data, dict) else None
                is_completed = (
                    bool(metadata.get("is_completed")) if isinstance(metadata, dict) else False
                )
                players = data.get("players") if isinstance(data, dict) else None

                if is_completed and isinstance(players, list) and players:
                    normalized = normalize_match_payload(raw)
                    self.store.save_stats(
                        group_code, spectra_endpoint, match_id, normalized
                    )
                    return

                self.store.mark_attempt(group_code, spectra_endpoint, match_id, None)
            except HenrikError as exc:
                self.store.mark_attempt(
                    group_code, spectra_endpoint, match_id, str(exc)
                )
                if exc.retry_after:
                    delay = max(delay, exc.retry_after)
                elif exc.status in (403, 429, 503, 408) or exc.status is None:
                    delay = max(delay, self.config.retry_error_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Keep transient failures from losing the map.
                self.store.mark_attempt(
                    group_code, spectra_endpoint, match_id, str(exc)
                )
                delay = max(delay, self.config.retry_error_seconds)

            await asyncio.sleep(delay)


class SpectraWatchManager:
    """Keep bounded, short-lived Socket.IO subscriptions to Spectra output.

    The browser renews a watch once per minute. A watch that is no longer renewed
    automatically disconnects, preventing stale OBS/browser sessions (or arbitrary
    public requests) from accumulating permanent server-side connections.
    """

    def __init__(self, tracker: StatsTracker, config: AppConfig):
        self.tracker = tracker
        self.config = config
        self._watch_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._last_touch: dict[tuple[str, str], float] = {}

    def ensure_watch(self, group_code: str, endpoint: str) -> bool:
        group_code = normalize_group_code(group_code)
        endpoint = normalize_endpoint(endpoint)
        if not group_code:
            raise ValueError("groupCode is required")
        if not endpoint:
            raise ValueError("spectraEndpoint is required")
        if not self.config.endpoint_allowed(endpoint):
            raise ValueError("Spectra endpoint is not allowed by config")

        key = (group_code, endpoint)
        self._last_touch[key] = time.monotonic()
        current = self._watch_tasks.get(key)
        if current and not current.done():
            return False

        if self.active_count >= self.config.max_active_watches:
            self._last_touch.pop(key, None)
            raise ValueError("Maximum active Spectra watches reached")

        task = asyncio.create_task(
            self._watch_loop(key),
            name=f"pcmt-stats-watch-{group_code}-{abs(hash(endpoint)) % 100000}",
        )
        self._watch_tasks[key] = task
        return True

    @property
    def active_count(self) -> int:
        return sum(1 for task in self._watch_tasks.values() if not task.done())

    def _is_fresh(self, key: tuple[str, str]) -> bool:
        touched = self._last_touch.get(key)
        return touched is not None and time.monotonic() - touched <= self.config.watch_ttl_seconds

    async def _watch_loop(self, key: tuple[str, str]) -> None:
        if socketio is None:
            raise RuntimeError("python-socketio is required for Spectra watching")

        group_code, endpoint = key
        try:
            while self._is_fresh(key):
                client = socketio.AsyncClient(
                    reconnection=True,
                    reconnection_attempts=0,
                    reconnection_delay=2,
                    reconnection_delay_max=15,
                    logger=False,
                    engineio_logger=False,
                )

                @client.event
                async def connect() -> None:
                    await client.emit("logon", json.dumps({"groupCode": group_code}))

                @client.on("match_data")
                async def match_data(raw: Any) -> None:
                    try:
                        data = json.loads(raw) if isinstance(raw, str) else raw
                        if not isinstance(data, dict):
                            return
                        match_id = str(data.get("matchId") or "").strip()
                        if not match_id:
                            return
                        puuid = self._first_player_puuid(data)
                        self.tracker.observe_match(group_code, match_id, endpoint, puuid)
                    except Exception:
                        # A malformed output packet should not terminate the watcher.
                        return

                try:
                    # Let python-socketio negotiate polling/WebSocket itself. This
                    # is more compatible with reverse proxies than forcing transport order.
                    await client.connect(endpoint)
                    while self._is_fresh(key) and client.connected:
                        await asyncio.sleep(10)
                except asyncio.CancelledError:
                    if client.connected:
                        await client.disconnect()
                    raise
                except Exception:
                    await asyncio.sleep(5)
                finally:
                    if client.connected:
                        await client.disconnect()
        finally:
            self._last_touch.pop(key, None)
            current = self._watch_tasks.get(key)
            if current is asyncio.current_task():
                self._watch_tasks.pop(key, None)

    @staticmethod
    def _first_player_puuid(data: dict[str, Any]) -> str | None:
        for team in data.get("teams") or []:
            if not isinstance(team, dict):
                continue
            for player in team.get("players") or []:
                if not isinstance(player, dict):
                    continue

                # Spectra Server currently serializes the Player object's
                # internal `riotId` property. Keep `playerId` first for
                # compatibility with any payload/version that exposes the
                # original roster field name directly.
                value = str(
                    player.get("playerId")
                    or player.get("riotId")
                    or ""
                ).strip()
                if value:
                    return value
        return None
