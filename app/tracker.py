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
        # Only maps whose completion has already been confirmed should contact
        # Henrik after a service restart. Live maps deliberately make zero
        # Henrik requests.
        for row in self.store.list_awaiting_stats():
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

        # Do not start Henrik polling here. We wait until Spectra confirms that
        # the match has disappeared from the active group.

    def mark_match_complete(
        self, group_code: str, spectra_endpoint: str, match_id: str
    ) -> None:
        if not self.store.mark_match_complete(group_code, spectra_endpoint, match_id):
            return
        row = self.store.get(group_code, spectra_endpoint)
        if row and row.get("matchId") == match_id and row.get("playerPuuid"):
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
        # Spectra's own backend waits 15 seconds after game_end before fetching
        # stats. Match that behavior so the normal path needs only one match API
        # request instead of polling throughout the map.
        await asyncio.sleep(self.config.post_match_delay_seconds)

        while True:
            row = self.store.get(group_code, spectra_endpoint)
            if (
                not row
                or row["matchId"] != match_id
                or row["state"] != "awaiting_stats"
            ):
                return
            puuid = row.get("playerPuuid")
            if not puuid:
                # There is no useful Henrik request we can make without a player
                # PUUID to resolve the region. Keep the durable state intact.
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

                # Henrik can occasionally index a match a little after Spectra
                # has ended it. Retry only in that post-match case.
                self.store.mark_attempt(group_code, spectra_endpoint, match_id, None)
            except HenrikError as exc:
                self.store.mark_attempt(
                    group_code, spectra_endpoint, match_id, str(exc)
                )
                if exc.retry_after:
                    delay = max(delay, exc.retry_after)
                elif exc.status in (403, 404, 429, 503, 408) or exc.status is None:
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

    Henrik is deliberately not used as a match-completion poller. We watch
    Spectra, and only after the active group disappears do we ask Henrik for the
    completed match.
    """

    PROBE_MATCH_TIMEOUT_SECONDS = 2.0
    PROBE_RETRY_SECONDS = 5.0

    def __init__(self, tracker: StatsTracker, config: AppConfig):
        self.tracker = tracker
        self.config = config
        self._watch_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._last_touch: dict[tuple[str, str], float] = {}
        self._completion_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._completion_match_ids: dict[tuple[str, str], str] = {}

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

    def resume_awaiting_end(self) -> None:
        """Resume Spectra-only completion probes after a Stats service restart."""
        for row in self.tracker.store.list_awaiting_end():
            endpoint = row["spectraEndpoint"]
            if not self.config.endpoint_allowed(endpoint):
                continue
            self._schedule_completion_probe(
                row["groupCode"], endpoint, row["matchId"]
            )

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

                        # Future Spectra versions may publish the final false
                        # transition. Current Spectra removes the Match object
                        # immediately at game_end, so also use the score-based
                        # candidate + definitive fresh-logon probe below.
                        if data.get("isRunning") is False or self._match_may_be_complete(data):
                            if self.tracker.store.mark_awaiting_end(
                                group_code, endpoint, match_id
                            ):
                                self._schedule_completion_probe(
                                    group_code, endpoint, match_id
                                )
                    except Exception:
                        # A malformed output packet should not terminate the watcher.
                        return

                try:
                    # Let python-socketio negotiate polling/WebSocket itself. This
                    # is more compatible with reverse proxies than forcing transport order.
                    await client.connect(endpoint)
                    while self._is_fresh(key) and client.connected:
                        await asyncio.sleep(5)
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


    def _schedule_completion_probe(
        self, group_code: str, endpoint: str, match_id: str
    ) -> None:
        key = self.tracker._key(group_code, endpoint)
        current = self._completion_tasks.get(key)
        current_match_id = self._completion_match_ids.get(key)
        if current and not current.done():
            if current_match_id == match_id:
                return
            current.cancel()

        self._completion_match_ids[key] = match_id
        self._completion_tasks[key] = asyncio.create_task(
            self._completion_probe_loop(group_code, endpoint, match_id),
            name=f"pcmt-stats-end-probe-{normalize_group_code(group_code)}-{abs(hash(normalize_endpoint(endpoint))) % 100000}",
        )

    async def _completion_probe_loop(
        self, group_code: str, endpoint: str, match_id: str
    ) -> None:
        key = self.tracker._key(group_code, endpoint)
        try:
            # Give Spectra a moment to process game_end/removeMatch after the
            # winning score packet.
            await asyncio.sleep(2)

            while True:
                row = self.tracker.store.get(group_code, endpoint)
                if (
                    not row
                    or row.get("matchId") != match_id
                    or row.get("state") != "awaiting_end"
                ):
                    return

                present = await self._probe_match_present(
                    group_code, endpoint, match_id
                )
                if present is False:
                    self.tracker.mark_match_complete(
                        group_code, endpoint, match_id
                    )
                    return

                # True means the same match is still active; None means the
                # endpoint/logon probe itself failed. Neither should spend a
                # Henrik request.
                await asyncio.sleep(self.PROBE_RETRY_SECONDS)
        finally:
            current = self._completion_tasks.get(key)
            if current is asyncio.current_task():
                self._completion_tasks.pop(key, None)
                self._completion_match_ids.pop(key, None)


    async def _probe_match_present(
        self, group_code: str, endpoint: str, match_id: str
    ) -> bool | None:
        """Return whether a fresh Spectra logon still has this exact match.

        Spectra always sends logon_success. It only sends the immediate
        match_data snapshot when MatchController still has the group. Therefore
        a successful logon followed by no snapshot is a definitive completion
        signal without consuming Henrik API quota.
        """
        if socketio is None:
            return None

        client = socketio.AsyncClient(
            reconnection=False,
            logger=False,
            engineio_logger=False,
        )
        logged_on = asyncio.Event()
        got_match = asyncio.Event()
        observed_match_id: str | None = None

        @client.event
        async def connect() -> None:
            await client.emit("logon", json.dumps({"groupCode": group_code}))

        @client.on("logon_success")
        async def logon_success(_: Any) -> None:
            logged_on.set()

        @client.on("match_data")
        async def match_data(raw: Any) -> None:
            nonlocal observed_match_id
            data = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(data, dict):
                return
            value = str(data.get("matchId") or "").strip()
            if not value:
                return
            observed_match_id = value
            got_match.set()

        try:
            await asyncio.wait_for(client.connect(endpoint), timeout=10)
            await asyncio.wait_for(logged_on.wait(), timeout=5)
            try:
                await asyncio.wait_for(
                    got_match.wait(), timeout=self.PROBE_MATCH_TIMEOUT_SECONDS
                )
            except TimeoutError:
                return False
            return observed_match_id == match_id
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        finally:
            if client.connected:
                await client.disconnect()

    @staticmethod
    def _match_may_be_complete(data: dict[str, Any]) -> bool:
        """Cheap completion candidate from the live Spectra score.

        This does not itself trigger Henrik. It only starts a fresh Spectra
        logon probe, which confirms that MatchController has actually removed
        the group before stats fetching begins.
        """
        teams = data.get("teams")
        if not isinstance(teams, list) or len(teams) < 2:
            return False

        scores: list[int] = []
        for team in teams[:2]:
            if not isinstance(team, dict):
                return False
            try:
                scores.append(int(team.get("roundsWon", 0)))
            except (TypeError, ValueError):
                return False

        high = max(scores)
        match_type = str(data.get("matchType") or "").strip().lower()

        # This is intentionally only a CANDIDATE, not a completion decision.
        # A fresh Spectra logon must subsequently prove that the group has been
        # removed. Starting at 13 also covers 13-12, overtime, draws, and custom
        # overtime settings without ever spending a Henrik request too early.
        if match_type == "swift" or data.get("switchRound") == 5:
            return high >= 5
        return high >= 13

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
