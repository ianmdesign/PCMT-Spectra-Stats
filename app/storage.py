from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONFIG_DIR = Path(os.environ.get("CONFIG_DIR", Path(__file__).resolve().parent.parent / "config"))
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
CONFIG_PATH = CONFIG_DIR / "config.json"
DB_PATH = DATA_DIR / "stats.sqlite3"
STATS_LIFETIME_HOURS = 24


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_group_code(value: str) -> str:
    return (value or "").strip().upper()


def normalize_endpoint(value: str) -> str:
    return (value or "").strip().rstrip("/")


@dataclass(frozen=True)
class AppConfig:
    allowed_spectra_endpoints: tuple[str, ...] = ("http://localhost:5200",)
    henrik_api_base_url: str = "https://api.henrikdev.xyz"
    poll_interval_seconds: int = 45
    retry_error_seconds: int = 60
    request_timeout_seconds: int = 15
    watch_ttl_seconds: int = 300
    max_active_watches: int = 50
    henrik_max_requests_per_minute: int = 12
    post_match_delay_seconds: int = 15

    @classmethod
    def load(cls) -> "AppConfig":
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        raw: dict[str, Any] = {}
        if CONFIG_PATH.exists():
            raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

        endpoints_raw = raw.get("allowedSpectraEndpoints", list(cls.allowed_spectra_endpoints))
        if not isinstance(endpoints_raw, list):
            raise ValueError("allowedSpectraEndpoints must be an array")
        endpoints = tuple(
            endpoint
            for endpoint in (normalize_endpoint(str(value)) for value in endpoints_raw)
            if endpoint
        )
        return cls(
            allowed_spectra_endpoints=endpoints,
            henrik_api_base_url=normalize_endpoint(
                str(raw.get("henrikApiBaseUrl", cls.henrik_api_base_url))
            ),
            poll_interval_seconds=max(
                10, int(raw.get("pollIntervalSeconds", cls.poll_interval_seconds))
            ),
            retry_error_seconds=max(
                15, int(raw.get("retryErrorSeconds", cls.retry_error_seconds))
            ),
            request_timeout_seconds=max(
                5, int(raw.get("requestTimeoutSeconds", cls.request_timeout_seconds))
            ),
            watch_ttl_seconds=max(60, int(raw.get("watchTtlSeconds", cls.watch_ttl_seconds))),
            max_active_watches=max(1, int(raw.get("maxActiveWatches", cls.max_active_watches))),
            henrik_max_requests_per_minute=min(
                30,
                max(
                    1,
                    int(
                        raw.get(
                            "henrikMaxRequestsPerMinute",
                            cls.henrik_max_requests_per_minute,
                        )
                    ),
                ),
            ),
            post_match_delay_seconds=max(
                5, int(raw.get("postMatchDelaySeconds", cls.post_match_delay_seconds))
            ),
        )

    @property
    def henrik_api_key(self) -> str:
        return os.environ.get("HENRIK_API_KEY", "").strip()

    def endpoint_allowed(self, endpoint: str) -> bool:
        return normalize_endpoint(endpoint) in self.allowed_spectra_endpoints


class StatsStore:
    """Persistent stats state keyed by Spectra endpoint + group code.

    A group code is only unique inside a particular Spectra server. Keeping the
    endpoint in the durable key allows one PCMT Stats instance to safely serve
    NA, EU and local Spectra servers even when they all use the same group code.
    """

    def __init__(self, path: Path = DB_PATH):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _create_group_stats_table(db: sqlite3.Connection, table_name: str = "group_stats") -> None:
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                group_code TEXT NOT NULL COLLATE NOCASE,
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
                last_error TEXT,
                PRIMARY KEY (spectra_endpoint, group_code)
            )
            """
        )

    @staticmethod
    def _group_stats_primary_key(db: sqlite3.Connection) -> list[str]:
        rows = db.execute("PRAGMA table_info(group_stats)").fetchall()
        keyed = sorted(
            ((int(row[5]), str(row[1])) for row in rows if int(row[5]) > 0),
            key=lambda item: item[0],
        )
        return [name for _, name in keyed]

    def _migrate_group_stats_if_needed(self, db: sqlite3.Connection) -> None:
        exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'group_stats'"
        ).fetchone()
        if not exists:
            self._create_group_stats_table(db)
            return

        if self._group_stats_primary_key(db) == ["spectra_endpoint", "group_code"]:
            return

        # v0.1.x keyed rows only by group_code. Preserve every existing row while
        # upgrading to the server-scoped key. Existing deployments therefore do
        # not need to delete their SQLite database during this update.
        db.execute("DROP TABLE IF EXISTS group_stats_v2")
        self._create_group_stats_table(db, "group_stats_v2")
        db.execute(
            """
            INSERT OR REPLACE INTO group_stats_v2 (
                group_code, match_id, spectra_endpoint, player_puuid, region,
                state, stats_json, tracked_at, stored_at, expires_at,
                last_attempt_at, last_error
            )
            SELECT
                group_code, match_id, spectra_endpoint, player_puuid, region,
                state, stats_json, tracked_at, stored_at, expires_at,
                last_attempt_at, last_error
            FROM group_stats
            """
        )
        db.execute("DROP TABLE group_stats")
        db.execute("ALTER TABLE group_stats_v2 RENAME TO group_stats")

    def _init_db(self) -> None:
        with self._lock, self._connect() as db:
            self._migrate_group_stats_if_needed(db)
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_group_stats_state ON group_stats(state, tracked_at)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_group_stats_group_code ON group_stats(group_code)"
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS region_cache (
                    player_puuid TEXT PRIMARY KEY,
                    region TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            db.commit()

    def observe_match(
        self,
        group_code: str,
        match_id: str,
        spectra_endpoint: str,
        player_puuid: str | None = None,
        timestamp_ms: int | None = None,
    ) -> tuple[dict[str, Any], bool]:
        group_code = normalize_group_code(group_code)
        match_id = (match_id or "").strip()
        spectra_endpoint = normalize_endpoint(spectra_endpoint)
        player_puuid = (player_puuid or "").strip() or None
        if not group_code:
            raise ValueError("groupCode is required")
        if not match_id:
            raise ValueError("matchId is required")
        if not spectra_endpoint:
            raise ValueError("spectraEndpoint is required")

        timestamp_ms = timestamp_ms if timestamp_ms is not None else now_ms()
        with self._lock, self._connect() as db:
            existing = db.execute(
                """
                SELECT * FROM group_stats
                WHERE spectra_endpoint = ? AND group_code = ?
                """,
                (spectra_endpoint, group_code),
            ).fetchone()
            if existing and existing["match_id"] == match_id:
                updates: list[str] = []
                params: list[Any] = []

                if player_puuid and not existing["player_puuid"]:
                    updates.append("player_puuid = ?")
                    params.append(player_puuid)

                # v0.2.x used "pending" for a live match and immediately started
                # Henrik polling. Upgrade that legacy state the next time Spectra
                # sends a packet, without disturbing newer completion states.
                if existing["state"] == "pending":
                    updates.append("state = 'live'")

                if updates:
                    params.extend([spectra_endpoint, group_code])
                    db.execute(
                        f"""
                        UPDATE group_stats SET {", ".join(updates)}
                        WHERE spectra_endpoint = ? AND group_code = ?
                        """,
                        params,
                    )
                    db.commit()
                return self.get(group_code, spectra_endpoint) or {}, False

            # A new match on this server+group immediately replaces only that
            # server+group's previously published map. Other Spectra servers using
            # the same code remain completely independent.
            db.execute(
                """
                INSERT INTO group_stats (
                    group_code, match_id, spectra_endpoint, player_puuid, region,
                    state, stats_json, tracked_at, stored_at, expires_at,
                    last_attempt_at, last_error
                ) VALUES (?, ?, ?, ?, NULL, 'live', NULL, ?, NULL, NULL, NULL, NULL)
                ON CONFLICT(spectra_endpoint, group_code) DO UPDATE SET
                    match_id = excluded.match_id,
                    player_puuid = excluded.player_puuid,
                    region = NULL,
                    state = 'live',
                    stats_json = NULL,
                    tracked_at = excluded.tracked_at,
                    stored_at = NULL,
                    expires_at = NULL,
                    last_attempt_at = NULL,
                    last_error = NULL
                """,
                (group_code, match_id, spectra_endpoint, player_puuid, timestamp_ms),
            )
            db.commit()
        return self.get(group_code, spectra_endpoint) or {}, True

    def mark_awaiting_end(
        self, group_code: str, spectra_endpoint: str, match_id: str
    ) -> bool:
        with self._lock, self._connect() as db:
            cursor = db.execute(
                """
                UPDATE group_stats SET state = 'awaiting_end'
                WHERE spectra_endpoint = ? AND group_code = ? AND match_id = ?
                  AND state IN ('pending', 'live', 'awaiting_end')
                """,
                (
                    normalize_endpoint(spectra_endpoint),
                    normalize_group_code(group_code),
                    match_id,
                ),
            )
            db.commit()
            return cursor.rowcount > 0

    def mark_match_complete(
        self, group_code: str, spectra_endpoint: str, match_id: str
    ) -> bool:
        with self._lock, self._connect() as db:
            cursor = db.execute(
                """
                UPDATE group_stats
                SET state = 'awaiting_stats', last_error = NULL
                WHERE spectra_endpoint = ? AND group_code = ? AND match_id = ?
                  AND state IN ('pending', 'live', 'awaiting_end', 'awaiting_stats')
                """,
                (
                    normalize_endpoint(spectra_endpoint),
                    normalize_group_code(group_code),
                    match_id,
                ),
            )
            db.commit()
            return cursor.rowcount > 0

    def get(self, group_code: str, spectra_endpoint: str) -> dict[str, Any] | None:
        group_code = normalize_group_code(group_code)
        spectra_endpoint = normalize_endpoint(spectra_endpoint)
        if not group_code or not spectra_endpoint:
            return None
        with self._lock, self._connect() as db:
            row = db.execute(
                """
                SELECT * FROM group_stats
                WHERE spectra_endpoint = ? AND group_code = ?
                """,
                (spectra_endpoint, group_code),
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_for_group(self, group_code: str) -> list[dict[str, Any]]:
        group_code = normalize_group_code(group_code)
        if not group_code:
            return []
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT * FROM group_stats WHERE group_code = ? ORDER BY spectra_endpoint",
                (group_code,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def list_pending(self) -> list[dict[str, Any]]:
        """Return all unresolved matches for diagnostics."""
        with self._lock, self._connect() as db:
            rows = db.execute(
                """
                SELECT * FROM group_stats
                WHERE state IN ('pending', 'live', 'awaiting_end', 'awaiting_stats')
                ORDER BY tracked_at
                """
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def list_awaiting_stats(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT * FROM group_stats WHERE state = 'awaiting_stats' ORDER BY tracked_at"
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def list_awaiting_end(self) -> list[dict[str, Any]]:
        """Matches that reached match point and still need Spectra end confirmation."""
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT * FROM group_stats WHERE state = 'awaiting_end' ORDER BY tracked_at"
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_cached_region(self, player_puuid: str) -> str | None:
        player_puuid = (player_puuid or "").strip()
        if not player_puuid:
            return None
        with self._lock, self._connect() as db:
            row = db.execute(
                "SELECT region FROM region_cache WHERE player_puuid = ?", (player_puuid,)
            ).fetchone()
        return str(row["region"]) if row else None

    def cache_region(self, player_puuid: str, region: str) -> None:
        player_puuid = (player_puuid or "").strip()
        region = (region or "").strip().lower()
        if not player_puuid or not region:
            return
        with self._lock, self._connect() as db:
            db.execute(
                """
                INSERT INTO region_cache (player_puuid, region, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player_puuid) DO UPDATE SET
                    region = excluded.region,
                    updated_at = excluded.updated_at
                """,
                (player_puuid, region, now_ms()),
            )
            db.commit()

    def set_region(
        self, group_code: str, spectra_endpoint: str, match_id: str, region: str
    ) -> bool:
        with self._lock, self._connect() as db:
            cursor = db.execute(
                """
                UPDATE group_stats SET region = ?, last_error = NULL
                WHERE spectra_endpoint = ? AND group_code = ? AND match_id = ?
                """,
                (
                    region,
                    normalize_endpoint(spectra_endpoint),
                    normalize_group_code(group_code),
                    match_id,
                ),
            )
            db.commit()
            return cursor.rowcount > 0

    def mark_attempt(
        self,
        group_code: str,
        spectra_endpoint: str,
        match_id: str,
        error: str | None = None,
    ) -> bool:
        with self._lock, self._connect() as db:
            cursor = db.execute(
                """
                UPDATE group_stats SET last_attempt_at = ?, last_error = ?
                WHERE spectra_endpoint = ? AND group_code = ? AND match_id = ?
                """,
                (
                    now_ms(),
                    error,
                    normalize_endpoint(spectra_endpoint),
                    normalize_group_code(group_code),
                    match_id,
                ),
            )
            db.commit()
            return cursor.rowcount > 0

    def save_stats(
        self,
        group_code: str,
        spectra_endpoint: str,
        match_id: str,
        response: dict[str, Any],
        timestamp_ms: int | None = None,
    ) -> bool:
        timestamp_ms = timestamp_ms if timestamp_ms is not None else now_ms()
        expires_at = timestamp_ms + STATS_LIFETIME_HOURS * 60 * 60 * 1000
        payload = json.dumps(response, separators=(",", ":"), ensure_ascii=False)
        with self._lock, self._connect() as db:
            cursor = db.execute(
                """
                UPDATE group_stats
                SET state = 'ready', stats_json = ?, stored_at = ?, expires_at = ?,
                    last_attempt_at = ?, last_error = NULL
                WHERE spectra_endpoint = ? AND group_code = ? AND match_id = ?
                """,
                (
                    payload,
                    timestamp_ms,
                    expires_at,
                    timestamp_ms,
                    normalize_endpoint(spectra_endpoint),
                    normalize_group_code(group_code),
                    match_id,
                ),
            )
            db.commit()
            return cursor.rowcount > 0

    def get_published_stats(
        self,
        group_code: str,
        spectra_endpoint: str,
        timestamp_ms: int | None = None,
    ) -> dict[str, Any] | None:
        timestamp_ms = timestamp_ms if timestamp_ms is not None else now_ms()
        row = self.get(group_code, spectra_endpoint)
        if not row or row["state"] != "ready" or not row["stats"]:
            return None
        expires_at = row.get("expiresAt")
        if not isinstance(expires_at, int) or timestamp_ms >= expires_at:
            self.expire(group_code, spectra_endpoint, row["matchId"])
            return None
        return row["stats"]

    def expire(self, group_code: str, spectra_endpoint: str, match_id: str) -> bool:
        with self._lock, self._connect() as db:
            cursor = db.execute(
                """
                UPDATE group_stats
                SET state = 'expired', stats_json = NULL
                WHERE spectra_endpoint = ? AND group_code = ? AND match_id = ?
                """,
                (
                    normalize_endpoint(spectra_endpoint),
                    normalize_group_code(group_code),
                    match_id,
                ),
            )
            db.commit()
            return cursor.rowcount > 0

    def cleanup_expired(self, timestamp_ms: int | None = None) -> int:
        timestamp_ms = timestamp_ms if timestamp_ms is not None else now_ms()
        with self._lock, self._connect() as db:
            cursor = db.execute(
                """
                UPDATE group_stats SET state = 'expired', stats_json = NULL
                WHERE state = 'ready' AND expires_at IS NOT NULL AND expires_at <= ?
                """,
                (timestamp_ms,),
            )
            db.commit()
            return cursor.rowcount

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        stats = None
        if row["stats_json"]:
            try:
                stats = json.loads(row["stats_json"])
            except json.JSONDecodeError:
                stats = None
        return {
            "groupCode": row["group_code"],
            "matchId": row["match_id"],
            "spectraEndpoint": row["spectra_endpoint"],
            "playerPuuid": row["player_puuid"],
            "region": row["region"],
            "state": row["state"],
            "stats": stats,
            "trackedAt": row["tracked_at"],
            "storedAt": row["stored_at"],
            "expiresAt": row["expires_at"],
            "lastAttemptAt": row["last_attempt_at"],
            "lastError": row["last_error"],
        }
