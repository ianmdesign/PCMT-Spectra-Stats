# PCMT Stats

Standalone stats backend for the PCMT Spectra frontend. It replaces the hosted Spectra stats dependency while leaving the stock Spectra Server unchanged.

## What this version does

- One PCMT Stats instance can serve multiple Spectra servers at the same time.
- A stats record is keyed by **`spectraEndpoint + groupCode`**, not by group code alone.
- NA `A`, EU `A`, and local `A` are therefore three independent matches.
- A new match on one server/group pair immediately hides only that pair's previous map.
- Completed stats remain available for exactly **24 hours** from successful storage unless replaced first by a new match on the same server/group pair.
- Pending maps poll HenrikDev for the completed match payload.
- Henrik requests share one global internal limiter. The default is 12 requests/minute, below a 30 requests/minute personal-key limit.
- Player region lookups are cached by PUUID in SQLite to avoid repeating account lookups on later maps.
- Existing v0.1.x SQLite databases are migrated automatically from a group-only primary key to the composite server/group key.

## Service flow

```text
Frontend (groupCode + its serverEndpoint)
             |
             | POST /api/watch
             v
        PCMT Stats :5500
             |
             | Socket.IO logon for that group code
             v
      selected Spectra :5200
             |
             | matchId + roster
             v
        HenrikDev v4 match API
             |
             v
       SQLite 24h cache
             |
             | GET /getStats?code=A&spectraEndpoint=...
             v
   Map Breakdown / Team Breakdown
```

## Configuration

`config/config.json`:

```json
{
  "allowedSpectraEndpoints": [
    "https://na.valospectra.com:5200",
    "https://eu.valospectra.com:5200",
    "http://imlxd.duckdns.org:5200",
    "http://localhost:5200"
  ],
  "henrikApiBaseUrl": "https://api.henrikdev.xyz",
  "henrikMaxRequestsPerMinute": 12,
  "pollIntervalSeconds": 45,
  "retryErrorSeconds": 60,
  "requestTimeoutSeconds": 15,
  "watchTtlSeconds": 300,
  "maxActiveWatches": 50
}
```

`allowedSpectraEndpoints` is an exact allow-list (after trailing-slash normalization). Add the exact `serverEndpoint` value used by every frontend container that should be allowed to register a watch.

The default internal Henrik limit of 12 requests/minute is intentionally lower than a 30 requests/minute personal key. All simultaneous games share that single limiter.

The 24-hour stats lifetime is fixed in code rather than configurable.

## Henrik API key

Create `.env` next to `docker-compose.yml`:

```env
HENRIK_API_KEY=your_key_here
```

Do not put the API key in `config.json` or commit it.

## Docker

```bash
docker compose up -d --build
```

Default port:

```text
5500
```

For a GHCR release image, `docker-compose.yml` defaults to:

```text
ghcr.io/ianmdesign/pcmt-stats:latest
```

## Frontend runtime config

All frontend containers can point at the same stats service:

```json
"statsEndpoint": "https://pcmtstats.ianmlxdesign.ca"
```

Each frontend keeps its own normal Spectra setting:

```json
"serverEndpoint": "https://na.valospectra.com:5200"
```

or EU/local as appropriate. The frontend integration sends that `serverEndpoint` to both `/api/watch` and `/getStats`, so identical group codes on different Spectra servers cannot collide.

## API

### Register/renew a watch

```http
POST /api/watch
Content-Type: application/json

{
  "groupCode": "A",
  "spectraEndpoint": "https://eu.valospectra.com:5200"
}
```

The frontend renews this periodically. Stale watches disconnect automatically.

### Get stats

```http
GET /getStats?code=A&spectraEndpoint=https%3A%2F%2Feu.valospectra.com%3A5200
```

While a map is pending or absent, the endpoint returns HTTP 200 with an empty `data.players` array so the existing 15-second frontend polling behavior remains quiet.

For backwards compatibility, `spectraEndpoint` may be omitted only when exactly one stored Spectra server currently uses that group code. If more than one server has the same code, the service deliberately returns no players rather than guessing which production requested the data.

### Diagnostics

```http
GET /api/status
GET /api/group/A?spectraEndpoint=https%3A%2F%2Feu.valospectra.com%3A5200
GET /api/internal/debug/tasks
GET /healthz
```

Diagnostics do not expose the Henrik key, player PUUIDs, or the full cached stats payload.

## Persistence and overwrite behavior

For each unique `(spectraEndpoint, groupCode)` pair:

1. Spectra reports a new `matchId`.
2. Any previously published stats for **that pair only** stop resolving immediately.
3. The new map is marked pending.
4. PCMT Stats fetches until the completed match is available.
5. The completed response is stored and served for 24 hours.
6. A later map on the same pair replaces it immediately; a map on a different Spectra server does not affect it.

SQLite lives at:

```text
data/stats.sqlite3
```

Restarting the service does not reset expiry timestamps or pending matches.

## Tests

```bash
pip install -r requirements.txt
PYTHONPATH=. python -m unittest discover -s tests -v
python -m compileall app tests
```
