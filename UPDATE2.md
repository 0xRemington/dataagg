# UPDATE2: BTCUSDT candles from Binance (2026-11-04)

BTC price candles are now collected from **Binance spot BTCUSDT** for the same timeframes as
SPY/QQQ (1m, 5m, 15m, 4h, 1d). They're stored in the same tables and served by the same
`/v1/bars` endpoint. Yahoo collection is unchanged.

## 1. What changed

| Change | Files |
|---|---|
| New collector: native Binance klines over WebSocket; REST fills history and gaps | `src/marketagg/crypto/binance.py` (new) |
| CLI: `marketagg binance bootstrap` (owner role) and `marketagg binance stream` | `src/marketagg/cli.py` |
| Migration 0008: `instruments.asset_class` also allows `crypto` (additive) | `migrations/versions/0008_crypto_asset_class.py` (new) |
| `register_instrument` accepts `exchange_timezone` (default unchanged) | `src/marketagg/db/ingest.py` |
| `/v1/bars` uses the Binance policy for `provider_id=binance`; 4h labels/warnings are UTC for Binance | `src/marketagg/research/repository.py`, `src/marketagg/api/app.py` |
| Compose: `binance-bootstrap` (one-shot, owner role) and `binance` (ingest role, egress) | `deploy/compose.yaml` |
| Tests | `tests/unit/test_binance.py`, `tests/integration/test_binance_store.py` (new); `test_migrations.py` head name and `test_deployment.py` egress set updated |

## 2. How it works

- **One source for live and history.** The WebSocket (`data-stream.binance.vision`) writes each
  kline once Binance marks it closed (`x=true`). REST (`data-api.binance.vision/api/v3/klines`)
  fills history on the first run, fills from the last stored bar after every reconnect, and
  re-checks the last 60 bars of every timeframe every 5 minutes. Only bars that differ get written.
- **Native klines for every timeframe.** Nothing is derived. Buckets are Binance's: UTC-anchored
  and 24/7. 4h = 00/04/08/12/16/20 UTC, 1d = the UTC day, `session_date` = UTC date.
- **Same storage path as Yahoo.** `IngestRepository.stage_bars` then `publish`, so batches,
  revisions, content hashing and immutability all behave the same.
- **First-run history:** 1m 30 days; 5m and 15m 365 days; 4h and 1d 5 years.

## 3. Reading it

```sh
python marketagg.py symbols          # now lists BTCUSDT (provider binance)
curl -H "Authorization: Bearer $TOKEN" \
  "http://100.113.208.48:8000/v1/bars?instrument_id=<BTCUSDT uuid>&provider_id=binance&timeframe=15m&start=2026-11-01T00:00:00Z&end=2026-11-04T00:00:00Z"
```

`provider_id` must be **`binance`** for BTCUSDT (SPY/QQQ stay `yahoo`).

## 4. Things to know

- **Volume is USDT notional** (quote-asset volume), rounded to a whole number, because the
  shared `volume` column is an integer. BTC-unit volume would round to nearly nothing on 1m.
- **`session_kind` is stored as `RTH`**, meaning "the instrument's regular session", which for a
  24/7 market is the whole UTC day. The API filters on RTH, so this keeps the endpoint unchanged.
- **USDT, not USD.** BTCUSDT trades a few dollars to a few tens of dollars from USD venues.
- `/health/ready` `last_published_known_at` is global, so Binance now keeps it fresh around
  the clock. Use `ops status` (per-scope, Yahoo cadences) to judge Yahoo freshness. Binance
  scopes (`binance_*`) aren't in its cadence table, so they never raise Yahoo alerts.
- The collector logs to `docker logs marketagg-binance-1`. It reconnects on any error (backoff
  from 1 s up to 60 s), and Binance's 24 h connection limit is handled the same way.

## 5. Verification

- ruff and mypy (strict) clean. Unit, contract and integration tests: **276 passed**, 1 skipped
  (live Yahoo opt-in). Run on a disposable PostgreSQL 18, which was removed afterwards.
- Deploy: a verified backup was taken first (`/backups/20261104T022940Z-6c297191`), and the
  source tree was saved to `~/dataagg-src-pre-binance-20261104T022935Z.tgz`. Then the image was
  rebuilt and the stack brought up while markets were closed. The migration is at
  `0008_crypto_asset_class`.

## 6. Rollback

```sh
docker compose -f deploy/compose.yaml stop binance && docker compose -f deploy/compose.yaml rm -f binance binance-bootstrap
# full code rollback: restore the tarball, then: docker tag marketagg:pre-binance marketagg:local
```

Binance rows can stay where they are. Nothing else reads `provider_id=binance`. Stopping the
`binance` service is enough to pause collection. On a full code rollback the old code expects
migration head 0007, so `/health/ready` reports a migration mismatch. Either keep 0008 (it only
widens a check) and ignore that, or restore the backup above.
