# Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12 (uv can install it: `uv python install 3.12`). No network access, database, or provider credentials are needed for unit and contract tests.

## Commands

Linux / macOS (bash):

```bash
uv sync --frozen --all-extras
uv run ruff check .
uv run mypy src/marketagg
uv run pytest tests/unit tests/contracts
uv run marketagg config check --profile fixture
REQUESTS_PER_MINUTE=0 uv run marketagg config check --profile fixture   # exits 2
```

Windows (PowerShell):

```powershell
uv sync --frozen --all-extras
uv run ruff check .
uv run mypy src/marketagg
uv run pytest tests/unit tests/contracts
uv run marketagg config check --profile fixture
$env:REQUESTS_PER_MINUTE = "0"; uv run marketagg config check --profile fixture; Remove-Item Env:REQUESTS_PER_MINUTE
```

All timestamps are handled as aware UTC regardless of the host timezone. Paths in configuration are `pathlib.Path` values. Use forward slashes or quoted Windows paths in `--env-file` files.

## Configuration

`marketagg config check [--profile fixture|development|production] [--env-file PATH] [--json]` validates typed settings and secret-file references. It exits `0` when valid and `2` when invalid. Precedence is profile defaults, then `--env-file`, then the process environment. See [.env.example](../.env.example) for every variable.

- Secrets are only accepted through `*_FILE` paths. Inline `DATABASE_URL`, `API_READ_TOKEN`, and similar variables are refused.
- A non-fake provider needs `LIVE_DATA_ENABLED=true` **and** a `SOURCE_REVIEW_ID`. A licensed provider also needs an existing `PROVIDER_CREDENTIALS_FILE`. The `fixture` profile never permits a live source.
- A non-loopback `API_BIND_HOST` requires both API token files.

## Tests

Pytest markers: `integration` (disposable PostgreSQL 18), `acceptance`, `performance`, and `live`. `live` tests are skipped unless `--run-live` is passed. Tests without the `integration` or `live` marker cannot open network sockets.

Golden contract fixtures live in `tests/fixtures/synthetic/`. If a contract changes on purpose, regenerate them with `MARKETAGG_UPDATE_GOLDEN=1 uv run pytest tests/contracts/test_provider.py`, then review the diff as a contract change.

## Integration, acceptance and release (T03–T19)

Integration and acceptance tests need a disposable PostgreSQL 18 server. Every database they create is named `marketagg_test_*` and is dropped afterwards. Without `MARKETAGG_TEST_ADMIN_DSN` these tests are reported as skipped, never as passed.

```bash
export MARKETAGG_TEST_ADMIN_DSN=postgresql://postgres@127.0.0.1:54318/postgres
uv run pytest tests/integration -m "not live"
uv run python scripts/run_acceptance.py --profile fixture [--signing-key-file PATH]
```

`run_acceptance.py` runs scenarios A01–A09 and the temporal adversaries from doc 05. It writes `reports/acceptance/<stamp>-fixture/` containing `manifest.json` (versions, image digests, seed, fixture checksums, per-scenario results, measured RPO/RTO and latencies, documented limits) and `manifest.sha256`, plus `manifest.hmac` when a key is given. It exits 0 only when every scenario passed and nothing was skipped.

The live MVP release gate (T19) runs after a five-session pilot:

```bash
uv run python scripts/measure_capacity.py --sessions 5 --output reports/pilot --profile production
uv run marketagg release check --report reports/pilot
```

`release check` exits 0 only with complete evidence. That means a live provider, five measured sessions, an approved source review (`reports/feasibility/live/<provider>/source-review.md` containing `Release decision: APPROVED` and no `UNRESOLVED` items), a signed-off acceptance manifest, a measured restore drill, and the coverage, lag, headroom and request-budget objectives from doc 06. Each run writes `reports/pilot/release-decision.json`.
