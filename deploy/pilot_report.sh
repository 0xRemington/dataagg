#!/usr/bin/env bash
# T19: measure the five-session pilot from the live database, then evaluate the release
# gate. Writes reports/pilot/{capacity.json,capacity.md,release-decision.json}.
set -uo pipefail
cd "$(dirname "$0")/.."
docker compose -f deploy/compose.yaml --profile ops run --rm pilot-report
sudo -u ubuntu -H bash -lc "cd '$PWD' && uv run marketagg release check --report reports/pilot" \
    > reports/pilot/release-check.log 2>&1
echo "release check exit=$? (0 = release allowed); see reports/pilot/release-decision.json"
