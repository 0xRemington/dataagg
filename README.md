# Options market-data aggregator — agent build guide

This folder is the architecture and implementation handoff requested by [the original brief](options_market_data_aggregator_astra_brief.md). It contains a build plan, not an implemented aggregator. Planning baseline: **2026-10-24**.

Read these documents in order:

1. [Assumptions, source evidence, and constraints](docs/build-guide/00-assumptions-and-constraints.md)
2. [Architecture, ADRs, and repository boundaries](docs/build-guide/01-architecture-and-adrs.md)
3. [Schemas, temporal semantics, and API contracts](docs/build-guide/02-schema-and-data-contracts.md)
4. [Collection, scheduling, backfill, and recovery](docs/build-guide/03-collection-scheduling-and-recovery.md)
5. [Agent tickets, dependencies, and launch prompt](docs/build-guide/04-agent-execution-roadmap.md)
6. [Tests and release acceptance](docs/build-guide/05-tests-and-acceptance.md)
7. [Deployment, operations, security, and costs](docs/build-guide/06-deployment-operations-and-costs.md)
8. [Provider upgrade strategy](docs/build-guide/07-provider-upgrade-strategy.md)

[Reference PostgreSQL DDL](docs/build-guide/schema/001_reference.sql) specifies the initial persistence contract. The database agent must turn it into ordered, tested Alembic migrations; it is not a deployment command. [Guide validation](docs/build-guide/VALIDATION.md) records what was checked for this handoff.

Start implementation using the **first agent prompt** at the end of document 04. Phase 1 can proceed with synthetic fixtures while Phase 0 resolves live-source feasibility. A live-data release requires both gates. No licensed feed, VPS, API credentials, data-use permission, or measured capacity is assumed to exist.

**Implementation status:** the MVP tickets T00–T19 are implemented. Live collection of SPY/QQQ from Yahoo Finance runs on the operator's VPS, under operator decision `operator-2026-10-26`. The dashboard and API are served on port 8000; see `deploy/runbooks/deploy.md`. The five-session pilot (2026-10-26 to 30) decides the MVP release automatically on 2026-10-31; see `reports/pilot/README.md`. T20–T24 (models, surfaces, backtests, alerts, licensed feed) wait until data aggregation is proven in production.

The essential outcome is a small, private, restart-safe collector with immutable options observations, versioned underlying candles, causal queries, a usable dashboard, and tested recovery. Model pricing and live trading signals follow later.
