# Build-guide validation

Checked on 2026-10-24. This is a planning artifact validation record, not evidence that the aggregator has been implemented or deployed.

## Checks performed

- Read the original brief and mapped its requested deliverables to eight numbered guides, the reference DDL, and the requirement traceability table in document 05.
- Checked current primary provider documentation/source code, Yahoo's published delay/redistribution notice, exchange calendar material, PostgreSQL documentation, and a licensed-provider example. Links and unresolved checks are recorded beside the relevant claims in 00, 06 and 07.
- Created 25 ordered tickets (T00–T24), each with the nine required ticket fields in addition to ID/title, plus phase gates, dependencies, owned paths, planned test commands, MVP cutline and first-agent prompt.
- Parsed the reference DDL with `pglast`: **41 PostgreSQL statements parsed successfully**. This verifies SQL syntax, not database permissions, cross-row invariants, migration behavior, or concurrency correctness.
- Validated requested artifact presence, local Markdown link targets, balanced fenced code blocks, ticket numbering/required fields, and SQL table-reference names with the included validator.
- Reviewed request/storage arithmetic and the agreement between architecture, schemas, schedules, roadmap and acceptance tests. Estimates are explicitly labeled; none are presented as measured pilot results.

## Reproduce document checks

From repository root:

```text
python scripts/validate_build_guide.py
uv run --with pglast python scripts/validate_build_guide.py --sql
```

The optional second command obtains a SQL parser in uv's tool environment. It does not install or run the future application. The script's checks are structural; technical implementation review remains required.

## Not executed / unresolved

- No live Yahoo collection or five-session pilot was run. The terms URL returned HTTP 999 during planning; rights/retention/automation permissions remain unresolved, so live-source enablement is gated in the plan.
- No application, deployment, API, UI, backup, or restore tests were run: those are downstream implementation deliverables with explicit acceptance commands.
- The local Docker client is installed, but `docker version` could not connect to `dockerDesktopLinuxEngine`. PostgreSQL DDL execution and migration tests were therefore **not run**. T03 must execute the migration chain and constraints against disposable PostgreSQL 18; parsing is not a substitute.
- No actual VPS quote, account entitlement, measured rate ceiling, historical-data availability, storage compression ratio, or performance/RPO/RTO result is claimed.

The original brief remains the requirements source. The guide resolves implementation defaults while preserving these external and experimental gates.
