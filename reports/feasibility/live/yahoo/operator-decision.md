# Operator source decision: Yahoo Finance via yfinance

- Review ID: `operator-2026-10-26`
- Date: 2026-10-26
- Decided by: the operator (repository owner), in the build session.
- Decision: **approved for private research collection** on the operator's VPS.
- Operator's stated rationale: "yfinance is a third party service anyway so its all good."
- Scope: SPY and QQQ, the bounded sampling in `config/universe.yahoo.yaml`, private use only, no redistribution.

This records the operator's decision and the responsibility they accepted. It is not
an independent legal review: Yahoo's terms page (HTTP 999 during planning) was not
retrieved or verified by the build agent. The redistribution restriction in Yahoo's
delay notice (SLN2310) still applies, and the operator controls who can reach the
deployment. Quote freshness stays unknown (no provider event times).
