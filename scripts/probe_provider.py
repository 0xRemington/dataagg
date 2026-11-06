"""Phase 0 provider feasibility probe (T00); see ``marketagg.feasibility``.

    uv run python scripts/probe_provider.py --provider fake --output reports/feasibility/fake
"""

from __future__ import annotations

import sys

from marketagg.feasibility import EXIT_BLOCKED, main, run_probe, write_report

__all__ = ["EXIT_BLOCKED", "main", "run_probe", "write_report"]

if __name__ == "__main__":
    sys.exit(main())
