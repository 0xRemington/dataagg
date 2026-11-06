"""Validate this planning handoff; does not test the future aggregator.

Run: python scripts/validate_build_guide.py
Optional SQL syntax parse: uv run --with pglast python scripts/validate_build_guide.py --sql
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    guide = root / "docs" / "build-guide"
    expected = [
        "00-assumptions-and-constraints.md",
        "01-architecture-and-adrs.md",
        "02-schema-and-data-contracts.md",
        "03-collection-scheduling-and-recovery.md",
        "04-agent-execution-roadmap.md",
        "05-tests-and-acceptance.md",
        "06-deployment-operations-and-costs.md",
        "07-provider-upgrade-strategy.md",
    ]
    errors: list[str] = []
    for name in expected:
        if not (guide / name).is_file():
            errors.append(f"Missing guide: {name}")
    markdown = [root / "README.md", *sorted(guide.glob("*.md"))]
    local_links = 0
    for path in markdown:
        content = path.read_text(encoding="utf-8")
        if content.count("```") % 2:
            errors.append(f"Unbalanced code fences: {path.name}")
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", content):
            if target.startswith(("http://", "https://", "#", "mailto:")):
                continue
            target_path = target.split("#", 1)[0]
            if not (path.parent / target_path).exists():
                errors.append(f"Broken local link: {path.name}: {target}")
            local_links += 1
    roadmap = (guide / expected[4]).read_text(encoding="utf-8")
    tickets = re.findall(r"^### (T\d{2}) /", roadmap, re.MULTILINE)
    wanted = [f"T{i:02}" for i in range(25)]
    if tickets != wanted:
        errors.append(f"Ticket order/coverage differs: {tickets}")
    fields = [
        "Goal", "Prerequisites", "Scope and owned paths",
        "Inputs / outputs / interfaces", "Implementation steps",
        "Acceptance criteria", "Test commands and expected results",
        "Risks / edge cases", "Do not change",
    ]
    for block in re.split(r"(?=^### T\d{2} /)", roadmap, flags=re.MULTILINE)[1:]:
        ticket = block[4:7]
        for field in fields:
            if f"**{field}:**" not in block:
                errors.append(f"{ticket}: missing {field}")
    referenced = set(re.findall(r"\bT\d{2}\b", roadmap))
    if referenced - set(wanted):
        errors.append(f"Unknown ticket references: {referenced - set(wanted)}")
    sql_path = guide / "schema" / "001_reference.sql"
    sql = sql_path.read_text(encoding="utf-8")
    tables = re.findall(r"CREATE TABLE (\w+)", sql)
    if len(tables) != len(set(tables)):
        errors.append("Duplicate table names")
    refs = re.findall(r"\bREFERENCES (\w+)", sql)
    if set(refs) - set(tables):
        errors.append(f"Undefined SQL references: {set(refs) - set(tables)}")
    if args.sql:
        from pglast import parse_sql

        statements = parse_sql(sql)
        print(f"PostgreSQL syntax parsed: {len(statements)} statements")
    if errors:
        raise SystemExit("\n".join(errors))
    words = sum(len(p.read_text(encoding="utf-8").split()) for p in markdown)
    print(f"PASS: {len(expected)} requested guides; {len(tickets)} complete tickets; "
          f"{len(tables)} SQL tables; {local_links} local links; {words:,} Markdown words")
    print("This is document/SQL syntax validation, not database or application execution.")


if __name__ == "__main__":
    main()
