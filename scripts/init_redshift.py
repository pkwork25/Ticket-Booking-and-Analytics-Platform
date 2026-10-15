"""Create the Redshift schema + views through the Redshift Data API (no JDBC client or VPN needed).

usage: python scripts/init_redshift.py --workgroup ticketpulse-wg --database ticketdb \
           --secret-arn arn:aws:secretsmanager:...:secret:ticketpulse/redshift/admin-xxxx
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_FILES = [ROOT / "redshift" / "01_tables.sql", ROOT / "redshift" / "02_views.sql"]


def split_sql(text: str) -> list[str]:
    """Split on ';' outside of comments / quotes. Good enough for our DDL files."""
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    stmts, buf, in_quote = [], [], False
    for ch in "\n".join(lines):
        if ch == "'":
            in_quote = not in_quote
        if ch == ";" and not in_quote:
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def main() -> int:
    import boto3

    p = argparse.ArgumentParser()
    p.add_argument("--workgroup", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--secret-arn", required=True)
    p.add_argument("--region", default=None)
    a = p.parse_args()

    client = boto3.client("redshift-data", region_name=a.region)
    for path in SQL_FILES:
        stmts = split_sql(path.read_text())
        resp = client.batch_execute_statement(
            WorkgroupName=a.workgroup, Database=a.database, SecretArn=a.secret_arn, Sqls=stmts
        )
        while True:
            d = client.describe_statement(Id=resp["Id"])
            if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
                break
            time.sleep(1)
        if d["Status"] != "FINISHED":
            raise SystemExit(f"{path.name}: {d['Status']} - {d.get('Error')}")
        print(f"{path.name}: {len(stmts)} statements OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
