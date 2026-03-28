"""
backup_and_clean_db.py
======================
Backs up trading_live and trading_paper PostgreSQL databases to timestamped
SQL files using pure Python (psycopg) — no pg_dump required.

Usage:
    python scripts/backup_and_clean_db.py              # backup + clean both
    python scripts/backup_and_clean_db.py --backup-only  # backup, no clean
    python scripts/backup_and_clean_db.py --clean-only   # clean, no backup
    python scripts/backup_and_clean_db.py --db live     # target one DB only
    python scripts/backup_and_clean_db.py --db paper    # target one DB only
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import psycopg
import psycopg.rows

# ── Configuration ──────────────────────────────────────────────────────────────

DATABASES = {
    "live":  "postgresql://postgres:admin1234@localhost:5432/trading_live",
    "paper": "postgresql://postgres:admin1234@localhost:5432/trading_paper",
}

# Dump order: parents before children (for INSERT restore order).
# Truncation order: children before parents (FK constraints).
TABLES_DUMP_ORDER = [
    "trades",
    "orders",
    "positions",
    "signals",
    "execution_logs",
    "performance_metrics",
]

TABLES_TRUNCATE_ORDER = list(reversed(TABLES_DUMP_ORDER))

BACKUP_DIR = Path("backups/db")

# ── Pure-Python backup ─────────────────────────────────────────────────────────

def _quote_value(val) -> str:
    """Render a Python value as a SQL literal."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, dict | list):
        # JSON columns
        s = json.dumps(val).replace("'", "''")
        return f"'{s}'"
    # str / datetime / enum values
    s = str(val).replace("'", "''")
    return f"'{s}'"


def backup_database(label: str, dsn: str, backup_dir: Path, tag: str) -> Path:
    """Dump every table as INSERT statements into a .sql file."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    out_file = backup_dir / f"{label}_{tag}.sql"

    print(f"  Backing up {label} → {out_file} ...", end=" ", flush=True)

    total_rows = 0
    with psycopg.connect(dsn, row_factory=psycopg.rows.dict_row) as conn:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(f"-- Delta Exchange DB backup: {label}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write(f"-- Source DSN: {dsn}\n\n")
            f.write("SET client_encoding = 'UTF8';\n")
            f.write("SET standard_conforming_strings = on;\n\n")

            with conn.cursor() as cur:
                for table in TABLES_DUMP_ORDER:
                    # Check table exists
                    cur.execute(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name=%s)",
                        (table,),
                    )
                    if not cur.fetchone()["exists"]:
                        f.write(f"-- Table '{table}' not found — skipped.\n\n")
                        continue

                    cur.execute(f'SELECT * FROM "{table}"')  # noqa: S608
                    rows = cur.fetchall()
                    f.write(f"-- Table: {table}  ({len(rows)} rows)\n")

                    if not rows:
                        f.write(f"-- (empty)\n\n")
                        continue

                    cols = list(rows[0].keys())
                    cols_sql = ", ".join(f'"{c}"' for c in cols)

                    for row in rows:
                        vals_sql = ", ".join(_quote_value(row[c]) for c in cols)
                        f.write(
                            f'INSERT INTO "{table}" ({cols_sql}) '
                            f"VALUES ({vals_sql}) ON CONFLICT DO NOTHING;\n"
                        )
                    f.write("\n")
                    total_rows += len(rows)

    size_kb = out_file.stat().st_size // 1024
    print(f"OK  ({total_rows} rows, {size_kb} KB)")
    return out_file


# ── Clean ──────────────────────────────────────────────────────────────────────

def _row_counts(conn, tables: list[str]) -> dict[str, int]:
    counts = {}
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')  # noqa: S608
            counts[table] = cur.fetchone()[0]
    return counts


def clean_database(label: str, dsn: str) -> None:
    """Truncate all trading tables and reset auto-increment sequences to 1."""
    print(f"  Cleaning {label} ...")
    with psycopg.connect(dsn, autocommit=False) as conn:
        before = _row_counts(conn, TABLES_TRUNCATE_ORDER)
        total_before = sum(before.values())
        print(f"    Rows before: { {t: before[t] for t in TABLES_TRUNCATE_ORDER} }")

        with conn.cursor() as cur:
            tables_sql = ", ".join(f'"{t}"' for t in TABLES_TRUNCATE_ORDER)
            cur.execute(f"TRUNCATE TABLE {tables_sql} RESTART IDENTITY CASCADE")
        conn.commit()

        after = _row_counts(conn, TABLES_TRUNCATE_ORDER)
        total_after = sum(after.values())
        print(f"    Rows after:  { {t: after[t] for t in TABLES_TRUNCATE_ORDER} }")
        print(f"    Deleted {total_before - total_after} rows — {label} is clean.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backup and/or clean trading_live and trading_paper databases."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--backup-only", action="store_true",
        help="Only create backups; do not truncate any data.",
    )
    group.add_argument(
        "--clean-only", action="store_true",
        help="Only truncate data; skip backup (dangerous — use with care).",
    )
    parser.add_argument(
        "--db", choices=["live", "paper"], default=None,
        help="Target a single database. Omit to process both.",
    )
    args = parser.parse_args()

    targets = {args.db: DATABASES[args.db]} if args.db else DATABASES
    tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"\n{'='*60}")
    print(f"  Delta Exchange DB Backup & Clean  —  {tag}")
    print(f"{'='*60}\n")

    backed_up: list[Path] = []
    errors: list[str] = []

    for label, dsn in targets.items():
        print(f"[{label.upper()}]")

        # ── Backup ──────────────────────────────────────────────────────────
        if not args.clean_only:
            try:
                path = backup_database(label, dsn, BACKUP_DIR, tag)
                backed_up.append(path)
            except Exception as exc:
                errors.append(f"Backup failed for {label}: {exc}")
                print(f"  !! Backup failed: {exc}")
                print(f"  !! Skipping clean for {label} to avoid data loss.")
                print()
                continue  # Never clean without a successful backup

        # ── Clean ───────────────────────────────────────────────────────────
        if not args.backup_only:
            if not args.clean_only:
                answer = input(
                    f"\n  Backup complete. Truncate ALL data in '{label}'? [yes/N]: "
                ).strip().lower()
                if answer != "yes":
                    print(f"  Skipped clean for {label}.")
                    print()
                    continue
            try:
                clean_database(label, dsn)
            except Exception as exc:
                errors.append(f"Clean failed for {label}: {exc}")
                print(f"  !! Clean failed: {exc}")

        print()

    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"{'='*60}")
    if backed_up:
        print("  Backups saved to:")
        for p in backed_up:
            print(f"    {p.resolve()}")
    if errors:
        print(f"\n  ERRORS ({len(errors)}):")
        for e in errors:
            print(f"    - {e}")
        sys.exit(1)
    else:
        print("  Done — no errors.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
