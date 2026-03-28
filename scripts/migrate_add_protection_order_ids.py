"""
Migration: add stop_order_id and tp_order_id columns to the positions table.

Run once against your PostgreSQL database:
    poetry run python scripts/migrate_add_protection_order_ids.py

Safe to run multiple times — uses IF NOT EXISTS checks so it will skip
columns that already exist.
"""

import os
import sys

# ── resolve project root so imports work ──────────────────────────────
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dotenv import load_dotenv
load_dotenv()

import sqlalchemy as sa
from delta_exchange_bot.core.settings import Settings

COLUMNS_TO_ADD = [
    ("stop_order_id", "VARCHAR(64)"),
    ("tp_order_id",   "VARCHAR(64)"),
]


def run_migration(dsn: str) -> None:
    engine = sa.create_engine(dsn)

    with engine.begin() as conn:
        # Detect dialect
        dialect = engine.dialect.name  # "postgresql" or "sqlite"

        for col_name, col_type in COLUMNS_TO_ADD:
            if dialect == "postgresql":
                # PostgreSQL supports IF NOT EXISTS on ALTER TABLE ADD COLUMN
                sql = sa.text(
                    f"ALTER TABLE positions ADD COLUMN IF NOT EXISTS "
                    f"{col_name} {col_type};"
                )
                conn.execute(sql)
                print(f"  ✅  {col_name} ({col_type}) — ensured on positions (PostgreSQL)")

            elif dialect == "sqlite":
                # SQLite does not support IF NOT EXISTS on ALTER TABLE ADD COLUMN.
                # Check via PRAGMA first.
                result = conn.execute(sa.text("PRAGMA table_info(positions)"))
                existing_cols = {row[1] for row in result.fetchall()}
                if col_name not in existing_cols:
                    conn.execute(sa.text(
                        f"ALTER TABLE positions ADD COLUMN {col_name} {col_type};"
                    ))
                    print(f"  ✅  {col_name} ({col_type}) — added to positions (SQLite)")
                else:
                    print(f"  ⏭   {col_name} — already exists, skipped (SQLite)")
            else:
                print(f"  ⚠️  Unknown dialect '{dialect}' — skipping {col_name}")

    engine.dispose()
    print("\nMigration complete.")


def _postgres_reachable(dsn: str) -> bool:
    try:
        engine = sa.create_engine(dsn, connect_args={"connect_timeout": 3})
        with engine.connect():
            pass
        engine.dispose()
        return True
    except Exception:
        return False


if __name__ == "__main__":
    settings = Settings()
    dsn = settings.postgres_dsn

    # Fallback to SQLite state.db if Postgres DSN is the placeholder default
    if "localhost" in dsn and not _postgres_reachable(dsn):
        state_db = Path("state.db")
        if state_db.exists():
            dsn = f"sqlite:///{state_db.resolve()}"
            print(f"Postgres unreachable — using SQLite: {dsn}")
        else:
            print("Neither PostgreSQL nor state.db found. Nothing to migrate.")
            sys.exit(1)

    print(f"Running migration on: {dsn.split('@')[-1] if '@' in dsn else dsn}")
    run_migration(dsn)
