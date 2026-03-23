import sys
from pathlib import Path

# Add src to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.persistence.db import DatabaseManager
from sqlalchemy import text

def check_db():
    try:
        settings = Settings()
        print(f"Connecting to: {settings.postgres_dsn.split('@')[-1]}") # Hide credentials
        
        db = DatabaseManager(settings.postgres_dsn)
        
        with db.get_session() as session:
            # 1. Connectivity Check
            session.execute(text("SELECT 1"))
            print("[PASS] Database connectivity verified.")
            
            # 2. Table Verification
            from delta_exchange_bot.persistence.models import Trade, Position, Order, Signal, ExecutionLog
            
            for model in [Trade, Position, Order, Signal, ExecutionLog]:
                count = session.query(model).count()
                print(f"[INFO] Table '{model.__tablename__}' exists and has {count} records.")
                
        print("\nSUMMARY: Database is working properly and schema is in sync.")
        
    except Exception as e:
        print(f"[FAIL] Database check failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_db()
