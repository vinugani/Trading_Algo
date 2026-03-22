import sqlite3
import os

def verify():
    db_path = "state.db"
    if not os.path.exists(db_path):
        print(f"DB not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check trade_records table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_records'")
    table = cursor.fetchone()
    if table:
        print("PASS: trade_records table exists.")
        cursor.execute("PRAGMA table_info(trade_records)")
        cols = [c[1] for c in cursor.fetchall()]
        print(f"Columns: {', '.join(cols)}")
    else:
        print("FAIL: trade_records table NOT found.")
    
    conn.close()

if __name__ == "__main__":
    verify()
