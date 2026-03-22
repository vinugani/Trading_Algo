import sqlite3
import json
from pathlib import Path
from datetime import datetime

def check_signals(db_path="state.db", limit=10):
    if not Path(db_path).exists():
        print(f"Error: Database {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        query = """
            SELECT ts, symbol, action, confidence, price, strategy_name 
            FROM signals 
            ORDER BY ts DESC 
            LIMIT ?
        """
        rows = conn.execute(query, (limit,)).fetchall()
        
        if not rows:
            print("No signals found in the database yet.")
            return

        print(f"{'Timestamp':<20} | {'Symbol':<10} | {'Action':<6} | {'Conf':<6} | {'Price':<10} | {'Strategy'}")
        print("-" * 80)
        for r in rows:
            print(f"{r['ts']:<20} | {r['symbol']:<10} | {r['action']:<6} | {r['confidence']:<6.2f} | {r['price']:<10.2f} | {r['strategy_name']}")
    
    except Exception as e:
        print(f"Error reading signals: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_signals()
