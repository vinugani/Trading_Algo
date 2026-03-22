poetry run python -c "
import sqlite3, os
db = sqlite3.connect('state.db')

print('=== TRADES ===')
trades = db.execute('SELECT id, symbol, side, size, price, ts FROM trades ORDER BY ts DESC LIMIT 10').fetchall()
if trades:
    for t in trades:
        print(f'  {t[5]} | {t[1]} | {t[2].upper()} | size={t[3]:.4f} | price={t[4]}')
else:
    print('  No trades yet')

print('')
print('=== SIGNALS ===')
sigs = db.execute('SELECT symbol, action, confidence, price, ts FROM signals ORDER BY ts DESC LIMIT 10').fetchall()
if sigs:
    for s in sigs:
        print(f'  {s[4]} | {s[0]} | {s[1].upper()} | confidence={s[2]:.2f} | price={s[3]}')
else:
    print('  No signals yet')

print('')
print('=== OPEN POSITIONS ===')
pos = db.execute('SELECT symbol, side, size, entry_price FROM open_position_state').fetchall()
if pos:
    for p in pos:
        print(f'  {p[0]} | {p[1].upper()} | size={p[2]:.4f} | entry={p[3]}')
else:
    print('  No open positions')

db.close()
"
