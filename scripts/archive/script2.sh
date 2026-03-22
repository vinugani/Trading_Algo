poetry run python -c "
import sqlite3, time
db = sqlite3.connect('state.db')
count = db.execute('SELECT COUNT(*) FROM trades').fetchone()[0]
print(f'Total paper trades so far: {count}')
db.close()
"
