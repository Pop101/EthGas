import time
import requests
import sqlite3

ETHERSCAN_KEY = 'KEY GOES HERE'

conn = sqlite3.connect('gas_table.db')
c = conn.cursor()

# Create necessary tables if they do not exist
c.execute('''CREATE TABLE IF NOT EXISTS GAS ([date] INTEGER PRIMARY KEY NOT NULL, [block] INTEGER NOT NULL, [slow] REAL NOT NULL, [average] REAL NOT NULL, [fast] REAL NOT NULL)''')
conn.commit()

assert 'KEY ' not in ETHERSCAN_KEY, 'Please paste your etherscan key into ./gas_table.py'

def clean_by_date(connection = conn, cursor = c, limit:int=365*24*3600, commit:bool = True):
    cursor.execute(f'''DELETE FROM GAS WHERE date < strftime('%s', 'now')-{int(limit)}''')
    if commit: connection.commit()

def append_gas(connection = conn, cursor = c):
    r = requests.get(f'https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={ETHERSCAN_KEY}')
    r.raise_for_status(); r = r.json()
    if 'result' not in r: return # TODO: do something here, maybe retry in 0.2s
    r = r['result']

    # If the block has not changed...
    recent = get_recent_gas(cursor = cursor, catch_error=False)
    if recent[0] == 0 and recent[1][0][1] == r['LastBlock']:
        return tuple(recent[1][0][1:])

    cursor.execute('''REPLACE INTO GAS VALUES (strftime('%s', 'now'), ?, ?, ?, ?)''', (int(r['LastBlock']), float(r['SafeGasPrice']), float(r['ProposeGasPrice']), float(r['FastGasPrice'])))
    connection.commit()
    return int(r['LastBlock']), float(r['SafeGasPrice']), float(r['ProposeGasPrice']), float(r['FastGasPrice'])

def update(connection = conn, cursor = c):
    clean_by_date(connection = connection, cursor = cursor, commit=False)
    append_gas(connection = connection, cursor = cursor)

def get_recent_gas(cursor = c, num:int = 1, catch_error:bool = True, ):
    cursor.execute('SELECT max(date), block, avg(slow), avg(average), avg(fast) FROM gas GROUP BY block ORDER BY block DESC LIMIT ?', (num,))

    rows = cursor.fetchall()
    if len(rows) > 0:
        return 1, rows
    elif catch_error:
        print('No table data yet')
        return 0, [append_gas(),] * num
    else: return 0, [[0] * 5] * num

def get_first_recorded_date(cursor = c):
    cursor.execute('SELECT * FROM GAS ORDER BY block ASC LIMIT 1')

    rows = cursor.fetchall()
    if len(rows) > 0:
        return 1, (rows[0][0], rows[0][1])
    else:
        return 0, (time.time(), time.time())

def get_historical_gas_by_date(cursor = c, date:int=time.time(), num:int = 1):
    cursor.execute('''SELECT max(date), block, avg(slow), avg(average), avg(fast) FROM gas WHERE date < ? GROUP BY block ORDER BY block DESC LIMIT ?''', (int(date), num))

    rows = cursor.fetchall()
    if len(rows) > 0:
        return 1, rows
    else:
        print('Table data does not span back this far')
        return 0, list()

def get_historical_gas_by_block(cursor = c, block:int = 0, num:int = 1):
    cursor.execute('''SELECT max(date), block, avg(slow), avg(average), avg(fast) FROM gas WHERE block < ? GROUP BY block ORDER BY block DESC LIMIT ?''', (int(block), num))

    rows = cursor.fetchall()
    if len(rows) > 0:
        return 1, rows
    else:
        print('Table data does not span back this far')
        return 0, list()

if __name__ == "__main__":
    update()
    print(get_recent_gas())
    print(get_first_recorded_date())