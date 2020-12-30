import gas_table
import sqlite3
import time, threading, json
from flask import Flask, request, abort, json, g, render_template, Response
from apscheduler.schedulers.background import BackgroundScheduler
from waitress import serve
app = Flask(__name__)

gas_table.c.close()
gas_table.conn.close()

DATABASE = 'gas_table.db'
UPDATE_INTERVAL = 1

def quick_update(): # Can't keep connection and cursor between threads, so quick open/close is fine
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    gas_table.update(connection=conn, cursor = c)
    c.close()
    conn.close()

def get_db(): # gets the db for flask app context
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.teardown_appcontext # gets the db for flask app context
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def str_is_int(string): # quickparse
    try: return int(string)
    except: return False

@app.route('/')
def home(): # render sick website
    return render_template('index.html')

@app.route('/datastream')
def chart_data(): # provide a datastream that sends an initial chunk of 100 msgs and then updates every second
    def generate_random_data():
        lastDat = None

        # Send past few entries
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        rows = gas_table.get_recent_gas(cursor = c, catch_error = False, num=100)
        c.close()
        conn.close()
        if len(rows) > 1 and rows[0] != 0:
            for entry in sorted(rows[1], key=lambda x: x[1]):
                row_dict = {'block':entry[1], 'date':entry[0], 'low':entry[2], 'avg':entry[3], 'high':entry[4]}
                lastDat = entry
                yield f'data:{json.dumps(row_dict)}\n\n'

        # Scan every x seconds for the most recent entry
        while True:
            # Scan db
            conn = sqlite3.connect(DATABASE)
            c = conn.cursor()
            rows = gas_table.get_recent_gas(cursor = c, catch_error = False)
            c.close()
            conn.close()

            # skip same block
            if len(rows) <= 1 or rows[0] == 0 or rows[1][0][1] <= lastDat[1]:
                time.sleep(UPDATE_INTERVAL)
                continue
            
            # send block data
            row_dict = {'block':rows[1][0][1], 'date':rows[1][0][0], 'low':rows[1][0][2], 'avg':rows[1][0][3], 'high':rows[1][0][4]}
            lastDat = rows[1][0]
            yield f'data:{json.dumps(row_dict)}\n\n'
            time.sleep(UPDATE_INTERVAL)

    return Response(generate_random_data(), mimetype='text/event-stream')

@app.route('/gas', methods=['GET'])
def get_gas(): # simply parse args and get gas via gas_table method
    conn = get_db()
    c = conn.cursor()

    number = int(request.args['num']) if 'num' in request.args and str_is_int(request.args['num']) else 1

    # Make sure table is not empty, fill and retry if it is
    first = gas_table.get_first_recorded_date(cursor = c)
    if(first[0] == 0):
        gas_table.append_gas(connection = conn, cursor = c)
        first = gas_table.get_first_recorded_date(cursor = c)
    first = first[1]

    if 'block' in request.args and str_is_int(request.args['block']):
        block = int(request.args['block'])
        if(block < first[1]): abort(410)
        rows = gas_table.get_historical_gas_by_block(cursor = c, block=block, num=number)
    
    elif 'date' in request.args and str_is_int(request.args['date']):
        date = int(request.args['date'])
        if(date < first[0]): abort(410)
        rows = gas_table.get_historical_gas_by_date(cursor = c, date=date, num=number)
    
    else: rows = gas_table.get_recent_gas(cursor = c, num=number)

    if(rows[0] == 0): abort(410)
    rows = rows[1]
    row_dict = {x[1]:{'date':x[0], 'low':x[2], 'avg':x[3], 'high':x[4]} for x in rows}
    return{
        'status': 1,
        'message': 'OK',
        'result': row_dict
    }

@app.route('/start', methods=['GET'])
def get_first(): # simple endpoint for get_first_recorded_date
    conn = get_db()
    c = conn.cursor()

    recorded = gas_table.get_first_recorded_date(cursor = c)
    if(recorded[0] == 0): abort(410)
    return {
        'status': 1,
        'message': 'OK',
        'result': {
            'firstDate': recorded[1][0],
            'firstBlock': recorded[1][1]
        }
    }

if __name__ == "__main__":
    apsched = BackgroundScheduler()
    apsched.start()

    apsched.add_job(quick_update, 'interval', seconds=UPDATE_INTERVAL)

    serve(app,host='0.0.0.0',port=8080)