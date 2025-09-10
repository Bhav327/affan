"""
BookMyShow - Minimal Python Flask app (single-file)
Scope: simple REST API for movie bookings in South Bangalore (sample cinemas)

How to run:
 1. Make sure Python 3.8+ and pip are installed.
 2. pip install flask
 3. python bookmyshow_south_bangalore.py
 4. Open http://127.0.0.1:5000/
"""

from flask import Flask, g, jsonify, request
import sqlite3
import os
from contextlib import closing
from datetime import datetime, timedelta

DB_PATH = 'bookings.db'

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# --- Database helpers ---


def get_db():
    """Return SQLite connection (initialize if needed)."""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db


def init_db(db_conn):
    """Create tables and seed sample data if empty."""
    cur = db_conn.cursor()
    # Create tables
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS cinemas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        area TEXT NOT NULL,
        address TEXT
    );

    CREATE TABLE IF NOT EXISTS movies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        duration_minutes INTEGER,
        language TEXT
    );

    CREATE TABLE IF NOT EXISTS shows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cinema_id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        show_time TEXT NOT NULL,
        screen TEXT,
        price INTEGER DEFAULT 150,
        FOREIGN KEY(cinema_id) REFERENCES cinemas(id),
        FOREIGN KEY(movie_id) REFERENCES movies(id)
    );

    CREATE TABLE IF NOT EXISTS seats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER NOT NULL,
        seat_label TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'available',
        FOREIGN KEY(show_id) REFERENCES shows(id)
    );

    CREATE TABLE IF NOT EXISTS bookings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER NOT NULL,
        customer_name TEXT NOT NULL,
        seats TEXT NOT NULL,
        total_price INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(show_id) REFERENCES shows(id)
    );
    ''')

    # Only seed if database is empty
    cur.execute("SELECT COUNT(*) FROM cinemas")
    if cur.fetchone()[0] == 0:
        cinemas = [
            ("Innovative Filmplex", "Jayanagar", "12th Main, Jayanagar"),
            ("PVR Koramangala", "Koramangala", "CMH Road"),
            ("INOX BTM", "BTM Layout", "5th Stage, BTM Layout")
        ]
        cur.executemany(
            'INSERT INTO cinemas (name, area, address) VALUES (?,?,?)', cinemas)

        movies = [
            ("Flight of Fancy", 140, "English"),
            ("Love in Bengaluru", 120, "Kannada"),
            ("Mystery at MG Road", 130, "Hindi")
        ]
        cur.executemany(
            'INSERT INTO movies (title, duration_minutes, language) VALUES (?,?,?)', movies)

        # Create shows
        now = datetime.now().replace(minute=0, second=0, microsecond=0)
        shows = []
        cinema_ids = [row[0] for row in cur.execute('SELECT id FROM cinemas')]
        movie_ids = [row[0] for row in cur.execute('SELECT id FROM movies')]

        for c in cinema_ids:
            for i, m in enumerate(movie_ids):
                for t in range(3):  # 3 shows per movie per cinema
                    show_time = (now + timedelta(hours=2 + i + t*3)).isoformat()
                    shows.append(
                        (c, m, show_time, f"Screen {t+1}", 150 + i*20))
        cur.executemany(
            'INSERT INTO shows (cinema_id, movie_id, show_time, screen, price) VALUES (?,?,?,?,?)', shows)

        # Create seats
        seat_rows = ['A', 'B', 'C', 'D', 'E']
        seats_to_insert = []
        show_ids = [row[0] for row in cur.execute('SELECT id FROM shows')]
        for sid in show_ids:
            for r in seat_rows:
                for n in range(1, 7):
                    label = f"{r}{n}"
                    seats_to_insert.append((sid, label, 'available'))
        cur.executemany(
            'INSERT INTO seats (show_id, seat_label, status) VALUES (?,?,?)', seats_to_insert)

    db_conn.commit()


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# --- Utility ---


def dict_from_row(row):
    return {k: row[k] for k in row.keys()}

# --- Routes ---


@app.route('/')
def index():
    return jsonify({
        'app': 'BookMyShow - South Bangalore (demo)',
        'endpoints': [
            {'GET': '/cinemas'}, {'GET': '/movies'},
            {'GET': '/cinemas/<id>/shows'}, {'GET': '/shows/<id>/seats'},
            {'POST': '/book'}, {'GET': '/bookings'}
        ]
    })


@app.route('/cinemas')
def list_cinemas():
    cur = get_db().execute('SELECT * FROM cinemas')
    return jsonify([dict_from_row(r) for r in cur.fetchall()])


@app.route('/movies')
def list_movies():
    cur = get_db().execute('SELECT * FROM movies')
    return jsonify([dict_from_row(r) for r in cur.fetchall()])


@app.route('/cinemas/<int:cinema_id>/shows')
def shows_for_cinema(cinema_id):
    cur = get_db().execute('''
        SELECT s.id, s.show_time, s.screen, s.price,
               m.title AS movie_title
        FROM shows s
        JOIN movies m ON s.movie_id = m.id
        WHERE s.cinema_id = ?
        ORDER BY s.show_time
    ''', (cinema_id,))
    return jsonify([dict_from_row(r) for r in cur.fetchall()])


@app.route('/shows/<int:show_id>/seats')
def seats_for_show(show_id):
    cur = get_db().execute(
        'SELECT seat_label, status FROM seats WHERE show_id=? ORDER BY seat_label',
        (show_id,))
    return jsonify({'show_id': show_id,
                    'seats': [dict_from_row(r) for r in cur.fetchall()]})


@app.route('/book', methods=['POST'])
def book_seats():
    data = request.get_json(force=True)
    required = ['show_id', 'customer_name', 'seats']
    if not all(k in data for k in required):
        return jsonify({'error': 'Missing fields: show_id, customer_name, seats[]'}), 400

    show_id = int(data['show_id'])
    customer = data['customer_name'].strip()
    seats_req = data['seats']

    if not isinstance(seats_req, list) or not seats_req:
        return jsonify({'error': 'Seats must be a non-empty list like ["A1","A2"]'}), 400

    db = get_db()
    cur = db.cursor()
    try:
        cur.execute('BEGIN')
        cur.execute('SELECT price FROM shows WHERE id=?', (show_id,))
        row = cur.fetchone()
        if not row:
            db.rollback()
            return jsonify({'error': 'Show not found'}), 404
        price = row['price']

        # Check seat availability
        placeholders = ','.join('?' for _ in seats_req)
        query = f"SELECT seat_label, status FROM seats WHERE show_id=? AND seat_label IN ({placeholders})"
        cur.execute(query, (show_id, *seats_req))
        rows = cur.fetchall()
        found = {r['seat_label']: r['status'] for r in rows}

        for s in seats_req:
            if s not in found:
                db.rollback()
                return jsonify({'error': f'Seat {s} does not exist'}), 400
            if found[s] != 'available':
                db.rollback()
                return jsonify({'error': f'Seat {s} is already booked'}), 409

        # Book seats
        for s in seats_req:
            cur.execute(
                'UPDATE seats SET status=? WHERE show_id=? AND seat_label=?',
                ('booked', show_id, s))

        total = price * len(seats_req)
        now = datetime.now().isoformat()
        seats_csv = ','.join(seats_req)
        cur.execute('''
            INSERT INTO bookings (show_id, customer_name, seats, total_price, created_at)
            VALUES (?,?,?,?,?)''',
                    (show_id, customer, seats_csv, total, now))
        booking_id = cur.lastrowid
        db.commit()

        return jsonify({'booking_id': booking_id, 'show_id': show_id,
                        'seats': seats_req, 'total_price': total}), 201
    except sqlite3.Error as e:
        db.rollback()
        return jsonify({'error': 'Database error', 'details': str(e)}), 500


@app.route('/bookings')
def list_bookings():
    cur = get_db().execute('''
        SELECT b.id, b.show_id, b.customer_name, b.seats,
               b.total_price, b.created_at,
               m.title AS movie_title, c.name AS cinema_name
        FROM bookings b
        JOIN shows s ON b.show_id = s.id
        JOIN movies m ON s.movie_id = m.id
        JOIN cinemas c ON s.cinema_id = c.id
        ORDER BY b.created_at DESC
    ''')
    return jsonify([dict_from_row(r) for r in cur.fetchall()])


if __name__ == '__main__':
    with closing(sqlite3.connect(DB_PATH)) as conn:
        init_db(conn)
    app.run(debug=True)
