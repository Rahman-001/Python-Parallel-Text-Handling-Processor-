from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3, json, io, csv
from werkzeug.security import generate_password_hash, check_password_hash
from backend_text_analysis import TextAnalyzer

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
DATABASE_PATH = 'users.db'

def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT UNIQUE, password TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS processed_history (id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT, score REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)')
        db.execute('CREATE INDEX IF NOT EXISTS idx_content ON processed_history(content)')
        db.execute('CREATE TABLE IF NOT EXISTS inbox (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, message TEXT, type TEXT, report_data TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)')
        db.execute('CREATE TABLE IF NOT EXISTS activity_history (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT, operations TEXT, status TEXT, records_count INTEGER, processing_time REAL, report_data TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)')
        db.execute('CREATE TABLE IF NOT EXISTS contact_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)')
init_db()

@app.route("/api/signup", methods=["POST"])
def signup():
    data = request.json
    pw = generate_password_hash(data.get("password"))
    try:
        with get_db() as db:
            db.execute("INSERT INTO users (name, email, password) VALUES (?, ?, ?)", (data.get("full_name"), data.get("email"), pw))
            db.commit()
        return jsonify({"message": "Success"}), 201
    except: return jsonify({"message": "Error"}), 400

@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    with get_db() as db:
        user = db.execute("SELECT * FROM users WHERE email = ?", (data.get("email"),)).fetchone()
    if user and check_password_hash(user['password'], data.get("password")):
        return jsonify({"message": "OK", "user": data.get("email")}), 200
    return jsonify({"message": "Error"}), 401

@app.route("/api/reset-password", methods=["POST"])
def reset_password():
    data = request.json
    new_pw = generate_password_hash(data.get("new_password"))
    with get_db() as db:
        db.execute("UPDATE users SET password = ? WHERE email = ?", (new_pw, data.get("email")))
        db.commit()
    return jsonify({"message": "Updated"}), 200

@app.route('/api/analyze', methods=['POST'])
def analyze():
    data = request.json
    analyzer = TextAnalyzer()
    results, raw_rows, stats, scores = analyzer.run_pipeline(data.get('text', ''), data.get('operations', []))
    if raw_rows:
        with get_db() as db:
            for idx, row in enumerate(raw_rows[:50]):
                db.execute("INSERT INTO processed_history (content, score) VALUES (?, ?)", (str(row), scores[idx]))
            report_json = json.dumps(results)
            db.execute("INSERT INTO inbox (title, message, type, report_data) VALUES (?, ?, ?, ?)", 
                       ("Analysis Task Completed", f"Successfully processed {len(raw_rows)} records.", "success", report_json))
            db.execute('''INSERT INTO activity_history (filename, operations, status, records_count, processing_time, report_data) 
                         VALUES (?, ?, ?, ?, ?, ?)''', (data.get('filename', 'Bulk_Data.csv'), ", ".join(data.get('operations', [])), "Completed", len(raw_rows), stats['processing_time'], report_json))
            db.commit()
    return jsonify({"results": results, "stats": stats})

@app.route('/api/search', methods=['GET'])
def search():
    q = request.args.get('q', '')
    with get_db() as db:
        rows = db.execute("SELECT * FROM processed_history WHERE content LIKE ? ORDER BY timestamp DESC LIMIT 10", (f'%{q}%',)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/history', methods=['GET'])
def get_history():
    with get_db() as db:
        rows = db.execute("SELECT * FROM activity_history ORDER BY timestamp DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/inbox', methods=['GET'])
def get_inbox():
    with get_db() as db:
        rows = db.execute("SELECT * FROM inbox ORDER BY timestamp DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/cleanup', methods=['POST'])
def cleanup():
    with get_db() as db:
        db.execute("DELETE FROM processed_history")
        db.execute("DELETE FROM activity_history")
        db.execute("DELETE FROM inbox")
        db.commit()
    return jsonify({"message": "Cleaned"}), 200

@app.route('/api/contact', methods=['POST'])
def contact():
    data = request.json
    with get_db() as db:
        db.execute("INSERT INTO contact_messages (name, email, message) VALUES (?, ?, ?)", (data.get('name'), data.get('email'), data.get('message')))
        db.commit()
    return jsonify({"message": "Sent"}), 200

@app.route('/api/health')
def health(): return jsonify({"status": "Success"})

if __name__ == '__main__':
    app.run(debug=True, port=5001)