from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3
import pandas as pd
from backend_text_analysis import TextAnalyzer

app = Flask(__name__)
CORS(app)
DB_PATH = 'text_storage.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    # MILESTONE 4: Optimized Indexing
    conn.execute('CREATE TABLE IF NOT EXISTS processed_chunks (id INTEGER PRIMARY KEY, chunk_text TEXT, score INTEGER, rules TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_text ON processed_chunks(chunk_text)')
    conn.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE, password TEXT)')
    conn.commit()
    conn.close()

init_db()

@app.route('/api/analyze', methods=['POST'])
def analyze():
    data = request.json
    analyzer = TextAnalyzer()
    nlp_results, chunks, stats = analyzer.run_pipeline(data.get('text', ''), data.get('operations', []))
    
    # MILESTONE 3: Database Storage
    conn = sqlite3.connect(DB_PATH)
    for c in chunks:
        conn.execute("INSERT INTO processed_chunks (chunk_text, score, rules) VALUES (?, ?, ?)", 
                     (c['text'], c['score'], c['matched_rules']))
    conn.commit()
    conn.close()

    return jsonify({"results": nlp_results, "stats": stats})

@app.route('/api/search', methods=['GET'])
def search():
    # MILESTONE 3: Search Function
    q = request.args.get('q', '')
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM processed_chunks WHERE chunk_text LIKE '%{q}%' LIMIT 10", conn)
    conn.close()
    return jsonify(df.to_dict('records'))

if __name__ == '__main__':
    app.run(debug=True, port=5001)