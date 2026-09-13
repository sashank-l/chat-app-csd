"""
dbservice/main.py
Central Shared Database Service (DBaaS)
Implements Part 3 Step 1 of LOAD_BALANCER_FIX_GUIDE.txt.
Backed by SQLite in WAL mode with high-throughput batch ingestion.
"""
import os
import sqlite3
import threading
from flask import Flask, request, jsonify

app = Flask(__name__)
DB_PATH = os.getenv("SHARED_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "chat_service.db"))
_db_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn

def init_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_id       TEXT UNIQUE,
            username     TEXT,
            display_name TEXT,
            ciphertext   TEXT,
            signature    TEXT,
            hmac_digest  TEXT,
            timestamp    TEXT,
            plaintext    TEXT
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shared_msg_id ON messages(msg_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shared_timestamp ON messages(timestamp);")
    conn.commit()
    conn.close()

init_db()

@app.route("/health", methods=["GET"])
def health():
    with _db_lock:
        conn = get_conn()
        cur = conn.execute("SELECT COUNT(*) FROM messages")
        count = cur.fetchone()[0]
        conn.close()
    return jsonify({
        "status": "healthy",
        "service": "Shared DBaaS",
        "db": DB_PATH,
        "messages_in_ram": count
    }), 200

@app.route("/messages/batch", methods=["POST"])
def post_messages_batch():
    batch = request.get_json(silent=True) or []
    if not isinstance(batch, list) or not batch:
        return jsonify({"status": "ok", "inserted": 0}), 200

    rows = []
    for item in batch:
        rows.append((
            item.get("msg_id"),
            item.get("username"),
            item.get("display_name", item.get("username")),
            item.get("ciphertext", ""),
            item.get("signature", ""),
            item.get("hmac_digest", item.get("record_hash", "")),
            str(item.get("timestamp", "")),
            item.get("plaintext", item.get("msg", ""))
        ))

    with _db_lock:
        conn = get_conn()
        try:
            conn.executemany(
                """INSERT OR IGNORE INTO messages 
                   (msg_id, username, display_name, ciphertext, signature, hmac_digest, timestamp, plaintext)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                rows
            )
            conn.commit()
        finally:
            conn.close()

    return jsonify({"status": "ok", "inserted": len(rows)}), 200

@app.route("/messages", methods=["GET"])
def get_messages():
    limit = request.args.get("limit", 100000, type=int)
    with _db_lock:
        conn = get_conn()
        cur = conn.execute(
            """SELECT id, msg_id, username, display_name, ciphertext, signature, hmac_digest, timestamp, plaintext 
               FROM messages ORDER BY id ASC LIMIT ?""",
            (limit,)
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
    return jsonify(rows), 200

@app.route("/reset-state", methods=["POST"])
def reset_state():
    with _db_lock:
        conn = get_conn()
        conn.execute("DELETE FROM messages")
        conn.execute("DELETE FROM sqlite_sequence WHERE name='messages'")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    return jsonify({"status": "ok", "message": "shared database state reset complete"}), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 4252))
    app.run(host="0.0.0.0", port=port, threaded=True)
