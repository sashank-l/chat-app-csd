import sqlite3
import threading

DB_PATH = "chat.db"
_local = threading.local()
_seen_msg_ids = set()
_seen_lock = threading.Lock()
_db_lock = threading.Lock()


def get_conn():
    """Get or create thread-local SQLite connection with optimized PRAGMAs."""
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Critical performance tuning for high-RPS SQLite
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA cache_size=50000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA busy_timeout=10000")
        _local.conn = conn
    return _local.conn


def init_db():
    global _seen_msg_ids
    with _db_lock:
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id      TEXT NOT NULL UNIQUE,
                username    TEXT NOT NULL,
                plaintext   TEXT NOT NULL,
                ciphertext  TEXT NOT NULL,
                signature   TEXT NOT NULL,
                pubkey_jwk  TEXT NOT NULL,
                timestamp   INTEGER NOT NULL,
                prev_hash   TEXT NOT NULL,
                record_hash TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_msg_id ON messages(msg_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username   TEXT PRIMARY KEY,
                pubkey_jwk TEXT NOT NULL
            )
        """)
        conn.commit()

        # Pre-populate in-memory set for O(1) deduplication check
        rows = conn.execute("SELECT msg_id FROM messages").fetchall()
        with _seen_lock:
            _seen_msg_ids = {r["msg_id"] for r in rows}


def get_last_hash() -> str:
    """Tail of the hash chain — needed so the next message can link to it."""
    with _db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT record_hash FROM messages ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row["record_hash"] if row else "0" * 64


def save_message(msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash) -> bool:
    """
    Save a message to the database.
    Returns True if inserted, False if msg_id already existed (duplicate).
    Uses O(1) in-memory check + INSERT OR IGNORE.
    """
    with _seen_lock:
        if msg_id in _seen_msg_ids:
            return False
        _seen_msg_ids.add(msg_id)

    with _db_lock:
        conn = get_conn()
        cursor = conn.execute(
            """INSERT OR IGNORE INTO messages
               (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
        )
        conn.commit()
        return cursor.rowcount > 0


def load_history(limit=100000):
    """Load all messages ordered by insertion id."""
    with _db_lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM messages ORDER BY id ASC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_messages_for_feed():
    """
    Returns messages in the exact format needed for /feed.
    Returns all messages in insertion order without artificial limit.
    """
    with _db_lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT msg_id, username, plaintext, timestamp FROM messages ORDER BY id ASC"
        ).fetchall()
        return [
            {
                "id": row["msg_id"],
                "client-name": row["username"],
                "msg": row["plaintext"],
                "timestamp": row["timestamp"]
            }
            for row in rows
        ]


def message_exists(msg_id: str) -> bool:
    """O(1) in-memory check if a message with the given msg_id already exists."""
    with _seen_lock:
        return msg_id in _seen_msg_ids


def get_message_count() -> int:
    """Get total number of stored messages."""
    with _seen_lock:
        return len(_seen_msg_ids)


def upsert_user_pubkey(username, pubkey_jwk_str):
    with _db_lock:
        conn = get_conn()
        conn.execute(
            "INSERT INTO users (username, pubkey_jwk) VALUES (?, ?) "
            "ON CONFLICT(username) DO UPDATE SET pubkey_jwk = excluded.pubkey_jwk",
            (username, pubkey_jwk_str)
        )
        conn.commit()