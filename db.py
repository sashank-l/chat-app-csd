import sqlite3
import threading
import uuid

DB_PATH = "chat.db"
_lock = threading.Lock()


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def init_db():
    with get_conn() as conn:
        # Core messages table with unique msg_id for deduplication
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


def get_last_hash() -> str:
    """Tail of the hash chain — needed so the next message can link to it."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT record_hash FROM messages ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row["record_hash"] if row else "0" * 64


def save_message(msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash) -> bool:
    """
    Save a message to the database.
    Returns True if inserted, False if msg_id already existed (duplicate).
    Uses INSERT OR IGNORE to prevent duplicate msg_id entries.
    """
    with _lock, get_conn() as conn:
        cursor = conn.execute(
            """INSERT OR IGNORE INTO messages
               (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
        )
        conn.commit()
        return cursor.rowcount > 0


def load_history(limit=10000):
    """Load all messages ordered by insertion id."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM messages ORDER BY id ASC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_messages_for_feed():
    """
    Returns messages in the exact format needed for /feed.
    Returns all messages in insertion order without artificial limit.
    """
    with get_conn() as conn:
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
    """Check if a message with the given msg_id already exists."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM messages WHERE msg_id = ?", (msg_id,)
        ).fetchone()
        return row is not None


def get_message_count() -> int:
    """Get total number of stored messages."""
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM messages").fetchone()
        return row["cnt"] if row else 0


def upsert_user_pubkey(username, pubkey_jwk_str):
    with _lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO users (username, pubkey_jwk) VALUES (?, ?) "
            "ON CONFLICT(username) DO UPDATE SET pubkey_jwk = excluded.pubkey_jwk",
            (username, pubkey_jwk_str)
        )
        conn.commit()