import sqlite3
import threading

DB_PATH = "chat.db"
_lock = threading.Lock()


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username    TEXT NOT NULL,
                ciphertext  TEXT NOT NULL,
                signature   TEXT NOT NULL,
                pubkey_jwk  TEXT NOT NULL,
                timestamp   INTEGER NOT NULL,
                prev_hash   TEXT NOT NULL,
                record_hash TEXT NOT NULL
            )
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


def save_message(username, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash):
    with _lock, get_conn() as conn:
        conn.execute(
            """INSERT INTO messages
               (username, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (username, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
        )
        conn.commit()


def load_history(limit=200):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM messages ORDER BY id ASC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def upsert_user_pubkey(username, pubkey_jwk_str):
    with _lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO users (username, pubkey_jwk) VALUES (?, ?) "
            "ON CONFLICT(username) DO UPDATE SET pubkey_jwk = excluded.pubkey_jwk",
            (username, pubkey_jwk_str)
        )
        conn.commit()