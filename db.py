import json
import os
import queue
import sqlite3
import threading
import time
import urllib.request

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chat.db")
DB_SERVICE_URL = os.getenv("DB_SERVICE_URL", "http://172.17.0.11:4000")
IS_SHARED_DB_HOST = os.getenv("IS_SHARED_DB_HOST", "false").lower() == "true"

_conn = None
_conn_pid = None
_db_lock = threading.RLock()

_seen_msg_ids = set()
_seen_lock = threading.Lock()

# Ultra-fast in-memory feed cache for instantaneous (sub-millisecond) /feed responses
_memory_feed = []
_feed_lock = threading.Lock()

# Async batched disk persistence queue
_write_queue = queue.Queue(maxsize=200000)
_writer_started = False
_writer_lock = threading.Lock()


def get_conn():
    """
    Get or create process-safe and thread-safe SQLite connection.
    Detects os.getpid() changes after Gunicorn fork.
    """
    global _conn, _conn_pid
    cur_pid = os.getpid()
    if _conn is None or _conn_pid != cur_pid:
        with _db_lock:
            if _conn is None or _conn_pid != cur_pid:
                c = sqlite3.connect(DB_PATH, timeout=60.0, check_same_thread=False)
                c.row_factory = sqlite3.Row
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("PRAGMA synchronous=OFF")
                c.execute("PRAGMA cache_size=-2000")
                c.execute("PRAGMA temp_store=MEMORY")
                c.execute("PRAGMA busy_timeout=60000")
                c.execute("PRAGMA wal_autocheckpoint=1000")
                _conn = c
                _conn_pid = cur_pid
    return _conn


def _background_writer():
    """Background worker thread that flushes write queue in micro-batches to disk."""
    while True:
        try:
            batch = []
            item = _write_queue.get(timeout=1.0)
            batch.append(item)
            _write_queue.task_done()

            while len(batch) < 200:
                try:
                    next_item = _write_queue.get_nowait()
                    batch.append(next_item)
                    _write_queue.task_done()
                except queue.Empty:
                    break

            if batch:
                with _db_lock:
                    conn = get_conn()
                    conn.executemany(
                        """INSERT OR IGNORE INTO messages
                           (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        batch
                    )
                    conn.commit()

                # Replicate batch to Central Shared DBaaS (Part 3 Step 2 of fix guide)
                if DB_SERVICE_URL and not IS_SHARED_DB_HOST:
                    try:
                        payload = [
                            {
                                "msg_id": r[0],
                                "username": r[1],
                                "display_name": r[1],
                                "plaintext": r[2],
                                "ciphertext": r[3],
                                "signature": r[4],
                                "pubkey_jwk": r[5],
                                "timestamp": str(r[6]),
                                "prev_hash": r[7],
                                "hmac_digest": r[8]
                            }
                            for r in batch
                        ]
                        req = urllib.request.Request(
                            f"{DB_SERVICE_URL}/messages/batch",
                            data=json.dumps(payload).encode("utf-8"),
                            headers={"Content-Type": "application/json", "User-Agent": "BackendReplicator/1.0"}
                        )
                        with urllib.request.urlopen(req, timeout=2.0) as resp:
                            pass
                    except Exception:
                        pass
        except queue.Empty:
            continue
        except Exception:
            time.sleep(0.05)


def ensure_writer_started():
    global _writer_started
    if not _writer_started:
        with _writer_lock:
            if not _writer_started:
                t = threading.Thread(target=_background_writer, daemon=True, name="DBWriterThread")
                t.start()
                _writer_started = True


def init_db():
    global _seen_msg_ids, _memory_feed
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_msg_id ON messages(msg_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username   TEXT PRIMARY KEY,
            pubkey_jwk TEXT NOT NULL
        )
    """)
    conn.commit()

    rows = conn.execute(
        "SELECT msg_id, username, plaintext, timestamp FROM messages ORDER BY id ASC"
    ).fetchall()

    with _seen_lock:
        _seen_msg_ids = {r[0] for r in rows}

    with _feed_lock:
        _memory_feed = [(r[0], r[1], r[2], r[3]) for r in rows]

    conn.close()
    ensure_writer_started()


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
    Save a message to memory and queue it for async disk commit.
    Returns True if accepted, False if msg_id already existed (duplicate).
    Sub-millisecond execution!
    """
    ensure_writer_started()

    with _seen_lock:
        if msg_id in _seen_msg_ids:
            return False
        _seen_msg_ids.add(msg_id)

    feed_tuple = (msg_id, username, plaintext, timestamp)
    with _feed_lock:
        _memory_feed.append(feed_tuple)

    db_tuple = (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
    try:
        _write_queue.put_nowait(db_tuple)
    except queue.Full:
        with _db_lock:
            conn = get_conn()
            conn.execute(
                """INSERT OR IGNORE INTO messages
                   (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                db_tuple
            )
            conn.commit()

    return True


def load_history(limit=100000):
    """Load all messages ordered by insertion id."""
    with _feed_lock:
        return [
            {
                "username": t[1],
                "plaintext": t[2],
                "timestamp": t[3]
            }
            for t in _memory_feed[:limit]
        ]


def get_messages_for_feed():
    """
    Returns messages in the exact format needed for /feed.
    Served directly from in-memory cache in < 1ms!
    """
    with _feed_lock:
        return [
            {
                "id": t[0],
                "client-name": t[1],
                "msg": t[2],
                "timestamp": t[3]
            }
            for t in _memory_feed
        ]


def get_feed_json() -> str:
    """
    Directly serialize in-memory feed to JSON string with minimal RAM overhead.
    """
    with _feed_lock:
        items = [
            {
                "id": t[0],
                "client-name": t[1],
                "msg": t[2],
                "timestamp": t[3]
            }
            for t in _memory_feed
        ]
        return json.dumps(items)


def reset_db():
    """Clear messages table, reset hash chain, and clean memory cache."""
    global _seen_msg_ids, _memory_feed
    with _db_lock:
        conn = get_conn()
        conn.execute("DELETE FROM messages")
        conn.execute("DELETE FROM sqlite_sequence WHERE name='messages'")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    with _seen_lock:
        _seen_msg_ids = set()
    with _feed_lock:
        _memory_feed = []


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


def save_messages_batch(batch):
    """
    Ingest a batch of messages into SQLite database and RAM feed cache.
    Used by Central Shared DBaaS endpoint POST /messages/batch.
    """
    if not batch:
        return 0

    rows = []
    feed_items = []
    new_seen = []

    for item in batch:
        msg_id = str(item.get("msg_id") or item.get("id") or "")
        if not msg_id:
            continue
        username = str(item.get("username") or item.get("display_name") or item.get("client-name") or "")
        plaintext = str(item.get("plaintext") or item.get("msg") or item.get("text") or "")
        ciphertext = str(item.get("ciphertext") or "")
        signature = str(item.get("signature") or "")
        pubkey_jwk = str(item.get("pubkey_jwk") or "")
        try:
            timestamp = int(item.get("timestamp") or time.time() * 1000)
        except (ValueError, TypeError):
            timestamp = int(time.time() * 1000)
        prev_hash = str(item.get("prev_hash") or "")
        record_hash = str(item.get("record_hash") or item.get("hmac_digest") or "")

        rows.append((msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash))
        feed_items.append((msg_id, username, plaintext, timestamp))
        new_seen.append(msg_id)

    if not rows:
        return 0

    with _db_lock:
        conn = get_conn()
        conn.executemany(
            """INSERT OR IGNORE INTO messages
               (msg_id, username, plaintext, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows
        )
        conn.commit()

    if not IS_SHARED_DB_HOST:
        with _seen_lock:
            for mid in new_seen:
                _seen_msg_ids.add(mid)

        with _feed_lock:
            _memory_feed.extend(feed_items)

    return len(rows)


def load_shared_messages(limit=100000):
    """Load messages formatted for cross-server inspection (GET /messages)."""
    with _db_lock:
        conn = get_conn()
        cur = conn.execute(
            """SELECT id, msg_id, username, username AS display_name, ciphertext, signature, 
                      record_hash AS hmac_digest, timestamp, plaintext 
               FROM messages ORDER BY id ASC LIMIT ?""",
            (limit,)
        )
        return [dict(r) for r in cur.fetchall()]


def reset_db():
    """Clear all messages from RAM caches, write queue, and SQLite database."""
    global _seen_msg_ids, _memory_feed
    
    # Drain write queue
    while not _write_queue.empty():
        try:
            _write_queue.get_nowait()
            _write_queue.task_done()
        except (queue.Empty, ValueError):
            break

    with _seen_lock:
        _seen_msg_ids.clear()

    with _feed_lock:
        _memory_feed.clear()

    with _db_lock:
        conn = get_conn()
        conn.execute("DELETE FROM messages")
        conn.execute("DELETE FROM sqlite_sequence WHERE name='messages'")
        conn.commit()
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass