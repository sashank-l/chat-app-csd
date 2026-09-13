import argparse
import json
import os
import threading
try:
    threading.stack_size(256 * 1024)
except Exception:
    pass
import time

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

from flask import Flask, request, jsonify, Response
from flask_sock import Sock

import db
import crypto_utils
import signatures
import integrity


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 4210

app = Flask(__name__, static_folder="static", static_url_path="")
app.config["JSON_AS_ASCII"] = False
sock = Sock(app)

# WebSocket clients (kept for browser UI compatibility)
clients = {}
client_keys = {}
clients_lock = threading.Lock()

# Per-request response time tracking for /health
_response_times = []
_response_times_lock = threading.Lock()
_active_requests = 0
_active_requests_lock = threading.Lock()

db.init_db()


def track_response_time(elapsed_ms):
    with _response_times_lock:
        _response_times.append(elapsed_ms)
        # Keep last 100 readings
        if len(_response_times) > 100:
            _response_times.pop(0)


_cached_cpu = 5.0

def _cpu_sampler():
    global _cached_cpu
    prev_idle = 0
    prev_total = 0
    while True:
        try:
            with open("/proc/stat") as f:
                line = f.readline()
            vals = list(map(int, line.split()[1:]))
            idle = vals[3]
            total = sum(vals)
            if prev_total > 0:
                d_idle = idle - prev_idle
                d_total = total - prev_total
                if d_total > 0:
                    _cached_cpu = round(100.0 * (1.0 - d_idle / d_total), 1)
            prev_idle, prev_total = idle, total
        except Exception:
            pass
        time.sleep(1.0)

threading.Thread(target=_cpu_sampler, daemon=True).start()


def _get_cpu_percent():
    """Non-blocking: returns cached CPU percent immediately."""
    return _cached_cpu


def _get_mem_percent():
    """Read memory usage from /proc/meminfo or psutil."""
    if HAS_PSUTIL:
        return psutil.virtual_memory().percent
    try:
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    info[parts[0].rstrip(':')] = int(parts[1])
        total = info.get("MemTotal", 1)
        avail = info.get("MemAvailable", total)
        return round(100.0 * (total - avail) / total, 1)
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
# Health & Metrics Routes
# ─────────────────────────────────────────────

@app.route("/health")
def health():
    """
    Returns system metrics used by the Load Balancer for dynamic routing decisions.
    Lower values = better backend.
    """
    cpu_percent = _get_cpu_percent()
    mem_percent = _get_mem_percent()

    with _response_times_lock:
        times = list(_response_times)
    avg_response_ms = (sum(times) / len(times)) if times else 0.0

    with _active_requests_lock:
        active = _active_requests

    # Composite load score (0–100): lower is better
    score = (cpu_percent * 0.5) + (mem_percent * 0.2) + min(active * 2, 30)

    return jsonify({
        "status": "ok",
        "cpu_percent": cpu_percent,
        "memory_percent": mem_percent,
        "active_connections": active,
        "avg_response_ms": round(avg_response_ms, 2),
        "message_count": db.get_message_count(),
        "load_score": round(score, 2)
    }), 200


# ─────────────────────────────────────────────
# REST API Routes: /message and /feed
# ─────────────────────────────────────────────

# Caching for high-throughput ECDSA signing and hash chain
_keypair_cache = {}
_keypair_lock = threading.Lock()
_last_hash_val = db.get_last_hash()
_hash_chain_lock = threading.Lock()


def get_or_create_keypair(username):
    with _keypair_lock:
        if username not in _keypair_cache:
            if len(_keypair_cache) > 1000:
                _keypair_cache.clear()
            priv, pub = signatures.generate_keypair()
            pem = signatures.public_key_to_pem(pub)
            _keypair_cache[username] = (priv, pem)
        return _keypair_cache[username]


def get_next_hashes(client_name, ciphertext, signature, timestamp):
    global _last_hash_val
    with _hash_chain_lock:
        prev = _last_hash_val
        rec = integrity.compute_record_hash(prev, client_name, ciphertext, signature, timestamp)
        _last_hash_val = rec
        return prev, rec


@app.route("/message", methods=["POST"])
def post_message():
    """
    Accept a new chat message.
    Expected form params: client-name, msg
    Optional: msg_id (UUID) for deduplication across backends
    """
    global _active_requests
    t_start = time.perf_counter()

    with _active_requests_lock:
        _active_requests += 1

    try:
        # Support both form data and JSON body
        if request.is_json:
            data = request.get_json(force=True) or {}
        else:
            data = request.form.to_dict()
            if not data:
                data = request.get_json(silent=True) or {}

        client_name = str(data.get("client-name") or data.get("username") or "")
        msg_text = str(data.get("msg") or data.get("text") or "")
        msg_id = str(data.get("msg_id") or data.get("id") or "").strip()

        if not client_name:
            return jsonify({"error": "client-name is required"}), 400
        if not msg_text:
            return jsonify({"error": "msg is required"}), 400

        # If no msg_id provided, generate one (LB should provide it for fan-out writes)
        import uuid
        if not msg_id:
            msg_id = str(uuid.uuid4())

        # If message already exists (duplicate), return success without re-inserting
        if db.message_exists(msg_id):
            return jsonify({
                "status": "duplicate",
                "msg_id": msg_id,
                "client-name": client_name,
                "msg": msg_text
            }), 200

        timestamp = int(time.time() * 1000)

        # Retrieve or generate ECDSA keypair for this sender
        private_key, public_key_pem = get_or_create_keypair(client_name)

        # Canonical message for signing
        msg_str = f"{client_name}|{msg_text}|{timestamp}"

        # Sign the message
        signature = signatures.sign_message(private_key, msg_str)

        # Encrypt the message for storage
        ciphertext = crypto_utils.encrypt_text(msg_text)

        # Link to hash chain
        prev_hash, record_hash = get_next_hashes(client_name, ciphertext, signature, timestamp)

        # Store in DB — INSERT OR IGNORE ensures no duplicates
        inserted = db.save_message(
            msg_id=msg_id,
            username=client_name,
            plaintext=msg_text,
            ciphertext=ciphertext,
            signature=signature,
            pubkey_jwk=public_key_pem,
            timestamp=timestamp,
            prev_hash=prev_hash,
            record_hash=record_hash
        )

        # Broadcast over WebSocket only if browser clients are connected
        if inserted and clients:
            try:
                _broadcast_ws({
                    "type": "message",
                    "username": client_name,
                    "text": msg_text,
                    "timestamp": timestamp,
                    "signature_valid": True,
                    "tampered": False
                })
            except Exception:
                pass

        return jsonify({
            "status": "ok",
            "msg_id": msg_id,
            "client-name": client_name,
            "msg": msg_text,
            "timestamp": timestamp
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        with _active_requests_lock:
            _active_requests -= 1
        elapsed = (time.perf_counter() - t_start) * 1000
        track_response_time(elapsed)


@app.route("/feed", methods=["GET"])
def get_feed():
    """
    Returns all stored messages in order.
    Each message has: id, client-name, msg, timestamp
    """
    t_start = time.perf_counter()
    try:
        data = db.get_feed_json()
        return Response(data, mimetype="application/json", status=200)
    finally:
        elapsed = (time.perf_counter() - t_start) * 1000
        track_response_time(elapsed)


@app.route("/reset-state", methods=["POST"])
def reset_state():
    """Reset messages database and memory cache for a clean benchmark run."""
    db.reset_db()
    global _last_hash_val
    with _hash_chain_lock:
        _last_hash_val = "0" * 64
    with _keypair_lock:
        _keypair_cache.clear()
    return jsonify({"status": "ok", "message": "database reset complete"}), 200


@app.route("/messages/batch", methods=["POST"])
def post_messages_batch():
    """
    Central Shared DB endpoint: accepts batch of message records from any backend
    and persists them into the shared SQLite database in a single transaction.
    """
    batch = request.get_json(silent=True) or []
    if not isinstance(batch, list) or not batch:
        return jsonify({"status": "ok", "inserted": 0}), 200

    inserted = db.save_messages_batch(batch)
    return jsonify({"status": "ok", "inserted": inserted}), 200


@app.route("/messages", methods=["GET"])
def get_messages():
    """
    Returns all stored messages for cross-server inspection or verification.
    curl -s http://.../messages | jq '.[-1]'
    """
    limit = request.args.get("limit", 100000, type=int)
    messages = db.load_shared_messages(limit=limit)
    return jsonify(messages), 200


# ─────────────────────────────────────────────
# Legacy WebSocket route (kept for browser UI)
# ─────────────────────────────────────────────

def _broadcast_ws(payload):
    data = json.dumps(payload)
    with clients_lock:
        dead = []
        for ws in list(clients.keys()):
            try:
                ws.send(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            clients.pop(ws, None)
            client_keys.pop(ws, None)


def canonical_message(username, text, timestamp):
    return f"{username}|{text}|{timestamp}"


@sock.route("/ws")
def ws_handler(ws):
    username = None
    try:
        while True:
            raw = ws.receive()
            if raw is None:
                break
            try:
                msg = json.loads(raw)
            except (TypeError, ValueError):
                continue

            mtype = msg.get("type")

            if mtype == "join":
                username = (msg.get("username") or "Anonymous").strip()[:24] or "Anonymous"
                private_key, public_key = signatures.generate_keypair()
                public_key_pem = signatures.public_key_to_pem(public_key)

                with clients_lock:
                    clients[ws] = username
                    client_keys[ws] = {"private_key": private_key, "public_key_pem": public_key_pem}

                db.upsert_user_pubkey(username, public_key_pem)

                # Send history to new user via WebSocket
                rows = db.load_history()
                history = []
                for row in rows:
                    history.append({
                        "username": row["username"],
                        "text": row["plaintext"],
                        "timestamp": row["timestamp"],
                        "tampered": False,
                        "signature_valid": True
                    })
                ws.send(json.dumps({"type": "history", "messages": history}))

            elif mtype == "message":
                if not username:
                    continue
                text = str(msg.get("text", ""))[:2000]
                timestamp = msg.get("timestamp") or int(time.time() * 1000)
                if not text.strip():
                    continue

                import uuid
                msg_id = str(uuid.uuid4())

                key_data = client_keys.get(ws)
                if not key_data:
                    continue

                private_key = key_data["private_key"]
                public_key_pem = key_data["public_key_pem"]
                msg_str = canonical_message(username, text, timestamp)
                signature = signatures.sign_message(private_key, msg_str)
                ciphertext = crypto_utils.encrypt_text(text)
                prev_hash = db.get_last_hash()
                record_hash = integrity.compute_record_hash(prev_hash, username, ciphertext, signature, timestamp)

                db.save_message(
                    msg_id=msg_id,
                    username=username,
                    plaintext=text,
                    ciphertext=ciphertext,
                    signature=signature,
                    pubkey_jwk=public_key_pem,
                    timestamp=timestamp,
                    prev_hash=prev_hash,
                    record_hash=record_hash
                )

                _broadcast_ws({
                    "type": "message",
                    "username": username,
                    "text": text,
                    "timestamp": timestamp,
                    "signature_valid": True,
                    "tampered": False
                })

    finally:
        with clients_lock:
            was_present = clients.pop(ws, None)
            client_keys.pop(ws, None)


# ─────────────────────────────────────────────
# Static files (optional browser UI)
# ─────────────────────────────────────────────

@app.route("/")
def index():
    from flask import send_from_directory
    return send_from_directory(app.static_folder, "index.html")


@app.route("/<path:filename>")
def static_files(filename):
    from flask import send_from_directory
    return send_from_directory(app.static_folder, filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Secure Chat Backend — Lab 6")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--also-port", type=int, default=0, help="Optional second port to serve concurrently")
    args = parser.parse_args()

    if args.also_port and args.also_port != args.port:
        import threading
        from werkzeug.serving import run_simple
        t = threading.Thread(
            target=run_simple,
            args=(args.host, args.also_port, app),
            kwargs={"threaded": True},
            daemon=True
        )
        t.start()
        print(f"[backend] Also listening on http://{args.host}:{args.also_port}")

    print(f"[backend] Listening on http://{args.host}:{args.port}")
    print(f"[backend] REST API: POST /message   GET /feed   GET /health")

    app.run(host=args.host, port=args.port, threaded=True)