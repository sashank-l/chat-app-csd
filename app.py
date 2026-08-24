import json
import threading
import time

from flask import Flask, send_from_directory
from flask_sock import Sock

import db
import crypto_utils
import signatures
import integrity


HOST = "0.0.0.0"
PORT = 4309


app = Flask(
    __name__,
    static_folder="static",
    static_url_path=""
)

sock = Sock(app)


# WebSocket -> username
clients = {}

# WebSocket -> sender signing keys for current session
client_keys = {}

clients_lock = threading.Lock()


db.init_db()


@app.route("/")
def index():

    return send_from_directory(
        app.static_folder,
        "index.html"
    )


@app.route("/<path:filename>")
def static_files(filename):

    return send_from_directory(
        app.static_folder,
        filename
    )


def broadcast(payload, exclude=None):

    data = json.dumps(payload)

    with clients_lock:

        dead = []

        for ws in list(clients.keys()):

            if ws is exclude:
                continue

            try:
                ws.send(data)

            except Exception:
                dead.append(ws)

        for ws in dead:

            clients.pop(ws, None)
            client_keys.pop(ws, None)


def broadcast_user_list():

    with clients_lock:

        users = list(clients.values())

    broadcast({
        "type": "userlist",
        "users": users,
        "count": len(users)
    })


def canonical_message(
    username: str,
    text: str,
    timestamp: int
) -> str:

    return f"{username}|{text}|{timestamp}"


def build_history_payload():

    rows = db.load_history()

    chain_ok = integrity.verify_chain(rows)

    history = []


    for row, intact in zip(rows, chain_ok):

        plaintext = crypto_utils.decrypt_text(
            row["ciphertext"]
        )

        decrypt_ok = plaintext is not None


        sig_valid = False

        if decrypt_ok:

            public_key_pem = row["pubkey_jwk"]

            msg_str = canonical_message(
                row["username"],
                plaintext,
                row["timestamp"]
            )

            sig_valid = signatures.verify_signature(
                public_key_pem,
                row["signature"],
                msg_str
            )


        history.append({

            "username": row["username"],

            "text": (
                plaintext
                if decrypt_ok
                else "[unreadable — ciphertext corrupted]"
            ),

            "timestamp": row["timestamp"],

            "tampered": not (
                intact and decrypt_ok
            ),

            "signature_valid": sig_valid
        })

    return history


@sock.route("/ws")
def ws_handler(ws):

    print("[connect] new socket opened")

    username = None


    try:

        while True:

            raw = ws.receive()

            if raw is None:
                break


            try:

                msg = json.loads(raw)

            except (TypeError, ValueError):

                print("[warn] received invalid JSON")

                continue


            mtype = msg.get("type")


            # ---------------------------------
            # JOIN
            # ---------------------------------

            if mtype == "join":

                username = (
                    msg.get("username")
                    or "Anonymous"
                ).strip()[:24] or "Anonymous"


                # Generate one ECDSA key pair
                # for this sender session.

                private_key, public_key = (
                    signatures.generate_keypair()
                )


                public_key_pem = (
                    signatures.public_key_to_pem(
                        public_key
                    )
                )


                with clients_lock:

                    clients[ws] = username

                    client_keys[ws] = {

                        "private_key": private_key,

                        "public_key_pem": public_key_pem
                    }

                    online_count = len(clients)


                # Store sender public key.
                # Private key is NEVER stored in DB.

                db.upsert_user_pubkey(
                    username,
                    public_key_pem
                )


                print(
                    f"[join] {username} joined "
                    f"({online_count} online)"
                )


                # Send stored history to new user.

                ws.send(
                    json.dumps({

                        "type": "history",

                        "messages":
                            build_history_payload()
                    })
                )


                broadcast({

                    "type": "notice",

                    "text":
                        f"{username} joined the chat",

                    "timestamp":
                        int(time.time() * 1000)
                })


                broadcast_user_list()


            # ---------------------------------
            # MESSAGE
            # ---------------------------------

            elif mtype == "message":

                if not username:
                    continue


                text = str(
                    msg.get("text", "")
                )[:2000]


                timestamp = (
                    msg.get("timestamp")
                    or int(time.time() * 1000)
                )


                if not text.strip():
                    continue


                # Get this sender's key pair.

                key_data = client_keys.get(ws)

                if not key_data:

                    ws.send(
                        json.dumps({

                            "type": "error",

                            "text":
                                "Signing key not found."
                        })
                    )

                    continue


                private_key = (
                    key_data["private_key"]
                )

                public_key_pem = (
                    key_data["public_key_pem"]
                )


                # Create canonical data.

                msg_str = canonical_message(
                    username,
                    text,
                    timestamp
                )


                # Requirement 6:
                # Create sender signature.

                signature = signatures.sign_message(
                    private_key,
                    msg_str
                )


                # Verify signature before storing.

                signature_valid = (
                    signatures.verify_signature(
                        public_key_pem,
                        signature,
                        msg_str
                    )
                )


                if not signature_valid:

                    print(
                        f"[warn] signature verification "
                        f"failed for {username}"
                    )

                    ws.send(
                        json.dumps({

                            "type": "error",

                            "text":
                                "Signature verification failed."
                        })
                    )

                    continue


                # Requirement 3:
                # Encrypt before database storage.

                ciphertext = (
                    crypto_utils.encrypt_text(text)
                )


                # Requirement 4:
                # Create hash chain.

                prev_hash = db.get_last_hash()


                record_hash = (
                    integrity.compute_record_hash(
                        prev_hash,
                        username,
                        ciphertext,
                        signature,
                        timestamp
                    )
                )


                # Requirement 1:
                # Save encrypted message.

                db.save_message(

                    username,

                    ciphertext,

                    signature,

                    public_key_pem,

                    timestamp,

                    prev_hash,

                    record_hash
                )


                print(
                    f"[message] {username}: {text}"
                )


                # Broadcast plaintext only to
                # currently connected clients.
                # Database still contains ciphertext.

                broadcast({

                    "type": "message",

                    "username": username,

                    "text": text,

                    "timestamp": timestamp,

                    "signature_valid": True,

                    "tampered": False
                })


            # ---------------------------------
            # TYPING
            # ---------------------------------

            elif mtype == "typing":

                if not username:
                    continue


                broadcast(

                    {

                        "type": "typing",

                        "username": username
                    },

                    exclude=ws
                )


            else:

                print(
                    f"[warn] unknown message type: "
                    f"{mtype}"
                )


    finally:

        with clients_lock:

            was_present = clients.pop(
                ws,
                None
            )

            client_keys.pop(
                ws,
                None
            )

            online_count = len(clients)


        if was_present:

            print(
                f"[leave] {was_present} left "
                f"({online_count} online)"
            )


            broadcast({

                "type": "notice",

                "text":
                    f"{was_present} left the chat",

                "timestamp":
                    int(time.time() * 1000)
            })


            broadcast_user_list()


        else:

            print(
                "[leave] socket closed "
                "before joining"
            )


if __name__ == "__main__":

    print(
        f"Group chat server listening on "
        f"http://{HOST}:{PORT}"
    )

    print(
        f"WebSocket endpoint: "
        f"ws://{HOST}:{PORT}/ws"
    )


    app.run(

        host=HOST,

        port=PORT,

        threaded=True
    )