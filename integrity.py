"""
Member 4 — Tamper Detection (requirement #4: "The system detects modification
of a stored message")

Every stored message is chained to the one before it, blockchain-style:

    record_hash = SHA256(prev_hash | username | ciphertext | signature | timestamp)

Each row stores its own record_hash AND the prev_hash it was built from. To
verify, we walk the table in order and recompute each hash from that row's
current fields. If anyone edits/deletes/reorders a row directly in the
database (bypassing the app), the recomputed hash will no longer match what's
stored — and every hash after that point breaks too, so tampering anywhere in
the history is detected, not just in the most recent message.
"""
import hashlib

GENESIS_HASH = "0" * 64


def compute_record_hash(prev_hash: str, username: str, ciphertext: str,
                         signature: str, timestamp: int) -> str:
    payload = f"{prev_hash}|{username}|{ciphertext}|{signature}|{timestamp}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def verify_record(row: dict, prev_hash: str) -> bool:
    """row needs: username, ciphertext, signature, timestamp, record_hash."""
    expected = compute_record_hash(
        prev_hash, row["username"], row["ciphertext"], row["signature"], row["timestamp"]
    )
    return expected == row["record_hash"]


def verify_chain(rows: list) -> list:
    """
    Walks the whole stored history in insertion order.
    Returns a list of booleans (one per row): True = intact, False = tampered
    (either that row was edited, or an earlier row in the chain was, which
    breaks every link after it).
    """
    results = []
    prev_hash = GENESIS_HASH
    for row in rows:
        results.append(verify_record(row, prev_hash))
        prev_hash = row["record_hash"]
    return results
