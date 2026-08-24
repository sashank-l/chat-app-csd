import sqlite3

DB_FILE = "chat.db"

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Get latest message
cursor.execute("""
    SELECT id
    FROM messages
    ORDER BY id DESC
    LIMIT 1
""")

row = cursor.fetchone()

if row is None:
    print("No messages found.")
    conn.close()
    exit()

message_id = row[0]

# Replace the stored public key
cursor.execute("""
    UPDATE messages
    SET pubkey_jwk = ?
    WHERE id = ?
""", (
    "-----BEGIN PUBLIC KEY-----\n"
    "MODIFIED_INVALID_PUBLIC_KEY_FOR_TEST\n"
    "-----END PUBLIC KEY-----",
    message_id
))

conn.commit()
conn.close()