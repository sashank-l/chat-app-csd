import sqlite3

DB_FILE = "chat.db"

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Get the latest message
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

# Modify ciphertext
cursor.execute("""
    UPDATE messages
    SET ciphertext = ?
    WHERE id = ?
""", (
    "MODIFIED_CIPHERTEXT_SECURITY_TEST",
    message_id
))

conn.commit()
conn.close()