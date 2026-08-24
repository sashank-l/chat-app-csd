# 💬 GRP-CHAT: Secure Real-Time Group Chat Application

A lightweight, real-time web-based group chat application built with **Flask**, **WebSockets**, and **SQLite**. The system implements multiple layers of cryptography, including server-side encryption at rest, ECDSA digital signatures for message authentication, and a SHA-256 hash chain for database tamper detection.

---

## 🔒 Security & Cryptographic Features

1. **Encryption at Rest (Fernet / AES-128-CBC + HMAC-SHA256)**
   - All message contents are encrypted before being written to SQLite (`chat.db`).
   - Uses `cryptography.fernet.Fernet` with a master secret key stored in `secret.key` or supplied via the `CHAT_SECRET_KEY` environment variable.
   - Plaintext messages are never stored in the database.

2. **Digital Signatures (ECDSA P-256 + SHA-256)**
   - Every active chat session generates an ECDSA keypair using curve `SECP256R1`.
   - Messages are canonicalized (`username|text|timestamp`) and signed with the sender's session private key.
   - Public keys (in PEM format) are stored alongside messages in SQLite to allow signature verification when history is loaded.

3. **Tamper-Evident Hash Chain (SHA-256)**
   - Stored messages are linked sequentially in a blockchain-style hash chain:
     $$\text{record\_hash} = \text{SHA256}(\text{prev\_hash} \mid \text{username} \mid \text{ciphertext} \mid \text{signature} \mid \text{timestamp})$$
   - Modifying, inserting, or deleting any message directly in the SQLite database breaks subsequent hashes in the chain, triggering visual tamper warnings (`⚠ tampered`) in the client UI.

4. **Real-Time WebSocket Architecture**
   - Built on `flask-sock` for low-latency bidirectional messaging.
   - Features online user lists, real-time connection status indicators, typing indicators, auto-reconnection, and message history synchronization.

---

## 🛠️ Technology Stack

- **Backend**: Python 3, Flask, Flask-Sock (WebSocket), SQLite3, Cryptography (`cryptography` library)
- **Frontend**: Vanilla JavaScript (ES6+), HTML5, Custom CSS3 (Dark Theme)

---

## 📁 Directory & File Structure

```
GRP-CHAT/
├── app.py              # Main Flask server & WebSocket handler (/ws)
├── crypto_utils.py     # Symmetric encryption at rest (Fernet AES-128)
├── db.py               # SQLite database setup, query execution & persistence
├── integrity.py        # SHA-256 hash chain creation & chain verification
├── signatures.py       # ECDSA P-256 key pair generation, signing & verification
├── cipher-test.py      # Security test script: simulates ciphertext database tampering
├── key-tampering.py    # Security test script: simulates public key database tampering
├── requirements.txt    # Python dependencies
├── static/
│   ├── index.html      # Single Page Application HTML structure
│   ├── app.js          # WebSocket connection, event handling & UI rendering
│   └── style.css       # Sleek dark-theme stylesheet and status badges
└── secret.key          # Auto-generated symmetric encryption key (gitignored)
```

---

## 📊 Database Schema (`chat.db`)

### `messages` Table
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `INTEGER PRIMARY KEY` | Auto-incrementing message ID |
| `username` | `TEXT` | Display name of sender |
| `ciphertext` | `TEXT` | Encrypted message content (Base64 Fernet token) |
| `signature` | `TEXT` | Base64-encoded ECDSA signature |
| `pubkey_jwk` | `TEXT` | Sender's public key in PEM format |
| `timestamp` | `INTEGER` | Unix timestamp (ms) |
| `prev_hash` | `TEXT` | SHA-256 hash of previous message entry |
| `record_hash` | `TEXT` | SHA-256 hash of current message entry |

### `users` Table
| Column | Type | Description |
| :--- | :--- | :--- |
| `username` | `TEXT PRIMARY KEY` | Registered user display name |
| `pubkey_jwk` | `TEXT` | Public key associated with user |

---

## 🚀 Getting Started

### 1. Prerequisites
Make sure Python 3.9+ is installed on your system.

### 2. Installation
Clone the repository and install dependencies:
```bash
# Clone repository
git clone <repository-url>
cd GRP-CHAT

# Create virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### 3. Running the Server
Start the Flask WebSocket application:
```bash
python3 app.py
```
The server will run at:
-

Open `http://10.1.75.51:4309/` in multiple browser windows or tabs to test multi-user real-time chat.

---

## 🧪 Security & Tamper Testing Scripts

The repository includes scripts to test the tamper-detection mechanisms of the application:

### Testing Ciphertext Modification (`cipher-test.py`)
Simulates an attacker modifying message content directly in the database:
```bash
python3 cipher-test.py
```
*Effect*: Upon refreshing or connecting a new client, the modified message displays `[unreadable — ciphertext corrupted]` and flags a `⚠ tampered` badge.

### Testing Public Key Tampering (`key-tampering.py`)
Simulates an attacker altering stored public keys to invalidate digital signatures:
```bash
python3 key-tampering.py
```
*Effect*: Signature verification fails for the targeted message, indicating unverified signature status in the UI.

---

## 📜 License & Notes

- **`secret.key`**: Automatically generated on first launch if not provided via `CHAT_SECRET_KEY` environment variable. Ensure this file is kept secret and not committed to version control.
