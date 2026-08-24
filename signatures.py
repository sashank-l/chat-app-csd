"""
Digital Signatures

Each connected sender receives an ECDSA P-256 signing key pair.

The private key is kept on the server for the active session.
The public key is stored with each message so the signature can be
verified again when chat history is loaded.
"""

import base64

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec


def generate_keypair():
    """
    Generate an ECDSA P-256 private/public key pair.
    """
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()

    return private_key, public_key


def public_key_to_pem(public_key) -> str:
    """
    Convert a public key into PEM text so it can be stored in SQLite.
    """
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    return pem.decode("utf-8")


def sign_message(private_key, message: str) -> str:
    """
    Sign a message using ECDSA with SHA-256.

    Returns the DER signature encoded as Base64 text.
    """
    signature = private_key.sign(
        message.encode("utf-8"),
        ec.ECDSA(hashes.SHA256())
    )

    return base64.b64encode(signature).decode("utf-8")


def verify_signature(
    public_key_pem: str,
    signature_b64: str,
    message: str
) -> bool:
    """
    Verify an ECDSA SHA-256 signature.
    """

    try:
        public_key = serialization.load_pem_public_key(
            public_key_pem.encode("utf-8")
        )

        signature = base64.b64decode(
            signature_b64
        )

        public_key.verify(
            signature,
            message.encode("utf-8"),
            ec.ECDSA(hashes.SHA256())
        )

        return True

    except (
        InvalidSignature,
        ValueError,
        TypeError
    ):
        return False