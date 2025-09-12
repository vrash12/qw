# utils/wallet_qr.py
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from flask import current_app
from sqlalchemy import text
from db import db
from secrets import token_urlsafe

SALT_WALLET_QR = "wallet-qr-v1"


def _serializer():
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_WALLET_QR)


def _ensure_wallet_account(uid: int) -> dict:
    """
    Ensure a wallet_accounts row exists; return {'user_id': int, 'qr_token': str|None}.
    Uses raw SQL to avoid ORM drift.
    """
    row = db.session.execute(
        text("SELECT user_id, qr_token FROM wallet_accounts WHERE user_id=:uid"),
        {"uid": uid},
    ).mappings().first()

    if row is None:
        db.session.execute(
            text("INSERT INTO wallet_accounts (user_id, balance_cents) VALUES (:uid, 0)"),
            {"uid": uid},
        )
        db.session.commit()
        return {"user_id": uid, "qr_token": None}

    return {"user_id": int(row["user_id"]), "qr_token": row["qr_token"]}

def build_wallet_token(user_id: int, *, rotate: bool = False, signed: bool = False, length: int = 24) -> str:
    """
    Return a wallet token for the given user.
    - Default: opaque, stable per user (stored in wallet_accounts.qr_token).
    - If rotate=True: mint a new opaque token and persist it.
    - If signed=True: return a signed token (not stored); verify_wallet_token already supports it.
    """
    uid = int(user_id)

    if signed:
        # Signed path (no DB write); verify_wallet_token tries this first.
        return _serializer().dumps({"user_id": uid})

    acct = _ensure_wallet_account(uid)

    if not rotate and acct.get("qr_token"):
        return acct["qr_token"]

    new_tok = token_urlsafe(length)
    db.session.execute(
        text("UPDATE wallet_accounts SET qr_token=:tok WHERE user_id=:uid"),
        {"tok": new_tok, "uid": uid},
    )
    db.session.commit()
    return new_tok

    
def verify_wallet_token(token: str, max_age: int = 60*60*24*30) -> int:
    """Return the commuter user_id for a given wallet_token, or raise ValueError."""
    tok = (token or "").strip()
    if not tok:
        raise ValueError("missing token")

    # Path 1: signed token support (if your /commuter endpoint issues signed tokens)
    try:
        data = _serializer().loads(tok, max_age=max_age)
        uid = int(data.get("user_id") or data.get("uid"))
        if uid > 0:
            return uid
    except (BadSignature, SignatureExpired):
        pass  # fall through to DB lookup

    # Path 2: opaque token stored in wallet_accounts.qr_token
    row = db.session.execute(
        text("SELECT user_id FROM wallet_accounts WHERE qr_token = :t"),
        {"t": tok},
    ).first()
    if row and row[0]:
        return int(row[0])

    raise ValueError("invalid/expired token")
