# utils/wallet_qr.py
import base64, hmac, time, os
from hashlib import sha256
from flask import current_app

def _secret() -> bytes:
    v = current_app.config.get("WALLET_QR_SECRET") or os.environ.get("WALLET_QR_SECRET")
    if not v:
        # dev-safe default only; set real secret in prod env
        v = "dev-wallet-secret-change-me"
    return v.encode()

def build_wallet_token(user_id: int, ttl_seconds: int = 300) -> str:
    ts = int(time.time())
    payload = f"v1|{user_id}|{ts}"
    sig = hmac.new(_secret(), payload.encode(), sha256).digest()
    b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")
    return f"WALLET:v1:{user_id}:{ts}:{b64}"

def verify_wallet_token(token: str) -> int:
    try:
        prefix, ver, uid, ts, sig = token.split(":")
        assert prefix == "WALLET" and ver == "v1"
        uid = int(uid); ts = int(ts)
        if abs(time.time() - ts) > 300:
            raise ValueError("TOKEN_EXPIRED")
        expected = hmac.new(_secret(), f"v1|{uid}|{ts}".encode(), sha256).digest()
        got = base64.urlsafe_b64decode(sig + "===")
        if not hmac.compare_digest(expected, got):
            raise ValueError("BAD_SIG")
        return uid
    except Exception as e:
        raise ValueError("INVALID_TOKEN") from e
