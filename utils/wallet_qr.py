# utils/wallet_qr.py
import secrets, re
from typing import Optional
from db import db
from models.wallet import WalletAccount

def _mint_token() -> str:
    # WLT-XXXX-XXXX-XXXX-XXXX (A–Z/0–9)
    raw = secrets.token_urlsafe(18)
    raw = re.sub(r'[^A-Za-z0-9]', '', raw).upper()[:16]
    return f"WLT-{raw[:4]}-{raw[4:8]}-{raw[8:12]}-{raw[12:16]}"

def build_wallet_token(user_id: int) -> str:
    acct = WalletAccount.query.filter_by(user_id=user_id).with_for_update().first()
    if not acct:
        acct = WalletAccount(user_id=user_id, balance_cents=0)
        db.session.add(acct)
        db.session.flush()
    if not acct.qr_token:
        acct.qr_token = _mint_token()
        db.session.commit()
    return acct.qr_token

def verify_wallet_token(token: str) -> Optional[int]:
    acct = WalletAccount.query.filter_by(qr_token=(token or "").strip()).first()
    return acct.user_id if acct else None
