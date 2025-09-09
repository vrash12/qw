# utils/wallet_qr.py
import secrets
from db import db
from models.wallet import WalletAccount

def _mint_token() -> str:
    # short, URL-safe token is fine for QR
    return secrets.token_urlsafe(24)

def build_wallet_token(user_id: int) -> str:
    # lock row for rotate/create & avoid races
    acct = WalletAccount.query.filter_by(user_id=user_id).with_for_update().first()
    if not acct:
        acct = WalletAccount(user_id=user_id, balance_cents=0, qr_token=_mint_token())
        db.session.add(acct)
        db.session.commit()
        return acct.qr_token

    if not acct.qr_token:
        acct.qr_token = _mint_token()
        db.session.commit()

    return acct.qr_token

def verify_wallet_token(token: str) -> int | None:
    acct = WalletAccount.query.filter_by(qr_token=token).first()
    return acct.user_id if acct else None
