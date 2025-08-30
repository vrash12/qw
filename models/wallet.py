# models/wallet.py
from db import db
from datetime import datetime

class WalletAccount(db.Model):
    __tablename__ = "wallet_accounts"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, unique=True, index=True)
    balance_cents = db.Column(db.Integer, nullable=False, default=0)
    status = db.Column(db.Enum("active", "suspended"), nullable=False, server_default="active")
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

class WalletLedger(db.Model):
    __tablename__ = "wallet_ledger"
    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey("wallet_accounts.id"), nullable=False, index=True)
    direction = db.Column(db.Enum("credit", "debit"), nullable=False)
    event = db.Column(db.Enum("topup", "ride", "refund", "reversal", "adjustment"), nullable=False)
    amount_cents = db.Column(db.Integer, nullable=False)
    running_balance_cents = db.Column(db.Integer, nullable=False)
    ref_table = db.Column(db.String(32))
    ref_id = db.Column(db.Integer)
    performed_by = db.Column(db.BigInteger, db.ForeignKey("users.id"))
    bus_id = db.Column(db.Integer, db.ForeignKey("buses.id"))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class TopUp(db.Model):
    __tablename__ = "wallet_topups"
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey("wallet_accounts.id"), nullable=False, index=True)
    method = db.Column(db.Enum("cash"), nullable=False, server_default="cash")  # Phase 1 only
    amount_cents = db.Column(db.Integer, nullable=False)
    status = db.Column(db.Enum("succeeded", "reversed"), nullable=False, server_default="succeeded")
    pao_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True)
    station_id = db.Column(db.Integer)  # optional if you track terminals
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
