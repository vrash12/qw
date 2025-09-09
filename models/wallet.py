# models/wallet.py
from db import db
<<<<<<< HEAD
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, synonym
=======
from datetime import datetime, timezone

UTCNOW = lambda: datetime.now(timezone.utc)
>>>>>>> 1a29b8b77ab124b7ddaf3563020cbcf5f994cd42

# ──────────────────────────────────────────────────────────────────────────────
# WalletAccount
#  - DB truth: primary key is user_id (no separate accounts.id)
#  - Provide .id as a synonym so legacy code using WalletAccount.id keeps working.
# ──────────────────────────────────────────────────────────────────────────────
class WalletAccount(db.Model):
    __tablename__ = "wallet_accounts"
<<<<<<< HEAD
=======
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, unique=True, index=True)
    balance_cents = db.Column(db.Integer, nullable=False, default=0)
    status = db.Column(db.Enum("active", "suspended"), nullable=False, server_default="active")
    qr_token = db.Column(db.String(64), unique=True, index=True, nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=UTCNOW)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=UTCNOW, onupdate=UTCNOW)
>>>>>>> 1a29b8b77ab124b7ddaf3563020cbcf5f994cd42

    user_id      = db.Column(db.Integer, primary_key=True)
    balance_cents = db.Column(db.Integer, nullable=False, default=0)
    qr_token     = db.Column(db.String(255), nullable=True, unique=True)

    created_at   = db.Column(db.DateTime, server_default=func.now(), nullable=False)
    updated_at   = db.Column(
        db.DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Compatibility alias so code can use .id interchangeably with .user_id
    id = synonym("user_id")

    # Relationships (FKs are defined on the child tables)
    ledger_entries = relationship(
        "WalletLedger",
        primaryjoin="WalletLedger.account_id==WalletAccount.user_id",
        foreign_keys="WalletLedger.account_id",
        backref="account",
        lazy="dynamic",
    )

    topups = relationship(
        "TopUp",
        primaryjoin="TopUp.account_id==WalletAccount.user_id",
        foreign_keys="TopUp.account_id",
        backref="account",
        lazy="dynamic",
    )


# ──────────────────────────────────────────────────────────────────────────────
# WalletLedger
#  - account_id references wallet_accounts.user_id (NOT a separate accounts.id)
# ──────────────────────────────────────────────────────────────────────────────
class WalletLedger(db.Model):
    __tablename__ = "wallet_ledger"
<<<<<<< HEAD
=======
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
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=UTCNOW)
>>>>>>> 1a29b8b77ab124b7ddaf3563020cbcf5f994cd42

    id                      = db.Column(db.Integer, primary_key=True, autoincrement=True)
    account_id              = db.Column(
        db.Integer,
        db.ForeignKey("wallet_accounts.user_id"),
        nullable=False,
        index=True,
    )
    direction               = db.Column(db.Enum("credit", "debit", name="wallet_direction"), nullable=False)
    event                   = db.Column(db.String(64), nullable=False)
    amount_cents            = db.Column(db.Integer, nullable=False)
    running_balance_cents   = db.Column(db.Integer, nullable=False)
    ref_table               = db.Column(db.String(64), nullable=True)
    ref_id                  = db.Column(db.Integer, nullable=True)

    created_at              = db.Column(db.DateTime, server_default=func.now(), nullable=False)


# ──────────────────────────────────────────────────────────────────────────────
# TopUp
#  - account_id references wallet_accounts.user_id
#  - pao_id records which PAO performed the top-up (needed by your queries)
#  - status is used by routes (e.g., filter status="succeeded")
# ──────────────────────────────────────────────────────────────────────────────
class TopUp(db.Model):
    __tablename__ = "wallet_topups"
<<<<<<< HEAD

    id            = db.Column(db.Integer, primary_key=True, autoincrement=True)
    account_id    = db.Column(
        db.Integer,
        db.ForeignKey("wallet_accounts.user_id"),
        nullable=False,
        index=True,
        doc="FK → wallet_accounts.user_id",
    )
    # who performed the top-up (PAO user)
    pao_id        = db.Column(
        db.Integer,
        db.ForeignKey("users.id"),
        nullable=True,
        index=True,
        doc="FK → users.id (PAO who topped up)",
    )

    method        = db.Column(db.Enum("cash", "gcash", name="topup_method"), nullable=False, default="cash")
    amount_cents  = db.Column(db.Integer, nullable=False)

    # keep this as simple string to make migrations easy
    status        = db.Column(db.String(16), nullable=False, server_default="succeeded", index=True)

    created_at    = db.Column(db.DateTime, server_default=func.now(), nullable=False)

    # relationships
    pao           = relationship("User", foreign_keys=[pao_id])
=======
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey("wallet_accounts.id"), nullable=False, index=True)
    method = db.Column(db.Enum("cash", "gcash", name="topup_method"), nullable=False, server_default="cash")
    amount_cents = db.Column(db.Integer, nullable=False)
    status = db.Column(db.Enum("succeeded", "reversed"), nullable=False, server_default="succeeded")
    pao_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True)
    station_id = db.Column(db.Integer)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=UTCNOW)
>>>>>>> 1a29b8b77ab124b7ddaf3563020cbcf5f994cd42
