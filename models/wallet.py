# models/wallet.py
from db import db
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, synonym

# ──────────────────────────────────────────────────────────────────────────────
# WalletAccount
#  - DB truth: primary key is user_id (no separate accounts.id)
#  - Provide .id as a synonym so legacy code using WalletAccount.id keeps working.
# ──────────────────────────────────────────────────────────────────────────────
class WalletAccount(db.Model):
    __tablename__ = "wallet_accounts"

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
