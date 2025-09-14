# backend/models/wallet.py
from __future__ import annotations
from db import db
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, synonym

class WalletAccount(db.Model):
    __tablename__ = "wallet_accounts"

    user_id       = db.Column(db.Integer, primary_key=True)
    balance_pesos = db.Column(db.Integer, nullable=False, default=0)  # whole pesos
    qr_token      = db.Column(db.String(255), nullable=True, unique=True)

    created_at    = db.Column(db.DateTime, server_default=func.now(), nullable=False)
    updated_at    = db.Column(db.DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Back-compat alias (no extra column)
    id = synonym("user_id")

    # Back-compat property in CENTS (different name; does NOT shadow column)
    @property
    def balance_cents(self) -> int:
        return int(self.balance_pesos) * 100

    @balance_cents.setter
    def balance_cents(self, value: int) -> None:
        try:
            self.balance_pesos = int(round(int(value) / 100.0))
        except Exception:
            self.balance_pesos = 0

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


class WalletLedger(db.Model):
    __tablename__ = "wallet_ledger"

    id                    = db.Column(db.Integer, primary_key=True, autoincrement=True)
    account_id            = db.Column(db.Integer, db.ForeignKey("wallet_accounts.user_id"), nullable=False, index=True)
    direction             = db.Column(db.Enum("credit", "debit", name="wallet_direction"), nullable=False)
    event                 = db.Column(db.String(64), nullable=False)

    amount_pesos          = db.Column(db.Integer, nullable=False)       # whole pesos
    running_balance_pesos = db.Column(db.Integer, nullable=False)       # whole pesos

    ref_table             = db.Column(db.String(64), nullable=True)
    ref_id                = db.Column(db.Integer, nullable=True)

    created_at            = db.Column(db.DateTime, server_default=func.now(), nullable=False)

    @property
    def amount_cents(self) -> int:
        return int(self.amount_pesos) * 100

    @amount_cents.setter
    def amount_cents(self, value: int) -> None:
        try:
            self.amount_pesos = int(round(int(value) / 100.0))
        except Exception:
            self.amount_pesos = 0

    @property
    def running_balance_cents(self) -> int:
        return int(self.running_balance_pesos) * 100

    @running_balance_cents.setter
    def running_balance_cents(self, value: int) -> None:
        try:
            self.running_balance_pesos = int(round(int(value) / 100.0))
        except Exception:
            self.running_balance_pesos = 0


class TopUp(db.Model):
    __tablename__ = "wallet_topups"

    id           = db.Column(db.Integer, primary_key=True, autoincrement=True)
    account_id   = db.Column(db.Integer, db.ForeignKey("wallet_accounts.user_id"), nullable=False, index=True)

    # Operator-less: no pao_id / teller_id
    method       = db.Column(db.Enum("cash", "gcash", name="topup_method"), nullable=False, default="cash")
    amount_pesos = db.Column(db.Integer, nullable=False)                 # whole pesos

    # We allow pending/succeeded/rejected/cancelled
    status       = db.Column(db.String(16), nullable=False, server_default="pending", index=True)

    # Optional provider metadata (if your schema has them; safe to keep nullable)
    provider     = db.Column(db.String(32), nullable=True)
    provider_ref = db.Column(db.String(128), nullable=True, unique=False)

    created_at   = db.Column(db.DateTime, server_default=func.now(), nullable=False)

    # Back-compat helper in CENTS (different name)
    @property
    def amount_cents(self) -> int:
        return int(self.amount_pesos) * 100

    @amount_cents.setter
    def amount_cents(self, value: int) -> None:
        try:
            self.amount_pesos = int(round(int(value) / 100.0))
        except Exception:
            self.amount_pesos = 0
