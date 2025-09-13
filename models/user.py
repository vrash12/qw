# models/user.py
from __future__ import annotations

from db import db
from werkzeug.security import generate_password_hash, check_password_hash


class User(db.Model):
    __tablename__ = "users"

    # Core identity
    id            = db.Column(db.Integer, primary_key=True)
    first_name    = db.Column(db.String(64),  nullable=False)
    last_name     = db.Column(db.String(64),  nullable=False)
    username      = db.Column(db.String(64),  unique=True, nullable=False)
    phone_number  = db.Column(db.String(32),  unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)

    # Roles:
    # - commuter: default end-user
    # - pao: Passenger Assistance Officer (issues tickets; no longer handles top-ups)
    # - manager: back-office/ops
    # - teller: handles wallet top-ups (cash / GCash)
    role = db.Column(
        db.Enum("commuter", "pao", "manager", "teller", name="user_role"),
        nullable=False,
        default="commuter",
        index=True,
    )

    # Bus assignment (used for PAOs)
    assigned_bus_id = db.Column(db.Integer, db.ForeignKey("buses.id"))
    assigned_bus    = db.relationship("Bus", back_populates="pao", uselist=False)

    # ──────────────────────────────────────────────────────────────────────────
    # Relationships
    # the tickets where this user is the *commuter/owner*
    ticket_sales = db.relationship(
        "TicketSale",
        foreign_keys="TicketSale.user_id",
        back_populates="user",
        cascade="all, delete-orphan",
    )

    # the tickets where this user is the *PAO issuer*
    issued_tickets = db.relationship(
        "TicketSale",
        foreign_keys="TicketSale.issued_by",
        back_populates="issuer",
        lazy="dynamic",
    )

    # Wallet top-ups processed by this user when acting as *operator*.
    # NOTE: The wallet_topups table currently uses legacy column name `pao_id`.
    #       We intentionally target that FK so the system works without a DB migration.
    processed_topups = db.relationship(
        "TopUp",
        primaryjoin="User.id==TopUp.pao_id",
        foreign_keys="TopUp.pao_id",
        lazy="dynamic",
        viewonly=True,  # keep it read-only from this side to avoid accidental writes
        doc="Top-ups recorded by this user (legacy FK name: pao_id).",
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Auth helpers
    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    # ──────────────────────────────────────────────────────────────────────────
    # Role helpers
    @property
    def is_commuter(self) -> bool:
        return self.role == "commuter"

    @property
    def is_pao(self) -> bool:
        return self.role == "pao"

    @property
    def is_manager(self) -> bool:
        return self.role == "manager"

    @property
    def is_teller(self) -> bool:
        return self.role == "teller"

    # Convenience aliases for business logic
    @property
    def can_issue_tickets(self) -> bool:
        return self.is_pao or self.is_manager

    @property
    def can_post_topups(self) -> bool:
        # Top-ups were moved from PAO to Teller. Keep manager override if desired.
        return self.is_teller or self.is_manager

    # Display helpers
    @property
    def name(self) -> str:
        try:
            fn = (self.first_name or "").strip()
            ln = (self.last_name or "").strip()
            return (fn + " " + ln).strip() or self.username
        except Exception:
            return self.username or f"User #{self.id}"

    def __repr__(self) -> str:  # pragma: no cover
        return f"<User id={self.id} role={self.role} username={self.username!r}>"
