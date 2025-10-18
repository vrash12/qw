from __future__ import annotations
from db import db
from sqlalchemy.sql import func
from werkzeug.security import generate_password_hash, check_password_hash

class User(db.Model):
    __tablename__ = "users"

    id               = db.Column(db.Integer, primary_key=True)
    # NO email column here – your DB doesn’t have it
    username         = db.Column(db.String(80), nullable=False, unique=True, index=True)
    phone_number     = db.Column(db.String(32), nullable=True, unique=True)
    first_name       = db.Column(db.String(80), nullable=True)
    last_name        = db.Column(db.String(80), nullable=True)
    role             = db.Column(db.String(32), nullable=False, default="commuter", index=True)

    assigned_bus_id  = db.Column(db.Integer, db.ForeignKey("buses.id"), nullable=True, index=True)

    password_hash    = db.Column(db.String(255), nullable=False)

    created_at       = db.Column(db.DateTime, server_default=func.now(), nullable=False)
    updated_at       = db.Column(db.DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # ── Relationships ────────────────────────────────────────────────────────
    assigned_bus = db.relationship(
        "Bus",
        back_populates="users",
        foreign_keys=[assigned_bus_id],
    )

    # Ticket relationships (to match TicketSale.back_populates)
    ticket_sales = db.relationship(
        "TicketSale",
        back_populates="user",
        foreign_keys="TicketSale.user_id",
        cascade="save-update",
    )

    issued_tickets = db.relationship(
        "TicketSale",
        back_populates="issuer",
        foreign_keys="TicketSale.issued_by",
        cascade="save-update",
    )

    # ── Helpers ─────────────────────────────────────────────────────────────
    def set_password(self, raw: str) -> None:
        self.password_hash = generate_password_hash(raw)

    def check_password(self, raw: str) -> bool:
        try:
            return check_password_hash(self.password_hash or "", raw or "")
        except Exception:
            return False

    @property
    def name(self) -> str:
        fn = (self.first_name or "").strip()
        ln = (self.last_name or "").strip()
        return (fn + " " + ln).strip() or (self.username or f"User #{self.id}")

