# models/user_otp.py
from __future__ import annotations
from db import db
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import BIGINT

class UserOtp(db.Model):
    __tablename__ = "user_otps"

    id          = db.Column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    user_id     = db.Column(BIGINT(unsigned=True), db.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    channel     = db.Column(db.String(10), nullable=False)         # 'email'
    destination = db.Column(db.String(254), nullable=False)
    purpose     = db.Column(db.String(32), nullable=False)         # 'signup'
    code_hash   = db.Column(db.String(64), nullable=False)         # sha256 hex string
    expires_at  = db.Column(db.DateTime, nullable=False)
    attempts    = db.Column(db.Integer, nullable=False, default=0)
    created_at  = db.Column(db.DateTime, nullable=False, server_default=func.now())

    # optional: relationship back to user
    user = db.relationship("User", backref=db.backref("otps", cascade="all, delete-orphan"))
