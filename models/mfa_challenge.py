# models/mfa_challenge.py  (new file)
from db import db
from sqlalchemy.sql import func
from datetime import datetime, timedelta

class MfaChallenge(db.Model):
    __tablename__ = "mfa_challenges"
    id         = db.Column(db.String(64), primary_key=True)   # random token
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), index=True, nullable=False)
    created_at = db.Column(db.DateTime, server_default=func.now(), nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)
    consumed   = db.Column(db.Boolean, nullable=False, default=False)
    method     = db.Column(db.String(16), nullable=True)      # set on verify ('totp'|'sms')
  
    code_hash  = db.Column(db.String(255), nullable=True)     # hashed 6-digit code
    phone      = db.Column(db.String(32), nullable=True)
    email      = db.Column(db.String(255), nullable=True)     # NEW
    user = db.relationship("User")
