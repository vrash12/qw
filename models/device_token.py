# models/device_token.py
from datetime import datetime
from db import db

class DeviceToken(db.Model):
    __tablename__ = "device_tokens"
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    token      = db.Column(db.String(255), unique=True, nullable=False)
    platform   = db.Column(db.String(32))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship("User", backref="device_tokens")
