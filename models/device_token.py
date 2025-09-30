# models/device_token.py
from db import db
from sqlalchemy.dialects.mysql import BIGINT as MySQLBigInt
from datetime import datetime

class DeviceToken(db.Model):
    __tablename__ = "device_tokens"
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(MySQLBigInt(unsigned=True),
                           db.ForeignKey('users.id', ondelete='CASCADE', onupdate='CASCADE'),
                           nullable=False, index=True)
    token      = db.Column(db.String(191), unique=True, nullable=False)
    platform   = db.Column(db.String(32))
    created_at = db.Column(db.DateTime, server_default=db.func.now(), nullable=False)
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now(), nullable=False)
