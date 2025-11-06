from db import db
from sqlalchemy.sql import func
from datetime import datetime

class Announcement(db.Model):
    __tablename__ = 'announcements'

    id         = db.Column(db.BigInteger, primary_key=True)  # matches BIGINT in MySQL
    message    = db.Column(db.Text, nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    timestamp  = db.Column(db.DateTime, server_default=func.now(), nullable=False, index=True)

    # ✅ IMPORTANT: map the bus_id column so it actually persists & can be queried
    bus_id     = db.Column(db.Integer, db.ForeignKey('buses.id'), nullable=True, index=True)

    author = db.relationship(
        'User',
        backref=db.backref('announcements', lazy=True, cascade='all, delete-orphan')
    )
    bus = db.relationship('Bus', lazy='joined')
