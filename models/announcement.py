from db import db
from datetime import datetime
from models.user import User  # adjust import

class Announcement(db.Model):
    __tablename__ = 'announcements'

    id = db.Column(db.Integer, primary_key=True)
    message = db.Column(db.Text, nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    # replace backref with back_populates
    author = db.relationship(
        'User',
        back_populates='announcements'
    )
