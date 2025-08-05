from db import db
from datetime import datetime

class Announcement(db.Model):
    __tablename__ = 'announcements'

    id = db.Column(db.Integer, primary_key=True)
    message = db.Column(db.Text, nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    # ✅ ALTERNATIVE: Use `backref` to define the relationship in one place.
    # This automatically creates the `user.announcements` collection on the User model.
    author = db.relationship(
        'User',
        backref=db.backref('announcements', lazy=True, cascade='all, delete-orphan')
    )