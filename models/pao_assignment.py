# models/pao_assignment.py
from datetime import datetime
from db import db

class PaoAssignment(db.Model):
    __tablename__ = "pao_assignments"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    bus_id = db.Column(db.Integer, db.ForeignKey("buses.id"), nullable=False, index=True)
    service_date = db.Column(db.Date, nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("user_id", "service_date", name="uq_paouser_day"),
        db.UniqueConstraint("bus_id", "service_date", name="uq_bus_day"),
    )
