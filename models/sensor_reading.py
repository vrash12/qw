# backend/models/sensor_reading.py
from db import db
from datetime import datetime

class SensorReading(db.Model):
    __tablename__ = 'sensor_readings'

    id           = db.Column(db.Integer, primary_key=True)
    timestamp    = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    in_count     = db.Column(db.Integer, nullable=False)
    out_count    = db.Column(db.Integer, nullable=False)
    total_count  = db.Column(db.Integer, nullable=False)

    # ← ADD THIS:
    bus_id       = db.Column(db.Integer, db.ForeignKey('buses.id'), nullable=False)
    bus          = db.relationship('Bus', back_populates='sensor_readings')
