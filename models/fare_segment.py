from db import db
from datetime import datetime

class FareSegment(db.Model):
    __tablename__ = 'fare_segments'

    id                        = db.Column(db.Integer, primary_key=True)
    trip_id                   = db.Column(db.Integer, db.ForeignKey('trips.id', ondelete='CASCADE'), nullable=False)
    origin_stop_time_id       = db.Column(db.Integer, db.ForeignKey('stop_times.id', ondelete='CASCADE'), nullable=False)
    destination_stop_time_id  = db.Column(db.Integer, db.ForeignKey('stop_times.id', ondelete='CASCADE'), nullable=False)
    distance_km               = db.Column(db.Numeric(6,2), nullable=False)
    price                     = db.Column(db.Numeric(10,2), nullable=False)
    created_at                = db.Column(db.DateTime, default=datetime.utcnow)

    # relationships for convenient access
    trip         = db.relationship('Trip', back_populates='fare_segments')
    origin       = db.relationship('StopTime', foreign_keys=[origin_stop_time_id])
    destination  = db.relationship('StopTime', foreign_keys=[destination_stop_time_id])
    qr_templates = db.relationship(
      'QRTemplate',
      back_populates='fare_segment',
      cascade='all, delete-orphan'
    )