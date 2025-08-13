from db import db
from sqlalchemy import func

class TripMetric(db.Model):
    __tablename__ = "trip_metrics"

    id         = db.Column(db.Integer, primary_key=True)
    trip_id    = db.Column(db.Integer, db.ForeignKey("trips.id"), unique=True, nullable=False)
    avg_pax    = db.Column(db.Integer, nullable=False)
    peak_pax   = db.Column(db.Integer, nullable=False)
    boarded    = db.Column(db.Integer, nullable=False)
    alighted   = db.Column(db.Integer, nullable=False)
    start_pax  = db.Column(db.Integer, nullable=False)
    end_pax    = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, server_default=func.now(), nullable=False)
