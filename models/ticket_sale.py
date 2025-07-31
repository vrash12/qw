from db import db
from datetime import datetime
import uuid

class TicketSale(db.Model):
    __tablename__ = 'ticket_sales'

    id                = db.Column(db.Integer, primary_key=True)
    user_id           = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    fare_segment_id = db.Column(db.Integer,
                                db.ForeignKey('fare_segments.id', ondelete='CASCADE'),
                                nullable=True)         

    price             = db.Column(db.Numeric(10, 2), nullable=False)
    ticket_uuid       = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    created_at        = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    # ← NEW COLUMNS
    reference_no      = db.Column(db.String(16), unique=True, nullable=False)
    passenger_type    = db.Column(db.Enum('regular','discount'), nullable=False, server_default='regular')
    paid              = db.Column(db.Boolean, nullable=False, server_default=db.text('0'))

    # relationships
    user              = db.relationship('User', back_populates='ticket_sales')
    fare_segment      = db.relationship('FareSegment')
    bus_id = db.Column(db.Integer, db.ForeignKey("buses.id"), nullable=False)

    bus = db.relationship("Bus", back_populates="tickets")